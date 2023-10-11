import Foundation
import Safehill_Crypto
import KnowledgeBase
import os
import Photos

protocol SHDownloadOperationProtocol {}


public class SHDownloadOperation: SHAbstractBackgroundOperation, SHBackgroundQueueProcessorOperationProtocol, SHDownloadOperationProtocol {
    
    public let log = Logger(subsystem: "com.safehill", category: "BG-DOWNLOAD")
    
    public let limit: Int?
    let user: SHLocalUser
    
    let delegates: [SHAssetDownloaderDelegate]
    let restorationDelegate: SHAssetActivityRestorationDelegate
    
    let photoIndexer: SHPhotosIndexer
    
    public init(user: SHLocalUser,
                delegates: [SHAssetDownloaderDelegate],
                restorationDelegate: SHAssetActivityRestorationDelegate,
                limitPerRun limit: Int? = nil,
                photoIndexer: SHPhotosIndexer? = nil) {
        self.user = user
        self.limit = limit
        self.delegates = delegates
        self.restorationDelegate = restorationDelegate
        self.photoIndexer = photoIndexer ?? SHPhotosIndexer()
    }
    
    var serverProxy: SHServerProxy {
        SHServerProxy(user: self.user)
    }
    
    public func clone() -> SHBackgroundOperationProtocol {
        SHDownloadOperation(
            user: self.user,
            delegates: self.delegates,
            restorationDelegate: self.restorationDelegate,
            limitPerRun: self.limit,
            photoIndexer: self.photoIndexer
        )
    }
    
    private func fetchAsset(withGlobalIdentifier globalIdentifier: GlobalIdentifier,
                            quality: SHAssetQuality,
                            request: SHDownloadRequestQueueItem,
                            completionHandler: @escaping (Result<any SHDecryptedAsset, Error>) -> Void) {
        let start = CFAbsoluteTimeGetCurrent()
        
        log.info("downloading assets with identifier \(globalIdentifier) version \(quality.rawValue)")
        serverProxy.getAssets(
            withGlobalIdentifiers: [globalIdentifier],
            versions: [quality]
        )
        { result in
            switch result {
            case .success(let assetsDict):
                guard assetsDict.count > 0,
                      let encryptedAsset = assetsDict[globalIdentifier] else {
                    completionHandler(.failure(SHHTTPError.ClientError.notFound))
                    return
                }
                do {
                    let localAssetStore = SHLocalAssetStoreController(
                        user: self.user
                    )
                    let decryptedAsset = try localAssetStore.decryptedAsset(
                        encryptedAsset: encryptedAsset,
                        quality: quality,
                        descriptor: request.assetDescriptor
                    )
                    completionHandler(.success(decryptedAsset))
                }
                catch {
                    completionHandler(.failure(error))
                }
            case .failure(let err):
                self.log.critical("unable to download assets \(globalIdentifier) version \(quality.rawValue) from server: \(err)")
                completionHandler(.failure(err))
            }
            let end = CFAbsoluteTimeGetCurrent()
            self.log.debug("[PERF] \(CFAbsoluteTime(end - start)) for version \(quality.rawValue)")
        }
    }
    
    public func content(ofQueueItem item: KBQueueItem) throws -> SHSerializableQueueItem {
        guard let data = item.content as? Data else {
            throw KBError.unexpectedData(item.content)
        }
        
        let unarchiver: NSKeyedUnarchiver
        if #available(macOS 10.13, *) {
            unarchiver = try NSKeyedUnarchiver(forReadingFrom: data)
        } else {
            unarchiver = NSKeyedUnarchiver(forReadingWith: data)
        }
        
        guard let downloadRequest = unarchiver.decodeObject(of: SHDownloadRequestQueueItem.self, forKey: NSKeyedArchiveRootObjectKey) else {
            throw KBError.unexpectedData(item)
        }
        
        return downloadRequest
    }
    
    internal func fetchApplePhotosLibraryAssets(for localIdentifiers: [String]) throws -> PHFetchResult<PHAsset>? {
        let group = DispatchGroup()
        var fetchResult: PHFetchResult<PHAsset>? = nil
        var appleLibraryFetchError: Error? = nil
        
        group.enter()
        self.photoIndexer.fetchCameraRollAssets(withFilters: [.withLocalIdentifiers(localIdentifiers)]) { result in
            switch result {
            case .success(let fullFetchResult):
                fetchResult = fullFetchResult
            case .failure(let err):
                appleLibraryFetchError = err
            }
            group.leave()
        }
        
        let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds))
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
        guard appleLibraryFetchError == nil else {
            throw appleLibraryFetchError!
        }
        
        return fetchResult
    }
    
    internal func fetchDescriptorsFromServer() throws -> [any SHAssetDescriptor] {
        let group = DispatchGroup()
        
        var descriptors = [any SHAssetDescriptor]()
        var error: Error? = nil
        
        group.enter()
        serverProxy.getRemoteAssetDescriptors { result in
            switch result {
            case .success(let descs):
                descriptors = descs
            case .failure(let err):
                error = err
            }
            group.leave()
        }
        
        let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultNetworkTimeoutInMilliseconds))
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
        
        guard error == nil else {
            throw error!
        }
        
        return descriptors
    }
    
    ///
    /// For all the descriptors whose originator user is `self.user`, notify the restoration delegate
    /// about all groups that need to be restored along with the queue item identifiers for each `groupId`.
    /// Uploads and shares will be reported separately, according to the contract in the delegate.
    ///
    /// - Parameters:
    ///   - descriptorsByGlobalIdentifier: all the descriptors keyed by asset global identifier
    ///   - completionHandler: the callback method
    func restoreQueueItems(
        descriptorsByGlobalIdentifier original: [GlobalIdentifier: any SHAssetDescriptor],
        filteringKeys: [GlobalIdentifier],
        completionHandler: @escaping (Swift.Result<Void, Error>) -> Void
    ) {
        guard original.count > 0 else {
            completionHandler(.success(()))
            return
        }
        
        let descriptorsByGlobalIdentifier = original.filter({ filteringKeys.contains($0.key) })
        
        guard descriptorsByGlobalIdentifier.count > 0 else {
            completionHandler(.success(()))
            return
        }
        
        ///
        /// Fetch from server users information (`SHServerUser` objects) for all user identifiers found in all descriptors
        ///
        var usersById = [String: SHServerUser]()
        var userIdentifiers = Set(descriptorsByGlobalIdentifier.values.flatMap { $0.sharingInfo.sharedWithUserIdentifiersInGroup.keys })
        userIdentifiers.formUnion(Set(descriptorsByGlobalIdentifier.values.compactMap { $0.sharingInfo.sharedByUserIdentifier }))
        
        do {
            usersById = try SHUsersController(localUser: self.user).getUsers(withIdentifiers: Array(userIdentifiers)).reduce([:], { partialResult, user in
                var result = partialResult
                result[user.identifier] = user
                return result
            })
        } catch {
            self.log.error("Unable to fetch users from local server: \(error.localizedDescription)")
            completionHandler(.failure(error))
            return
        }
        
        var uploadQueueItemsIdsByGroupId = [String: [String]]()
        var shareQueueItemsIdsByGroupId = [String: [String]]()
        var userIdsInvolvedInRestoration = Set<String>()
        
        for (_, descriptor) in descriptorsByGlobalIdentifier {
            guard descriptor.sharingInfo.sharedByUserIdentifier == user.identifier else {
                continue
            }
            
            // TODO: Is it possible for the local identifier not to exist? What happens when the asset is only on the server, and not in the local library?
            guard let localIdentifier = descriptor.localIdentifier else {
                continue
            }
            
            if descriptor.sharingInfo.sharedWithUserIdentifiersInGroup.count == 1,
               let recipientUserId = descriptor.sharingInfo.sharedWithUserIdentifiersInGroup.keys.first,
               recipientUserId == user.identifier {
                ///
                /// This handles assets just shared with self, meaning that they were just uploaded to the lockbox.
                /// Restore the upload from the queue
                ///
                let groupId = descriptor.sharingInfo.sharedWithUserIdentifiersInGroup[recipientUserId]!
                
                let queueItemIdentifier = SHUploadPipeline.queueItemIdentifier(
                    groupId: groupId,
                    assetLocalIdentifier: localIdentifier,
                    versions: [.lowResolution, .hiResolution],
                    users: []
                )
                
                if uploadQueueItemsIdsByGroupId[groupId] == nil {
                    uploadQueueItemsIdsByGroupId[groupId] = [queueItemIdentifier]
                } else {
                    uploadQueueItemsIdsByGroupId[groupId]!.append(queueItemIdentifier)
                }
            } else {
                var userIdsByGroup = [String: [String]]()
                for (userId, groupId) in descriptor.sharingInfo.sharedWithUserIdentifiersInGroup {
                    guard userId != user.identifier else {
                        continue
                    }
                    if userIdsByGroup[groupId] == nil {
                        userIdsByGroup[groupId] = [userId]
                    } else {
                        userIdsByGroup[groupId]!.append(userId)
                    }
                }
                
                for (groupId, userIds) in userIdsByGroup {
                    var queueItemIdentifiers = [String]()
                    
                    ///
                    /// There are 2 possible cases:
                    /// 1. The asset was first uploaded, then shared with other users
                    ///     -> the .low and .hi resolutions were uploaded right away, no .mid. In this case the identifier only reference this user
                    /// 2. The asset was shared with other users before uploading
                    ///     -> the .low and .mid resolutions were uploaded first, then the .hi resolution
                    ///
                    /// Because of this, ask to restore all 3 combinations
                    ///
                    queueItemIdentifiers.append(
                        SHUploadPipeline.queueItemIdentifier(
                            groupId: groupId,
                            assetLocalIdentifier: localIdentifier,
                            versions: [.lowResolution, .hiResolution],
                            users: []
                        )
                    )
                    queueItemIdentifiers.append(
                        SHUploadPipeline.queueItemIdentifier(
                            groupId: groupId,
                            assetLocalIdentifier: localIdentifier,
                            versions: [.lowResolution, .midResolution],
                            users: userIds.map({ usersById[$0]! })
                        )
                    )
                    queueItemIdentifiers.append(
                        SHUploadPipeline.queueItemIdentifier(
                            groupId: groupId,
                            assetLocalIdentifier: localIdentifier,
                            versions: [.hiResolution],
                            users: userIds.map({ usersById[$0]! })
                        )
                    )
                    if uploadQueueItemsIdsByGroupId[groupId] == nil {
                        uploadQueueItemsIdsByGroupId[groupId] = queueItemIdentifiers
                    } else {
                        uploadQueueItemsIdsByGroupId[groupId]!.append(contentsOf: queueItemIdentifiers)
                    }
                    
                    ///
                    /// When sharing with other users, `.midResolution` is uploaded first with `.lowResolution`
                    /// and `.hiResolution` comes later.
                    ///
                    queueItemIdentifiers = [String]()
                    
                    queueItemIdentifiers.append(
                        SHUploadPipeline.queueItemIdentifier(
                            groupId: groupId,
                            assetLocalIdentifier: localIdentifier,
                            versions: [.lowResolution, .midResolution],
                            users: userIds.map({ usersById[$0]! })
                        )
                    )
                    queueItemIdentifiers.append(
                        SHUploadPipeline.queueItemIdentifier(
                            groupId: groupId,
                            assetLocalIdentifier: localIdentifier,
                            versions: [.hiResolution],
                            users: userIds.map({ usersById[$0]! })
                        )
                    )
                    
                    userIds.forEach({ userIdsInvolvedInRestoration.insert($0) })
                    
                    if shareQueueItemsIdsByGroupId[groupId] == nil {
                        shareQueueItemsIdsByGroupId[groupId] = queueItemIdentifiers
                    } else {
                        shareQueueItemsIdsByGroupId[groupId]!.append(contentsOf: queueItemIdentifiers)
                    }
                }
            }
        }
        
        self.log.debug("upload queue items by group \(uploadQueueItemsIdsByGroupId)")
        self.log.debug("share queue items by group \(shareQueueItemsIdsByGroupId)")
        
        for (groupId, queueItemIdentifiers) in uploadQueueItemsIdsByGroupId {
            self.restorationDelegate.restoreUploadQueueItems(withIdentifiers: queueItemIdentifiers, in: groupId)
        }
        
        for (groupId, queueItemIdentifiers) in shareQueueItemsIdsByGroupId {
            self.restorationDelegate.restoreShareQueueItems(withIdentifiers: queueItemIdentifiers, in: groupId)
        }
        
        self.restorationDelegate.didCompleteRestoration(
            userIdsInvolvedInRestoration: Array(userIdsInvolvedInRestoration)
        )
        
        completionHandler(.success(()))
    }
    
    ///
    /// Fetch descriptors from server (remote or local, depending on whether it's running as part of the
    /// `SHLocalActivityRestoreOperation` or the `SHDownloadOperation`.
    /// Filters out the blacklisted assets and users, as well as the non-completed uploads.
    /// Call the delegate with the full manifest of assets shared by OTHER users, regardless of the limit on the task config) for the assets.
    /// Return the full set of descriptors fetched from the server, keyed by global identifier, limiting the result based on the task config.
    ///
    /// - Parameter completionHandler: the callback
    internal func processDescriptors(
        completionHandler: @escaping (Swift.Result<[String: SHAssetDescriptor], Error>) -> Void
    ) {
        let start = CFAbsoluteTimeGetCurrent()
        
        var descriptors = [any SHAssetDescriptor]()
        do {
            descriptors = try self.fetchDescriptorsFromServer()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        ///
        /// Filter out the ones:
        /// - whose assets were blacklisted
        /// - whose users were blacklisted
        /// - haven't started upload (`.notStarted` is only relevant for the `SHLocalActivityRestoreOperation`)
        ///
        descriptors = descriptors.filter {
            DownloadBlacklist.shared.isBlacklisted(assetGlobalIdentifier: $0.globalIdentifier) == false
            && DownloadBlacklist.shared.isBlacklisted(userIdentifier: $0.sharingInfo.sharedByUserIdentifier) == false
            && $0.uploadState == .completed || $0.uploadState == .partial
        }
        
        guard descriptors.count > 0 else {
            completionHandler(.success([:]))
            return
        }
        
        /// For the Filter out the `didReceiveAssetDescriptors(_:referencing:completionHandler)` delegate, filter the ones:
        /// - shared by self
        /// - whose sender is unknown (the delegate method `didReceiveAuthorizationRequest(for:referencing:)` will take care of that)
        let descriptorsSharedByOthers = descriptors.filter {
            $0.sharingInfo.sharedByUserIdentifier != user.identifier
            && ((try? SHKGQuery.isKnownUser(withIdentifier: $0.sharingInfo.sharedByUserIdentifier)) ?? false)
        }
        
        ///
        /// Fetch from server users information (`SHServerUser` objects)
        /// for all user identifiers found in all descriptors shared by OTHER known users
        ///
        var users = [SHServerUser]()
        var userIdentifiers = Set(descriptorsSharedByOthers.flatMap { $0.sharingInfo.sharedWithUserIdentifiersInGroup.keys })
        userIdentifiers.formUnion(Set(descriptorsSharedByOthers.compactMap { $0.sharingInfo.sharedByUserIdentifier }))
        
        do {
            users = try SHUsersController(localUser: self.user).getUsers(withIdentifiers: Array(userIdentifiers))
        } catch {
            self.log.error("Unable to fetch users from local server: \(error.localizedDescription)")
            completionHandler(.failure(error))
            return
        }
        
        ///
        /// Call the delegate with the full manifest of whitelisted assets **ONLY for the assets shared by other known users**.
        /// The ones shared by THIS user will be restored through the restoration delegate.
        ///
        self.delegates.forEach({
            $0.didReceiveAssetDescriptors(descriptorsSharedByOthers,
                                          referencing: users,
                                          completionHandler: nil)
        })
        
        let end = CFAbsoluteTimeGetCurrent()
        self.log.debug("[PERF] it took \(CFAbsoluteTime(end - start)) to fetch \(descriptors.count) descriptors")
        
        var descriptorsByGlobalIdentifier = [String: any SHAssetDescriptor]()
        for descriptor in descriptors {
            descriptorsByGlobalIdentifier[descriptor.globalIdentifier] = descriptor
            ///
            /// Limit based on the task configuration
            ///
            if let limit = self.limit, descriptorsByGlobalIdentifier.count > limit {
                break
            }
        }
        completionHandler(.success(descriptorsByGlobalIdentifier))
    }
    
    
    ///
    /// Get the apple photos identifiers in the local library corresponding to the assets in the descriptors having a local identifier
    /// If they exist, then do not decrypt them but just serve the local asset.
    /// If they don't, return them in the callback method so they can be decrypted.
    ///
    /// - Parameters:
    ///   - descriptorsByGlobalIdentifier: all the descriptors by local identifier
    ///   - completionHandler: the callback method
    internal func mergeDescriptorsWithApplePhotosAssets(
        descriptorsByGlobalIdentifier original: [GlobalIdentifier: any SHAssetDescriptor],
        filteringKeys: [GlobalIdentifier],
        completionHandler: @escaping (Swift.Result<[GlobalIdentifier], Error>) -> Void
    ) {
        guard original.count > 0 else {
            completionHandler(.success([]))
            return
        }
        
        let descriptorsByGlobalIdentifier = original.filter({ filteringKeys.contains($0.key) })
        
        guard descriptorsByGlobalIdentifier.count > 0 else {
            completionHandler(.success([]))
            return
        }
        
        let localIdentifiersInDescriptors = descriptorsByGlobalIdentifier.values.compactMap({ $0.localIdentifier })
        var phAssetsByLocalIdentifier = [String: PHAsset]()
        
        do {
            let fetchResult = try self.fetchApplePhotosLibraryAssets(for: localIdentifiersInDescriptors)
            fetchResult?.enumerateObjects { phAsset, _, _ in
                phAssetsByLocalIdentifier[phAsset.localIdentifier] = phAsset
            }
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        var filteredGlobalIdentifiers = [GlobalIdentifier]()
        for descriptor in descriptorsByGlobalIdentifier.values {
            if let localId = descriptor.localIdentifier,
               phAssetsByLocalIdentifier.keys.contains(localId) {
                self.delegates.forEach({
                    $0.didIdentify(
                        localAsset: phAssetsByLocalIdentifier[localId]!,
                        correspondingTo: descriptor.globalIdentifier
                    )
                })
            } else {
                filteredGlobalIdentifiers.append(descriptor.globalIdentifier)
            }
        }
        
        completionHandler(.success(filteredGlobalIdentifiers))
    }
    
    ///
    /// Either start download for the assets that are authorized, or request authorization for the ones that need authorization.
    /// A download needs explicit authorization from the user if the sender has never shared an asset with this user before.
    /// Once the link is established, all other downloads won't need authorization.
    ///
    /// - Parameter descriptorsByGlobalIdentifier: the descriptors keyed by their global identifier
    /// - Parameter completionHandler: the callback method
    private func downloadOrRequestAuthorization(
        forAssetsIn descriptors: [any SHAssetDescriptor],
        completionHandler: @escaping (Swift.Result<Void, Error>) -> Void
    ) {
        guard descriptors.count > 0 else {
            completionHandler(.success(()))
            return
        }
        
        var mutableDescriptors = descriptors
        let partitionIndex = mutableDescriptors.partition { descr in
            if descr.sharingInfo.sharedByUserIdentifier == self.user.identifier {
                return true
            }
            do {
                return try SHKGQuery.isKnownUser(withIdentifier: descr.sharingInfo.sharedByUserIdentifier)
            } catch {
                return false
            }
        }
        let unauthorizedDownloadDescriptors = Array(mutableDescriptors[..<partitionIndex])
        let authorizedDownloadDescriptors = Array(mutableDescriptors[partitionIndex...])
        
        self.log.info("found \(descriptors.count) assets on the server. Need to authorize \(unauthorizedDownloadDescriptors.count), can download \(authorizedDownloadDescriptors.count). limit=\(self.limit ?? 0)")
        
        let downloadsManager = SHAssetsDownloadManager(user: self.user)
        
        if unauthorizedDownloadDescriptors.count > 0 {
            ///
            /// For downloads waiting explicit authorization:
            /// - descriptors are added to the unauthorized download queue
            /// - the index of assets to authorized per user is updated (userStore, keyed by `auth-<USER_ID>`)
            /// - the delegate method `handleDownloadAuthorization(ofDescriptors:users:)` is called
            ///
            /// When the authorization comes (via `SHAssetDownloadController::authorizeDownloads(for:completionHandler:)`):
            /// - the downloads will move from the unauthorized to the authorized queue
            /// - the delegate method `handleAssetDescriptorResults(for:user:)` is called
            ///
            downloadsManager.waitForDownloadAuthorization(forDescriptors: unauthorizedDownloadDescriptors) { result in
                switch result {
                case .failure(let error):
                    self.log.warning("failed to enqueue unauthorized download for \(unauthorizedDownloadDescriptors.count) descriptors. \(error.localizedDescription). This operation will be attempted again")
                default: break
                }
            }
            
            var users = [SHServerUser]()
            var userIdentifiers = Set(unauthorizedDownloadDescriptors.flatMap { $0.sharingInfo.sharedWithUserIdentifiersInGroup.keys })
            userIdentifiers.formUnion(Set(unauthorizedDownloadDescriptors.compactMap { $0.sharingInfo.sharedByUserIdentifier }))
            
            do {
                users = try SHUsersController(localUser: self.user).getUsers(withIdentifiers: Array(userIdentifiers))
            } catch {
                self.log.error("Unable to fetch users from local server: \(error.localizedDescription)")
                completionHandler(.failure(error))
                return
            }

            self.delegates.forEach({
                $0.didReceiveAuthorizationRequest(
                    for: unauthorizedDownloadDescriptors,
                    referencing: users
                )
            })
        }
        
        if authorizedDownloadDescriptors.count > 0 {
            ///
            /// For downloads that don't need authorization:
            /// - the delegate method `handleDownloadAuthorization(ofDescriptors:users:)` is called
            /// - descriptors are added to the unauthorized download queue
            /// - the index of assets to authorized per user is updated (userStore, keyed by `auth-<USER_ID>`)
            ///
            downloadsManager.startDownload(of: authorizedDownloadDescriptors,
                                           completionHandler: completionHandler)
        } else {
            completionHandler(.success(()))
        }
    }
    
    internal func processAssetsInDescriptors(
        descriptorsByGlobalIdentifier: [GlobalIdentifier: any SHAssetDescriptor],
        completionHandler: @escaping (Swift.Result<Void, Error>) -> Void
    ) {
        ///
        /// Get all asset descriptors associated with this user from the server.
        /// Descriptors serve as a manifest to determine what to decrypt
        ///
        guard descriptorsByGlobalIdentifier.count > 0 else {
            completionHandler(.success(()))
            return
        }
        
        ///
        /// Create 2 partitions:
        /// - one for assets uploaded/shared by THIS user
        /// - one for assets shared by OTHER users
        ///
        var sharedBySelfGlobalIdentifiers = [GlobalIdentifier]()
        var sharedByOthersGlobalIdentifiers = [GlobalIdentifier]()
        for (globalIdentifier, descriptor) in descriptorsByGlobalIdentifier {
            if descriptor.sharingInfo.sharedByUserIdentifier == self.user.identifier {
                sharedBySelfGlobalIdentifiers.append(globalIdentifier)
            } else {
                sharedByOthersGlobalIdentifiers.append(globalIdentifier)
            }
        }
        
        ///
        /// Handle the assets that are present in the local library differently
        /// These don't need to be downloaded.
        /// The delegate needs to be notified that they should be marked as "uploaded on the server"
        ///
        self.mergeDescriptorsWithApplePhotosAssets(
            descriptorsByGlobalIdentifier: descriptorsByGlobalIdentifier,
            filteringKeys: sharedBySelfGlobalIdentifiers
        ) { result in
            switch result {
            case .success(let nonLocalPhotoLibraryGlobalIdentifiers):
                let start = CFAbsoluteTimeGetCurrent()
                
                ///
                /// Get the encrypted assets for the ones not found in the apple photos library to start decryption.
                ///
                self.processForDownload(
                    descriptorsByGlobalIdentifier: descriptorsByGlobalIdentifier,
                    nonApplePhotoLibrarySharedBySelfGlobalIdentifiers: nonLocalPhotoLibraryGlobalIdentifiers,
                    sharedBySelfGlobalIdentifiers: sharedBySelfGlobalIdentifiers,
                    sharedByOthersGlobalIdentifiers: sharedByOthersGlobalIdentifiers
                ) { result in
                    let end = CFAbsoluteTimeGetCurrent()
                    self.log.debug("[localDownload][PERF] it took \(CFAbsoluteTime(end - start)) to process \(descriptorsByGlobalIdentifier.count) asset descriptors")
                    completionHandler(result)
                }
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    func processForDownload(
        descriptorsByGlobalIdentifier: [GlobalIdentifier: any SHAssetDescriptor],
        nonApplePhotoLibrarySharedBySelfGlobalIdentifiers: [GlobalIdentifier],
        sharedBySelfGlobalIdentifiers: [GlobalIdentifier],
        sharedByOthersGlobalIdentifiers: [GlobalIdentifier],
        completionHandler: @escaping (Swift.Result<Void, Error>) -> Void
    ) {
        
        ///
        /// First find out which assets are already on the local server.
        /// These will not be processed, because the sync background operation will take care of keeping them in sync.
        /// See `SHSyncOperation`
        ///
        serverProxy.localServer.getAssetDescriptors(
            forAssetGlobalIdentifiers: Array(descriptorsByGlobalIdentifier.keys)
        ) { result in
            var globalIdentifiersOnLocalServer = [GlobalIdentifier]()
            if case .success(let descrs) = result {
                globalIdentifiersOnLocalServer = descrs.map({ $0.globalIdentifier })
            }
            
            guard descriptorsByGlobalIdentifier.count > globalIdentifiersOnLocalServer.count else {
                completionHandler(.success(()))
                return
            }
            
            let group = DispatchGroup()
            var errors = [Error]()

            group.enter()
            
            ///
            /// FOR THE ONES SHARED BY THIS USER
            /// Download them directly from server if:
            /// - They are not in the local library
            /// - They are not in the local server
            /// - They are shared by this user
            ///
            let globalIdentifiersNotOnLocalSharedBySelfNotOnPhotoLibrary = Set(nonApplePhotoLibrarySharedBySelfGlobalIdentifiers).subtracting(globalIdentifiersOnLocalServer)
            let downloadsManager = SHAssetsDownloadManager(user: self.user)
            downloadsManager.startDownload(
                of: globalIdentifiersNotOnLocalSharedBySelfNotOnPhotoLibrary.compactMap({ descriptorsByGlobalIdentifier[$0] })
            ) { result in
                if case .failure(let failure) = result {
                    errors.append(failure)
                }
                group.leave()
            }
            
            ///
            /// FOR THE ONES SHARED BY OTHER USERS
            /// Decrypt the remainder from the local store, namely assets:
            /// - not shared by self
            /// - not in the apple photos library (we'll use the apple photos version as long as it exists)
            ///
            let globalIdentifiersNotOnLocalSharedByOthers = Set(sharedByOthersGlobalIdentifiers).subtracting(globalIdentifiersOnLocalServer)
            
            group.enter()
            self.downloadOrRequestAuthorization(
                forAssetsIn: globalIdentifiersNotOnLocalSharedByOthers.compactMap({ descriptorsByGlobalIdentifier[$0] })
            ) { result in
                if case .failure(let failure) = result {
                    errors.append(failure)
                }
                group.leave()
            }
            
            group.notify(queue: DispatchQueue.global()) {
                if errors.count > 0 {
                    self.log.error("failed downloading assets with errors: \(errors.map({ $0.localizedDescription }).joined(separator: ","))")
                    completionHandler(.failure(errors.first!))
                } else {
                    completionHandler(.success(()))
                }
            }
        }
    }
    
    private func downloadAssets(completionHandler: @escaping (Swift.Result<Void, Error>) -> Void) {
        do {
            var count = 1
            guard let queue = try? BackgroundOperationQueue.of(type: .download) else {
                self.log.error("[downloadAssets] unable to connect to local queue or database")
                completionHandler(.failure(SHBackgroundOperationError.fatalError("Unable to connect to local queue or database")))
                return
            }
            
            while let item = try queue.peek() {
                let start = CFAbsoluteTimeGetCurrent()
                
                log.info("[downloadAssets] downloading assets from descriptors in item \(count), with identifier \(item.identifier) created at \(item.createdAt)")
                
                guard let downloadRequest = try? content(ofQueueItem: item) as? SHDownloadRequestQueueItem else {
                    log.error("[downloadAssets] unexpected data found in DOWNLOAD queue. Dequeueing")
                    
                    do { _ = try queue.dequeue() }
                    catch {
                        log.warning("[downloadAssets] dequeuing failed of unexpected data in DOWNLOAD. ATTENTION: this operation will be attempted again.")
                    }
                    
                    continue
                }
                
                guard DownloadBlacklist.shared.isBlacklisted(assetGlobalIdentifier: downloadRequest.assetDescriptor.globalIdentifier) == false else {
                    self.log.info("[downloadAssets] skipping item \(downloadRequest.assetDescriptor.globalIdentifier) because it was attempted too many times")
                    
                    do { _ = try queue.dequeue() }
                    catch {
                        log.warning("[downloadAssets] dequeuing failed of unexpected data in DOWNLOAD. ATTENTION: this operation will be attempted again.")
                    }
                    
                    continue
                }
                
                let globalIdentifier = downloadRequest.assetDescriptor.globalIdentifier
                let descriptor = downloadRequest.assetDescriptor
                
                // MARK: Start
                
                for groupId in descriptor.sharingInfo.groupInfoById.keys {
                    self.delegates.forEach({
                        $0.didStartDownloadOfAsset(withGlobalIdentifier: globalIdentifier, in: groupId)
                    })
                }
                
                let group = DispatchGroup()
                var shouldContinue = true
                
                // MARK: Get Low Res asset
                
                group.enter()
                self.fetchAsset(withGlobalIdentifier: globalIdentifier,
                                quality: .lowResolution,
                                request: downloadRequest) { result in
                    switch result {
                    case .success(let decryptedAsset):
                        DownloadBlacklist.shared.removeFromBlacklist(assetGlobalIdentifier: globalIdentifier)
                        
                        self.delegates.forEach({
                            $0.didFetchLowResolutionAsset(decryptedAsset)
                        })
                        for groupId in descriptor.sharingInfo.groupInfoById.keys {
                            self.delegates.forEach({
                                $0.didCompleteDownloadOfAsset(
                                    withGlobalIdentifier: decryptedAsset.globalIdentifier,
                                    in: groupId
                                )
                            })
                        }
                    case .failure(let error):
                        shouldContinue = false
                        
                        // Record the failure for the asset
                        if error is SHCypher.DecryptionError {
                            DownloadBlacklist.shared.blacklist(globalIdentifier: globalIdentifier)
                        } else {
                            DownloadBlacklist.shared.recordFailedAttempt(globalIdentifier: globalIdentifier)
                        }
                        
                        for groupId in descriptor.sharingInfo.groupInfoById.keys {
                            self.delegates.forEach({
                                $0.didFailDownloadOfAsset(
                                    withGlobalIdentifier: globalIdentifier,
                                    in: groupId,
                                    with: error
                                )
                            })
                            if DownloadBlacklist.shared.isBlacklisted(assetGlobalIdentifier: globalIdentifier) {
                                self.delegates.forEach({
                                    $0.didFailRepeatedlyDownloadOfAsset(
                                        withGlobalIdentifier: globalIdentifier,
                                        in: groupId
                                    )
                                })
                            }
                        }
                    }
                    group.leave()
                }
                
                let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDownloadTimeoutInMilliseconds))
                guard dispatchResult == .success, shouldContinue == true else {
                    do { _ = try queue.dequeue() }
                    catch {
                        log.warning("[downloadAssets] dequeuing failed of unexpected data in DOWNLOAD. ATTENTION: this operation will be attempted again.")
                    }
                    
                    continue
                }
                
                let end = CFAbsoluteTimeGetCurrent()
                log.debug("[downloadAssets][PERF] it took \(CFAbsoluteTime(end - start)) to download the asset")
                
                do { _ = try queue.dequeue() }
                catch {
                    log.warning("[downloadAssets] asset \(globalIdentifier) was downloaded but dequeuing failed, so this operation will be attempted again.")
                }
                
                count += 1
                
                guard !self.isCancelled else {
                    log.info("[downloadAssets] download task cancelled. Finishing")
                    state = .finished
                    break
                }
            }
            
            completionHandler(.success(()))
            
        } catch {
            log.error("[downloadAssets] error executing download task: \(error.localizedDescription)")
            completionHandler(.failure(error))
        }
    }
    
    private func runOnce(completionHandler: @escaping (Swift.Result<Void, Error>) -> Void) {
        
        ///
        /// Get all asset descriptors associated with this user from the server.
        /// Descriptors serve as a manifest to determine what to download.
        ///
        self.processDescriptors { descResult in
            switch descResult {
            case .failure(let error):
                self.log.error("failed to download descriptors: \(error.localizedDescription)")
                completionHandler(.failure(error))
            case .success(let descriptorsByGlobalIdentifier):
                self.processAssetsInDescriptors(
                    descriptorsByGlobalIdentifier: descriptorsByGlobalIdentifier
                ) { descAssetResult in
                    switch descAssetResult {
                    case .failure(let error):
                        self.log.error("failed to process assets in descriptors: \(error.localizedDescription)")
                        completionHandler(.failure(error))
                    case .success():
                        ///
                        /// Get all asset descriptors associated with this user from the server.
                        /// Descriptors serve as a manifest to determine what to download
                        ///
                        self.downloadAssets { result in
                            if case .failure(let error) = result {
                                self.log.error("failed to download assets: \(error.localizedDescription)")
                                completionHandler(.failure(error))
                            } else {
                                completionHandler(.success(()))
                            }
                        }
                    }
                }
            }
        }
    }
    
    public override func main() {
        guard !self.isCancelled else {
            state = .finished
            return
        }
        
        state = .executing
        
        self.runOnce { result in
            self.delegates.forEach({
                $0.didCompleteDownloadCycle(with: result)
            })
            self.state = .finished
        }
    }
}

// MARK: History Queue updates based on Server Descriptors

//extension SHDownloadOperation {
//    
//    ///
//    /// Based on a set of asset descriptors fetch from server, update the local history queues (`UploadHistoryQueue` and `ShareHistoryQueue`).
//    /// For instance, an asset marked as backed up on server might result as not backed up on client.
//    /// This method will ensure that the upload event on server will result in an entry in the UploadHistoryQueue,
//    /// and the share event will result in an entry in the ShareHistoryQueue.
//    ///
//    /// - Parameters:
//    ///   - descriptorsByLocalIdentifier: the list of server asset descriptors keyed by localIdentifier
//    ///   - users: the manifest of user details fetched from server
//    ///
//    private func updateHistoryQueues(with descriptorsByLocalIdentifier: [String: any SHAssetDescriptor],
//                                    users: [SHServerUser]) {
//        guard let queue = try? BackgroundOperationQueue.of(type: .successfulUpload) else {
//            self.log.error("[sync] unable to connect to local queue or database")
//            return
//        }
//        
//        for (localIdentifier, descriptor) in descriptorsByLocalIdentifier {
//            let condition = KBGenericCondition(.beginsWith, value: SHQueueOperation.queueIdentifier(for: localIdentifier))
//            if let keys = try? queue.keys(matching: condition),
//               keys.count > 0 {
//                ///
//                /// Nothing to do, the asset is already marked as uploaded in the queue
//                ///
//            } else {
//                ///
//                /// Determine group, event originator and shared with from `descriptor.sharingInfo.sharedByUserIdentifier`
//                ///
//                let eventOriginator = users.first(where: { $0.identifier == descriptor.sharingInfo.sharedByUserIdentifier })
//                
//                guard let eventOriginator = eventOriginator,
//                      eventOriginator.identifier == self.user.identifier else {
//                    log.warning("[sync] can't mark a local asset as backed up if not owned by this user \(self.user.name)")
//                    break
//                }
//                
//                var groupId: String? = nil
//                for (userId, gid) in descriptor.sharingInfo.sharedWithUserIdentifiersInGroup {
//                    if userId == eventOriginator.identifier {
//                        groupId = gid
//                        break
//                    }
//                }
//                
//                guard let groupId = groupId else {
//                    log.warning("[sync] the asset descriptor sharing information doesn't seem to include the event originator")
//                    break
//                }
//                
//                let sharedWith = descriptor.sharingInfo.sharedWithUserIdentifiersInGroup
//                    .keys
//                    .map { userIdentifier in users.first(where: { user in user.identifier == userIdentifier } )! }
//                
//                // TODO: This is a best effort to recover state from Server, but it will still result in incorrect event dates, because of the lack of an API in KnowledgeBase.framework to enqueue an item with a specific timestamp
//                /// The timestamp should be retrieved from `descriptor.sharingInfo.groupInfoById[groupId]`
//                
////                let item = SHUploadHistoryItem(
////                    localIdentifier: localIdentifier,
////                    groupId: groupId,
////                    eventOriginator: eventOriginator,
////                    sharedWith: sharedWith
////                )
////
////                try? item.enqueue(in: UploadHistoryQueue, with: localIdentifier)
////                for delegate in self.outboundDelegates {
////                    if let delegate = delegate as? SHAssetUploaderDelegate {
////                        delegate.didCompleteUpload(
////                            itemWithLocalIdentifier: localIdentifier,
////                            globalIdentifier: descriptor.globalIdentifier,
////                            groupId: groupId
////                        )
////                    }
////                    if sharedWith.filter({ $0.identifier != self.user.identifier}).count > 0 {
////                        if let delegate = delegate as? SHAssetSharerDelegate {
////                            delegate.didCompleteSharing(
////                                itemWithLocalIdentifier: localIdentifier,
////                                globalIdentifier: descriptor.globalIdentifier,
////                                groupId: groupId,
////                                with: sharedWith
////                            )
////                        }
////                    }
////                }
//            }
//        }
//    }
//}

// MARK: - Download Operation Processor

public class SHAssetsDownloadQueueProcessor : SHBackgroundOperationProcessor<SHDownloadOperation> {
    
    public static var shared = SHAssetsDownloadQueueProcessor(
        delayedStartInSeconds: 0,
        dispatchIntervalInSeconds: 7
    )
    private override init(delayedStartInSeconds: Int,
                          dispatchIntervalInSeconds: Int? = nil) {
        super.init(delayedStartInSeconds: delayedStartInSeconds, dispatchIntervalInSeconds: dispatchIntervalInSeconds)
    }
}
