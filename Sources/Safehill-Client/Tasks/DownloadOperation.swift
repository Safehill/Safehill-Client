import Foundation
import Safehill_Crypto
import KnowledgeBase
import os
import Photos

protocol SHDownloadOperationProtocol {}


public class SHDownloadOperation: SHAbstractBackgroundOperation, SHBackgroundQueueProcessorOperationProtocol, SHDownloadOperationProtocol {
    
    public let log = Logger(subsystem: "com.safehill", category: "BG-DOWNLOAD")
    
    public let limit: Int?
    let user: SHLocalUserProtocol
    
    let downloaderDelegates: [SHAssetDownloaderDelegate]
    let assetsSyncDelegates: [SHAssetSyncingDelegate]
    let threadsSyncDelegates: [SHThreadSyncingDelegate]
    let restorationDelegate: SHAssetActivityRestorationDelegate
    
    let photoIndexer: SHPhotosIndexer
    
    public init(user: SHLocalUserProtocol,
                downloaderDelegates: [SHAssetDownloaderDelegate],
                assetsSyncDelegates: [SHAssetSyncingDelegate],
                threadsSyncDelegates: [SHThreadSyncingDelegate],
                restorationDelegate: SHAssetActivityRestorationDelegate,
                limitPerRun limit: Int? = nil,
                photoIndexer: SHPhotosIndexer? = nil) {
        self.user = user
        self.limit = limit
        self.downloaderDelegates = downloaderDelegates
        self.assetsSyncDelegates = assetsSyncDelegates
        self.threadsSyncDelegates = threadsSyncDelegates
        self.restorationDelegate = restorationDelegate
        self.photoIndexer = photoIndexer ?? SHPhotosIndexer()
    }
    
    var serverProxy: SHServerProxy { self.user.serverProxy }
    
    public func clone() -> SHBackgroundOperationProtocol {
        SHDownloadOperation(
            user: self.user,
            downloaderDelegates: self.downloaderDelegates,
            assetsSyncDelegates: self.assetsSyncDelegates,
            threadsSyncDelegates: self.threadsSyncDelegates,
            restorationDelegate: self.restorationDelegate,
            limitPerRun: self.limit,
            photoIndexer: self.photoIndexer
        )
    }
    
    private func fetchAsset(withGlobalIdentifier globalIdentifier: GlobalIdentifier,
                            quality: SHAssetQuality,
                            descriptor: any SHAssetDescriptor,
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
                        descriptor: descriptor
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
        return try self.fetchDescriptorsFromServer(for: nil)
    }
    
    internal func fetchDescriptorsFromServer(
        for globalIdentifiers: [GlobalIdentifier]? = nil
    ) throws -> [any SHAssetDescriptor] {
        let group = DispatchGroup()
        
        var descriptors = [any SHAssetDescriptor]()
        var error: Error? = nil
        
        group.enter()
        serverProxy.getRemoteAssetDescriptors(for: globalIdentifiers) { result in
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
    
    internal func getUsers(
        withIdentifiers userIdentifiers: [UserIdentifier]
    ) throws -> [UserIdentifier: any SHServerUser] {
        try SHUsersController(localUser: self.user).getUsers(withIdentifiers: userIdentifiers)
    }
    
    ///
    /// Fetch descriptors from server (remote or local, depending on whether it's running as part of the
    /// `SHLocalActivityRestoreOperation` or the `SHDownloadOperation`.
    /// Filters out the blacklisted assets and users, as well as the non-completed uploads.
    /// Call the delegate with the full manifest of assets shared by OTHER users, regardless of the limit on the task config) for the assets.
    /// Return a tuple with the following values:
    /// 1. the full set of descriptors fetched from the server, keyed by global identifier, limiting the result based on the task config.
    /// 2. the globalIdentifiers whose sender is either self or is a known, authorized user. We return this information here so we have to query the graph once.
    ///
    /// - Parameters:
    ///   - descriptors: the descriptors to process
    ///   - completionHandler: the callback
    ///
    internal func processDescriptors(
        _ descriptors: [any SHAssetDescriptor],
        priority: TaskPriority,
        completionHandler: @escaping (Result<[GlobalIdentifier: any SHAssetDescriptor], Error>) -> Void
    ) {
        ///
        /// Filter out the ones:
        /// - whose assets were blacklisted
        /// - whose users were blacklisted
        /// - haven't started upload (`.notStarted` is only relevant for the `SHLocalActivityRestoreOperation`)
        ///
        Task(priority: priority) {
            let globalIdentifiers = descriptors.map({ $0.globalIdentifier })
            var senderIds = descriptors.map({ $0.sharingInfo.sharedByUserIdentifier })
            let blacklistedAssets = await SHDownloadBlacklist.shared.areBlacklisted(
                assetGlobalIdentifiers: globalIdentifiers
            )
            let blacklistedUsers = await SHDownloadBlacklist.shared.areBlacklisted(
                userIdentifiers: senderIds
            )
            
            let filteredDescriptors = descriptors.filter {
                (blacklistedAssets[$0.globalIdentifier] ?? false) == false
                && (blacklistedUsers[$0.sharingInfo.sharedByUserIdentifier] ?? false) == false
                && $0.uploadState == .completed || $0.uploadState == .partial
            }
            
            guard filteredDescriptors.count > 0 else {
                self.downloaderDelegates.forEach({
                    $0.didReceiveAssetDescriptors([], referencing: [:])
                })
                completionHandler(.success([:]))
                return
            }
            
            /// When calling the delegate method `didReceiveAssetDescriptors(_:referencing:)`
            /// filter out the ones whose sender is unknown.
            /// The delegate method `didReceiveAuthorizationRequest(for:referencing:)` will take care of those.
            senderIds = filteredDescriptors.map({ $0.sharingInfo.sharedByUserIdentifier })
            let knownUsers: [UserIdentifier: Bool]
            do { knownUsers = try SHKGQuery.areUsersKnown(withIdentifiers: senderIds) }
            catch {
                log.error("failed to read from the graph to fetch \"known user\" information. Terminating download operation early. \(error.localizedDescription)")
                completionHandler(.failure(error))
                return
            }
            
            var descriptorsFromKnownUsers = filteredDescriptors.filter {
                knownUsers[$0.sharingInfo.sharedByUserIdentifier] ?? false
            }
            
            ///
            /// Fetch from server users information (`SHServerUser` objects)
            /// for all user identifiers found in all descriptors shared by OTHER known users
            ///
            var usersDict = [UserIdentifier: any SHServerUser]()
            var userIdentifiers = Set(descriptorsFromKnownUsers.flatMap { $0.sharingInfo.sharedWithUserIdentifiersInGroup.keys })
            userIdentifiers.formUnion(Set(descriptorsFromKnownUsers.compactMap { $0.sharingInfo.sharedByUserIdentifier }))
            
            do {
                usersDict = try self.getUsers(withIdentifiers: Array(userIdentifiers))
            } catch {
                self.log.error("Unable to fetch users from local server: \(error.localizedDescription)")
                completionHandler(.failure(error))
                return
            }
            
            ///
            /// Filter out the descriptor that reference any user that could not be retrieved.
            /// For those, we expect the `SHDownloadOperation` to process them
            ///
            descriptorsFromKnownUsers = descriptorsFromKnownUsers.filter { desc in
                if usersDict[desc.sharingInfo.sharedByUserIdentifier] == nil {
                    return false
                }
                for sharedWithUserId in desc.sharingInfo.sharedWithUserIdentifiersInGroup.keys {
                    if usersDict[sharedWithUserId] == nil {
                        return false
                    }
                }
                return true
            }
            
            ///
            /// Call the delegate with the full manifest of whitelisted assets **ONLY for the assets shared by other known users**.
            /// The ones shared by THIS user will be restored through the restoration delegate.
            ///
            self.downloaderDelegates.forEach({
                $0.didReceiveAssetDescriptors(descriptorsFromKnownUsers,
                                              referencing: usersDict)
            })
            
            var descriptorsByGlobalIdentifier = [String: any SHAssetDescriptor]()
            for descriptor in filteredDescriptors {
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
        completionHandler: @escaping (Result<[GlobalIdentifier], Error>) -> Void
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
                self.downloaderDelegates.forEach({
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
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        guard descriptors.count > 0 else {
            completionHandler(.success(()))
            return
        }
        
        let senderIds = descriptors.map({ $0.sharingInfo.sharedByUserIdentifier })
        let knownUsers: [UserIdentifier: Bool]
        do { knownUsers = try SHKGQuery.areUsersKnown(withIdentifiers: senderIds) }
        catch {
            log.error("failed to read from the graph to fetch \"known user\" information. Terminating authorization request operation early. \(error.localizedDescription)")
            completionHandler(.failure(error))
            return
        }
        
        var mutableDescriptors = descriptors
        let partitionIndex = mutableDescriptors.partition {
            knownUsers[$0.sharingInfo.sharedByUserIdentifier] ?? false
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
            
            var usersDict = [UserIdentifier: any SHServerUser]()
            var userIdentifiers = Set(unauthorizedDownloadDescriptors.flatMap { $0.sharingInfo.sharedWithUserIdentifiersInGroup.keys })
            userIdentifiers.formUnion(Set(unauthorizedDownloadDescriptors.compactMap { $0.sharingInfo.sharedByUserIdentifier }))
            
            do {
                usersDict = try SHUsersController(localUser: self.user).getUsers(
                    withIdentifiers: Array(userIdentifiers)
                )
            } catch {
                self.log.error("Unable to fetch users from local server: \(error.localizedDescription)")
                completionHandler(.failure(error))
                return
            }

            self.downloaderDelegates.forEach({
                $0.didReceiveAuthorizationRequest(
                    for: unauthorizedDownloadDescriptors,
                    referencing: usersDict
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
        completionHandler: @escaping (Result<Void, Error>) -> Void
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
                    self.log.debug("[PERF] it took \(CFAbsoluteTime(end - start)) to process \(descriptorsByGlobalIdentifier.count) asset descriptors")
                    completionHandler(result)
                }
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    private func recreateLocalAssetsAndQueueItems(for globalIdentifiers: [GlobalIdentifier],
                                                  descriptorsByGlobalIdentifier: [GlobalIdentifier: any SHAssetDescriptor]) {
        guard globalIdentifiers.count > 0, descriptorsByGlobalIdentifier.count > 0 else {
            return
        }
        
        ///
        /// Get the `.lowResolution` assets data from the remote server
        ///
        self.serverProxy.remoteServer.getAssets(
            withGlobalIdentifiers: globalIdentifiers,
            versions: [.lowResolution]
        ) { fetchResult in
            switch fetchResult {
            case .success(let assetsDict):
                ///
                /// Create the `SHEncryptedAsset` in the local server
                ///
                self.serverProxy.localServer.create(
                    assets: Array(assetsDict.values),
                    descriptorsByGlobalIdentifier: descriptorsByGlobalIdentifier,
                    uploadState: .completed
                ) { localCreationResult in
                    switch localCreationResult {
                    case .success(_):
                        do {
                            let usersReferenced = try self.serverProxy
                                .getUsers(inAssetDescriptors: Array(descriptorsByGlobalIdentifier.values))
                            
                            ///
                            /// Re-create the successful upload and share queue items
                            ///
                            self.restoreQueueItems(
                                descriptorsByGlobalIdentifier: descriptorsByGlobalIdentifier,
                                usersReferenced: usersReferenced
                            )
                        } catch {
                            self.log.warning("[downloadAssets] failed to fetch users from remote server when restoring items in local library but not in local server")
                        }
                        
                    case .failure(let error):
                        self.log.warning("[downloadAssets] failed to create assets in local server. Assets in the local library but uploaded will not be marked as such. This operation will be attempted again. \(error.localizedDescription)")
                    }
                }
            case .failure(let error):
                self.log.warning("[downloadAssets] failed to fetch assets from remote server. Assets in the local library but uploaded will not be marked as such. This operation will be attempted again. \(error.localizedDescription)")
            }
        }
    }
    
    private func restoreQueueItems(descriptorsByGlobalIdentifier: [GlobalIdentifier: any SHAssetDescriptor],
                                   usersReferenced: [any SHServerUser]) {
        guard descriptorsByGlobalIdentifier.count > 0 else {
            return
        }
        
        let myUser = self.user
        let otherUsersById: [String: any SHServerUser] = usersReferenced.reduce([:]) { partialResult, user in
            var result = partialResult
            result[user.identifier] = user
            return result
        }
        
        let remoteServerDescriptorByAssetGid = descriptorsByGlobalIdentifier.filter({
            $0.value.sharingInfo.sharedByUserIdentifier == myUser.identifier
            && $0.value.localIdentifier != nil
        })
        
        self.restorationDelegate.didStartRestoration()
        var userIdsInvolvedInRestoration = Set<String>()
        
        for remoteDescriptor in remoteServerDescriptorByAssetGid.values {
            var uploadLocalAssetIdByGroupId = [String: Set<String>]()
            var shareLocalAssetIdsByGroupId = [String: Set<String>]()
            var groupIdToUploadItem = [String: (SHUploadHistoryItem, Date)]()
            var groupIdToShareItem = [String: (SHShareHistoryItem, Date)]()
            
            for (recipientUserId, groupId) in remoteDescriptor.sharingInfo.sharedWithUserIdentifiersInGroup {
                let localIdentifier = remoteDescriptor.localIdentifier!
                let groupCreationDate = remoteDescriptor.sharingInfo.groupInfoById[groupId]?.createdAt ?? Date()
                
                if recipientUserId == myUser.identifier {
                    if uploadLocalAssetIdByGroupId[groupId] == nil {
                        uploadLocalAssetIdByGroupId[groupId] = [localIdentifier]
                    } else {
                        uploadLocalAssetIdByGroupId[groupId]!.insert(localIdentifier)
                    }
                    
                    let item = SHUploadHistoryItem(
                        localAssetId: localIdentifier,
                        globalAssetId: remoteDescriptor.globalIdentifier,
                        versions: [.lowResolution, .hiResolution],
                        groupId: groupId,
                        eventOriginator: myUser,
                        sharedWith: [],
                        isBackground: false
                    )
                    groupIdToUploadItem[groupId] = (item, groupCreationDate)
                } else {
                    guard let user = otherUsersById[recipientUserId] else {
                        log.critical("[downloadAssets] inconsistency between user ids referenced in descriptors and user objects returned from server")
                        continue
                    }
                    
                    if shareLocalAssetIdsByGroupId[groupId] == nil {
                        shareLocalAssetIdsByGroupId[groupId] = [localIdentifier]
                    } else {
                        shareLocalAssetIdsByGroupId[groupId]!.insert(localIdentifier)
                    }
                    if groupIdToShareItem[groupId] == nil {
                        let item = SHShareHistoryItem(
                            localAssetId: localIdentifier,
                            globalAssetId: remoteDescriptor.globalIdentifier,
                            versions: [.lowResolution, .hiResolution],
                            groupId: groupId,
                            eventOriginator: myUser,
                            sharedWith: [user],
                            isBackground: false
                        )
                        groupIdToShareItem[groupId] = (item, groupCreationDate)
                        
                        userIdsInvolvedInRestoration.insert(user.identifier)
                    } else {
                        var users = [any SHServerUser]()
                        users.append(contentsOf: groupIdToShareItem[groupId]!.0.sharedWith)
                        users.append(user)
                        let item = SHShareHistoryItem(
                            localAssetId: localIdentifier,
                            globalAssetId: remoteDescriptor.globalIdentifier,
                            versions: [.lowResolution, .hiResolution],
                            groupId: groupId,
                            eventOriginator: myUser,
                            sharedWith: users,
                            isBackground: false
                        )
                        groupIdToShareItem[groupId] = (item, groupCreationDate)
                        
                        for user in users {
                            userIdsInvolvedInRestoration.insert(user.identifier)
                        }
                    }
                }
            }
            
            guard groupIdToUploadItem.count + groupIdToShareItem.count > 0 else {
                continue
            }
            
            for (uploadItem, timestamp) in groupIdToUploadItem.values {
                if (try? uploadItem.insert(
                    in: BackgroundOperationQueue.of(type: .successfulUpload),
                    at: timestamp
                )) == nil {
                    log.warning("[downloadAssets] unable to enqueue successful upload item groupId=\(uploadItem.groupId), localIdentifier=\(uploadItem.localIdentifier)")
                }
            }
            for (shareItem, timestamp) in groupIdToShareItem.values {
                if (try? shareItem.insert(
                    in: BackgroundOperationQueue.of(type: .successfulShare),
                    at: timestamp
                )) == nil {
                    log.warning("[downloadAssets] unable to enqueue successful share item groupId=\(shareItem.groupId), localIdentifier=\(shareItem.localIdentifier)")
                }
            }

            log.debug("[downloadAssets] upload local asset identifiers by group \(uploadLocalAssetIdByGroupId)")
            log.debug("[downloadAssets] share local asset identifiers by group \(shareLocalAssetIdsByGroupId)")

            for (groupId, localIdentifiers) in uploadLocalAssetIdByGroupId {
                self.restorationDelegate.restoreUploadQueueItems(
                    forLocalIdentifiers: Array(localIdentifiers),
                    in: groupId
                )
            }

            for (groupId, localIdentifiers) in shareLocalAssetIdsByGroupId {
                self.restorationDelegate.restoreShareQueueItems(
                    forLocalIdentifiers: Array(localIdentifiers),
                    in: groupId
                )
            }
        }
        
        self.restorationDelegate.didCompleteRestoration(userIdsInvolvedInRestoration: Array(userIdsInvolvedInRestoration))
    }
    
    func processForDownload(
        descriptorsByGlobalIdentifier: [GlobalIdentifier: any SHAssetDescriptor],
        nonApplePhotoLibrarySharedBySelfGlobalIdentifiers: [GlobalIdentifier],
        sharedBySelfGlobalIdentifiers: [GlobalIdentifier],
        sharedByOthersGlobalIdentifiers: [GlobalIdentifier],
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        
        ///
        /// First find out which assets are already on the local server.
        /// These will not be processed, because the sync background operation will take care of keeping them in sync.
        /// See `SHSyncOperation`
        ///
        
        let globalIdentifiersOnRemoteServer = Array(descriptorsByGlobalIdentifier.keys)
        serverProxy.localServer.getAssetDescriptors(
            forAssetGlobalIdentifiers: globalIdentifiersOnRemoteServer
        ) { result in
            var remoteGlobalIdentifiersAlsoOnLocalServer = [GlobalIdentifier]()
            if case .success(let descrs) = result {
                remoteGlobalIdentifiersAlsoOnLocalServer = descrs.map({ $0.globalIdentifier })
            }
            
            guard globalIdentifiersOnRemoteServer.count > remoteGlobalIdentifiersAlsoOnLocalServer.count else {
                completionHandler(.success(()))
                return
            }
            
            let group = DispatchGroup()
            var errors = [Error]()
            
            ///
            /// FOR THE ONES SHARED BY THIS USER
            /// Assets that are already in the local server are filtered out at this point.
            /// So we only deal with assets that:
            /// - are shared by THIS user
            /// - are not in the local server
            ///
            /// There's 2 sub-cases:
            /// 1. assets are in the Apple Photos library (shared by this user and in the Photos library, but not in the local server)
            ///     - This can happen when:
            ///         - they were shared from a different device (so this device doesn't have a record of that Photo Library photo being uploaded)
            ///         - the user signed out and wiped all the local asset information, including photos that were in the Photo Library
            /// 2. assets are not in the Apple Photos library (shared by this user and NOT in the Photos library and not in the local server)
            ///     - This can happen when:
            ///         - they were deleted from the local library
            ///         - they were shared from a different device and they are not on this device's Photo Library
            ///
            /// (2) is handled by downloading them as regular downloads from other users, with the only difference that authorization is skipped
            ///
            /// For (1), identification `didIdentify(localAsset:correspondingTo:)` happens in `mergeDescriptorsWithApplePhotosAssets(descriptorsByGlobalIdentifier:filteringKeys:completionHandler:)`, so we only need to take care of adding the asset to the local server, add items to the success queue (upload and share) and call the restoration delegate.
            ///
            
            /// (1)
            let globalIdentifiersNotOnLocalSharedBySelfInApplePhotoLibrary = sharedBySelfGlobalIdentifiers
                .subtract(remoteGlobalIdentifiersAlsoOnLocalServer)
                .subtract(nonApplePhotoLibrarySharedBySelfGlobalIdentifiers)
            
            self.recreateLocalAssetsAndQueueItems(
                for: globalIdentifiersNotOnLocalSharedBySelfInApplePhotoLibrary,
                descriptorsByGlobalIdentifier: descriptorsByGlobalIdentifier
            )
            
            /// (2)
            let globalIdentifiersNotOnLocalSharedBySelfNotInApplePhotoLibrary = nonApplePhotoLibrarySharedBySelfGlobalIdentifiers.subtract(remoteGlobalIdentifiersAlsoOnLocalServer)
            
            let downloadsManager = SHAssetsDownloadManager(user: self.user)
            
            group.enter()
            downloadsManager.startDownload(
                of: globalIdentifiersNotOnLocalSharedBySelfNotInApplePhotoLibrary.compactMap({ descriptorsByGlobalIdentifier[$0] })
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
            /// - not in the Apple Photos Library (we'll use the apple photos version as long as it exists)
            ///
            let globalIdentifiersNotOnLocalSharedByOthers = sharedByOthersGlobalIdentifiers
                .subtract(remoteGlobalIdentifiersAlsoOnLocalServer)
            
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
                    self.log.error("[downloadAssets] failed downloading assets with errors: \(errors.map({ $0.localizedDescription }).joined(separator: ","))")
                    completionHandler(.failure(errors.first!))
                } else {
                    completionHandler(.success(()))
                }
            }
        }
    }
    
    private func downloadAssets(completionHandler: @escaping (Result<Void, Error>) -> Void) {
        Task(priority: .medium) {
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
                    
                    let globalIdentifier = downloadRequest.assetDescriptor.globalIdentifier
                    let descriptor = downloadRequest.assetDescriptor
                    
                    // MARK: Start
                    
                    let groupId = descriptor.sharingInfo.sharedWithUserIdentifiersInGroup[self.user.identifier]!
                    self.downloaderDelegates.forEach({
                        $0.didStartDownloadOfAsset(
                            withGlobalIdentifier: globalIdentifier,
                            descriptor: descriptor,
                            in: groupId
                        )
                    })
                    
                    guard await SHDownloadBlacklist.shared.isBlacklisted(assetGlobalIdentifier: downloadRequest.assetDescriptor.globalIdentifier) == false else {
                        self.log.info("[downloadAssets] skipping item \(downloadRequest.assetDescriptor.globalIdentifier) because it was attempted too many times")
                        
                        do {
                            let downloadsManager = SHAssetsDownloadManager(user: self.user)
                            try downloadsManager.stopDownload(ofAssetsWith: [globalIdentifier])
                        } catch {
                            log.warning("[downloadAssets] dequeuing failed of unexpected data in DOWNLOAD. ATTENTION: this operation will be attempted again.")
                        }
                        
                        continue
                    }
                    
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
                    
                    // MARK: Get Low Res asset
                    
                    self.fetchAsset(withGlobalIdentifier: globalIdentifier,
                                    quality: .lowResolution,
                                    descriptor: downloadRequest.assetDescriptor) { result in
                        
                        switch result {
                        case .success(let decryptedAsset):
                            self.downloaderDelegates.forEach({
                                $0.didFetchLowResolutionAsset(decryptedAsset)
                            })
                            
                            let groupId = descriptor.sharingInfo.sharedWithUserIdentifiersInGroup[self.user.identifier]!
                            self.downloaderDelegates.forEach({
                                $0.didCompleteDownloadOfAsset(
                                    withGlobalIdentifier: decryptedAsset.globalIdentifier,
                                    in: groupId
                                )
                            })
                            
                            Task(priority: .low) {
                                await SHDownloadBlacklist.shared.removeFromBlacklist(assetGlobalIdentifier: globalIdentifier)
                            }
                        case .failure(let error):
                            let groupId = descriptor.sharingInfo.sharedWithUserIdentifiersInGroup[self.user.identifier]!
                            self.downloaderDelegates.forEach({
                                $0.didFailDownloadOfAsset(
                                    withGlobalIdentifier: globalIdentifier,
                                    in: groupId,
                                    with: error
                                )
                            })
                            
                            Task(priority: .low) {
                                if error is SHCypher.DecryptionError {
                                    await SHDownloadBlacklist.shared.blacklist(globalIdentifier: globalIdentifier)
                                } else {
                                    await SHDownloadBlacklist.shared.recordFailedAttempt(globalIdentifier: globalIdentifier)
                                }
                                
                                if await SHDownloadBlacklist.shared.isBlacklisted(assetGlobalIdentifier: globalIdentifier) {
                                    self.downloaderDelegates.forEach({
                                        $0.didFailRepeatedlyDownloadOfAsset(
                                            withGlobalIdentifier: globalIdentifier,
                                            in: groupId
                                        )
                                    })
                                }
                            }
                        }
                        
                        let end = CFAbsoluteTimeGetCurrent()
                        self.log.debug("[downloadAssets][PERF] it took \(CFAbsoluteTime(end - start)) to download the asset")
                    }
                }
                
                completionHandler(.success(()))
            } catch {
                log.error("[downloadAssets] error executing download task: \(error.localizedDescription)")
                completionHandler(.failure(error))
            }
        }
    }
    
    public func runOnce(
        for assetGlobalIdentifiers: [GlobalIdentifier]? = nil,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        
        guard self.user is SHAuthenticatedLocalUser else {
            completionHandler(.failure(SHLocalUserError.notAuthenticated))
            return
        }
        
        DispatchQueue.global(qos: .background).async {
            let group = DispatchGroup()
            
            var localDescriptors = [any SHAssetDescriptor]()
            var remoteDescriptors = [any SHAssetDescriptor]()
            var descriptorsByGlobalIdentifier = [GlobalIdentifier: any SHAssetDescriptor]()
            var localError: Error? = nil
            var remoteError: Error? = nil
            var dispatchResult: DispatchTimeoutResult? = nil
            
            ///
            /// Get all asset descriptors associated with this user from the server.
            /// Descriptors serve as a manifest to determine what to download.
            ///
            group.enter()
            self.serverProxy.getRemoteAssetDescriptors(
                for: (assetGlobalIdentifiers?.isEmpty ?? true) ? nil : assetGlobalIdentifiers!
            ) { remoteResult in
                switch remoteResult {
                case .success(let descriptors):
                    remoteDescriptors = descriptors
                case .failure(let err):
                    self.log.error("failed to fetch descriptors from REMOTE server when syncing: \(err.localizedDescription)")
                    remoteError = err
                }
                group.leave()
            }
            
            ///
            /// Get all the local descriptors.
            /// These need to be filtered out. Syncing will take care of updating descriptors that are present locally
            ///
            group.enter()
            self.serverProxy.getLocalAssetDescriptors { localResult in
                switch localResult {
                case .success(let descriptors):
                    localDescriptors = descriptors
                case .failure(let err):
                    self.log.error("failed to fetch descriptors from LOCAL server when syncing: \(err.localizedDescription)")
                    localError = err
                }
                group.leave()
            }
            
            group.notify(queue: .global(qos: .background)) {
                guard remoteError == nil else {
                    completionHandler(.failure(remoteError!))
                    return
                }
                guard localError == nil else {
                    completionHandler(.failure(localError!))
                    return
                }
                
                var remoteDescriptorsCopy = Array(remoteDescriptors)
                let p = remoteDescriptorsCopy.partition { remoteDesc in
                    localDescriptors.contains(where: { $0.globalIdentifier == remoteDesc.globalIdentifier })
                }
                let remoteOnlyDescriptors = remoteDescriptorsCopy[..<p]
                let remoteAndLocalDescriptors = remoteDescriptorsCopy[p...]
                
                let start = CFAbsoluteTimeGetCurrent()
                var processingError: Error? = nil
                
                ///
                /// Given the descriptors that are only in local, determine what needs to be downloaded (CREATES)
                ///
                group.enter()
                self.processDescriptors(Array(remoteOnlyDescriptors), priority: .background) { descResult in
                    switch descResult {
                    case .failure(let err):
                        self.log.error("failed to download descriptors: \(err.localizedDescription)")
                        processingError = err
                    case .success(let val):
                        descriptorsByGlobalIdentifier = val
                    }
                    group.leave()
                    
                    let end = CFAbsoluteTimeGetCurrent()
                    self.log.debug("[PERF] it took \(CFAbsoluteTime(end - start)) to fetch \(remoteDescriptors.count) descriptors")
                }
                
                group.notify(queue: .global(qos: .background)) {
                    guard processingError == nil else {
                        completionHandler(.failure(processingError!))
                        return
                    }
                    
                    group.enter()
                    self.processAssetsInDescriptors(
                        descriptorsByGlobalIdentifier: descriptorsByGlobalIdentifier
                    ) { descAssetResult in
                        switch descAssetResult {
                        case .failure(let err):
                            self.log.error("failed to process assets in descriptors: \(err.localizedDescription)")
                            processingError = err
                        case .success():
                            ///
                            /// Get all asset descriptors associated with this user from the server.
                            /// Descriptors serve as a manifest to determine what to download
                            ///
                            self.downloadAssets { result in
                                if case .failure(let err) = result {
                                    self.log.error("failed to download assets: \(err.localizedDescription)")
                                    processingError = err
                                }
                                group.leave()
                            }
                        }
                    }
                    
                    ///
                    /// Given the whole remote descriptors set, sync local and remote state (REMOVALS and UPDATES)
                    ///
                    let syncOperation = SHSyncOperation(
                        user: self.user as! SHAuthenticatedLocalUser,
                        assetsDelegates: self.assetsSyncDelegates,
                        threadsDelegates: self.threadsSyncDelegates
                    )
                    group.enter()
                    syncOperation.sync(
                        remoteDescriptors: Array(remoteAndLocalDescriptors),
                        localDescriptors: localDescriptors
                    ) { syncResult in
                        group.leave()
                    }
        
                    ///
                    /// Given the whole remote descriptors set (to retrieve threads and groups), sync the interactions
                    ///
                    group.enter()
                    syncOperation.syncInteractions(
                        remoteDescriptors: remoteDescriptors
                    ) { syncInteractionsResult in
                        group.leave()
                    }
                    
                    group.notify(queue: .global(qos: .background)) {
                        guard processingError == nil else {
                            completionHandler(.failure(processingError!))
                            return
                        }
                        
                        completionHandler(.success(()))
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
            self.downloaderDelegates.forEach({
                $0.didCompleteDownloadCycle(with: result)
            })
            self.state = .finished
        }
    }
}

// MARK: - Download Operation Processor

public class SHFullDownloadPipelineProcessor : SHBackgroundOperationProcessor<SHDownloadOperation> {
    
    public static var shared = SHFullDownloadPipelineProcessor(
        delayedStartInSeconds: 0,
        dispatchIntervalInSeconds: 7
    )
    private override init(delayedStartInSeconds: Int,
                          dispatchIntervalInSeconds: Int? = nil) {
        super.init(delayedStartInSeconds: delayedStartInSeconds, dispatchIntervalInSeconds: dispatchIntervalInSeconds)
    }
}
