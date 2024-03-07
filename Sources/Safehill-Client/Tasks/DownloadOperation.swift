import Foundation
import KnowledgeBase
import os
import Photos

protocol SHDownloadOperationProtocol {}


public class SHDownloadOperation: SHAbstractBackgroundOperation, SHBackgroundQueueProcessorOperationProtocol, SHDownloadOperationProtocol {
    
    public let log = Logger(subsystem: "com.safehill", category: "BG-DOWNLOAD")
    
    let delegatesQueue = DispatchQueue(label: "com.safehill.download.delegates")
    
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
    ///   - priority: the task priority.  Usually`.high` for initial restoration at start, `.background` for background downloads
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
                let downloaderDelegates = self.downloaderDelegates
                self.delegatesQueue.async {
                    downloaderDelegates.forEach({
                        $0.didReceiveAssetDescriptors([], referencing: [:])
                    })
                }
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
            let descriptorsFromKnownUsersImmutable = descriptorsFromKnownUsers
            let userDictsImmutable = usersDict
            let downloaderDelegates = self.downloaderDelegates
            self.delegatesQueue.async {
                downloaderDelegates.forEach({
                    $0.didReceiveAssetDescriptors(descriptorsFromKnownUsersImmutable,
                                                  referencing: userDictsImmutable)
                })
            }
            
            var descriptorsByGlobalIdentifier = [String: any SHAssetDescriptor]()
            for descriptor in descriptorsFromKnownUsers {
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
        var globalToPHAsset = [GlobalIdentifier: PHAsset]()
        for descriptor in descriptorsByGlobalIdentifier.values {
            if let localId = descriptor.localIdentifier,
               let phAsset = phAssetsByLocalIdentifier[localId] {
                globalToPHAsset[descriptor.globalIdentifier] = phAsset
            } else {
                filteredGlobalIdentifiers.append(descriptor.globalIdentifier)
            }
        }
        
        let downloaderDelegates = self.downloaderDelegates
        self.delegatesQueue.async {
            downloaderDelegates.forEach({
                $0.didIdentify(globalToLocalAssets: globalToPHAsset)
            })
        }
        
        completionHandler(.success(filteredGlobalIdentifiers))
    }
    
    ///
    /// Request authorization for the ones that need authorization, and return the assets that are authorized to start downloading.
    /// A download needs explicit authorization from the user if the sender has never shared an asset with this user before.
    /// Once the link is established, all other downloads won't need authorization.
    ///
    /// - Parameter descriptorsByGlobalIdentifier: the descriptors keyed by their global identifier
    /// - Parameter completionHandler: the callback method, returning the descriptors that are ready to be downloaded
    private func requestAuthorizationForUnknownUsers(
        forAssetsIn descriptors: [any SHAssetDescriptor],
        completionHandler: @escaping (Result<[any SHAssetDescriptor], Error>) -> Void
    ) {
        guard descriptors.count > 0 else {
            completionHandler(.success([]))
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

            let downloaderDelegates = self.downloaderDelegates
            self.delegatesQueue.async {
                downloaderDelegates.forEach({
                    $0.didReceiveAuthorizationRequest(
                        for: unauthorizedDownloadDescriptors,
                        referencing: usersDict
                    )
                })
            }
        }
        
        if authorizedDownloadDescriptors.count > 0 {
            ///
            /// For downloads that don't need authorization:
            /// - the delegate method `handleDownloadAuthorization(ofDescriptors:users:)` is called
            /// - descriptors are returned
            /// - the index of assets to authorized per user is updated (userStore, keyed by `auth-<USER_ID>`)
            ///
            completionHandler(.success(authorizedDownloadDescriptors))
        } else {
            completionHandler(.success([]))
        }
    }
    
    /// Given a list of descriptors determines which ones need to be dowloaded, authorized, marked as backed up in the library, etc.
    /// Returns the list of descriptors for the assets that are ready to be downloaded
    /// - Parameters:
    ///   - descriptorsByGlobalIdentifier: the descriptors, keyed by asset global identifier
    ///   - completionHandler: the callback, returning the assets to be downloaded, or an error
    internal func processAssetsInDescriptors(
        descriptorsByGlobalIdentifier: [GlobalIdentifier: any SHAssetDescriptor],
        completionHandler: @escaping (Result<[any SHAssetDescriptor], Error>) -> Void
    ) {
        ///
        /// Get all asset descriptors associated with this user from the server.
        /// Descriptors serve as a manifest to determine what to decrypt
        ///
        guard descriptorsByGlobalIdentifier.count > 0 else {
            completionHandler(.success([]))
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
        completionHandler: @escaping (Result<[any SHAssetDescriptor], Error>) -> Void
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
                completionHandler(.success([]))
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
            ///         - the user signed out and wiped all the local asset information, including photos that had and still have references to the Photo Library
            /// 2. assets are not in the Apple Photos library (shared by this user and NOT in the Photos library and not in the local server)
            ///     - This can happen when:
            ///         - they were deleted from the local library
            ///         - they were shared from a different device and they are not on this device's Photo Library
            ///
            /// (2) is handled by downloading them as regular downloads from other users, with the only difference that authorization is skipped
            ///
            /// For (1), identification `didIdentify(globalToLocalAssets:)` happens in `mergeDescriptorsWithApplePhotosAssets(descriptorsByGlobalIdentifier:filteringKeys:completionHandler:)`, so we only need to take care of adding the asset to the local server, add items to the success queue (upload and share) and call the restoration delegate.
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
            
            var descriptorsReadyToDownload = globalIdentifiersNotOnLocalSharedBySelfNotInApplePhotoLibrary.compactMap({ descriptorsByGlobalIdentifier[$0] })
            
            ///
            /// FOR THE ONES SHARED BY OTHER USERS
            /// Decrypt the remainder from the local store, namely assets:
            /// - not shared by self
            /// - not in the Apple Photos Library (we'll use the apple photos version as long as it exists)
            ///
            let globalIdentifiersNotOnLocalSharedByOthers = sharedByOthersGlobalIdentifiers
                .subtract(remoteGlobalIdentifiersAlsoOnLocalServer)
            
            group.enter()
            self.requestAuthorizationForUnknownUsers(
                forAssetsIn: globalIdentifiersNotOnLocalSharedByOthers.compactMap({ descriptorsByGlobalIdentifier[$0] })
            ) { result in
                switch result {
                case .success(let descs):
                    descriptorsReadyToDownload.append(contentsOf: descs)
                case .failure(let failure):
                    errors.append(failure)
                }
                group.leave()
            }
            
            group.notify(queue: DispatchQueue.global()) {
                if errors.count > 0 {
                    self.log.error("[downloadAssets] failed downloading assets with errors: \(errors.map({ $0.localizedDescription }).joined(separator: ","))")
                    completionHandler(.failure(errors.first!))
                } else {
                    completionHandler(.success(descriptorsReadyToDownload))
                }
            }
        }
    }
    
    func downloadAssets(
        for descriptors: [any SHAssetDescriptor],
        completionHandler: @escaping (Result<[(any SHDecryptedAsset, any SHAssetDescriptor)], Error>) -> Void
    ) {
        ///
        /// Get all asset descriptors associated with this user from the server.
        /// Descriptors serve as a manifest to determine what to download
        ///
        var successfullyDownloadedAssetsAndDescriptors = [(any SHDecryptedAsset, any SHAssetDescriptor)]()
        var downloadError: Error? = nil
        let dispatchGroup = DispatchGroup()
        
        for descriptor in descriptors {
            guard !self.isCancelled else {
                log.info("[downloadAssets] download task cancelled. Finishing")
                state = .finished
                break
            }
            
            let groupId = descriptor.sharingInfo.sharedWithUserIdentifiersInGroup[self.user.identifier]!
            
            let downloaderDelegates = self.downloaderDelegates
            self.delegatesQueue.async {
                downloaderDelegates.forEach({
                    $0.didStartDownloadOfAsset(
                        withGlobalIdentifier: descriptor.globalIdentifier,
                        descriptor: descriptor,
                        in: groupId
                    )
                })
            }
            
            dispatchGroup.enter()
            SHAssetsDownloadManager(user: self.user).downloadAsset(for: descriptor) {
                result in
                
                switch result {
                
                case .failure(let error):
                    self.log.error("failed to download asset \(descriptor.globalIdentifier)")
                    
                    downloadError = error
                    
                    let downloaderDelegates = self.downloaderDelegates
                    self.delegatesQueue.async {
                        downloaderDelegates.forEach {
                            $0.didFailDownloadOfAsset(
                                withGlobalIdentifier: descriptor.globalIdentifier,
                                in: groupId,
                                with: error
                            )
                        }
                    }
                    
                    if case SHAssetDownloadError.assetIsBlacklisted(_) = error {
                        self.log.info("[downloadAssets] skipping item \(descriptor.globalIdentifier) because it was attempted too many times")
                        self.delegatesQueue.async {
                            downloaderDelegates.forEach({
                                $0.didFailRepeatedlyDownloadOfAsset(
                                    withGlobalIdentifier: descriptor.globalIdentifier,
                                    in: groupId
                                )
                            })
                        }
                    }
                    
                case .success(let decryptedAsset):
                    let downloaderDelegates = self.downloaderDelegates
                    self.delegatesQueue.async {
                        downloaderDelegates.forEach({
                            $0.didFetchLowResolutionAsset(decryptedAsset)
                        })
                        
                        downloaderDelegates.forEach({
                            $0.didCompleteDownloadOfAsset(
                                withGlobalIdentifier: decryptedAsset.globalIdentifier,
                                in: groupId
                            )
                        })
                    }
                    
                    successfullyDownloadedAssetsAndDescriptors.append((decryptedAsset, descriptor))
                }
                
                dispatchGroup.leave()
            }
        }
        
        dispatchGroup.notify(queue: .global()) {
            if let downloadError {
                completionHandler(.failure(downloadError))
            } else {
                completionHandler(.success(successfullyDownloadedAssetsAndDescriptors))
            }
        }
    }
    
    public func runOnce(
        for assetGlobalIdentifiers: [GlobalIdentifier]? = nil,
        completionHandler: @escaping (Result<[(any SHDecryptedAsset, any SHAssetDescriptor)], Error>) -> Void
    ) {
        
        guard self.user is SHAuthenticatedLocalUser else {
            completionHandler(.failure(SHLocalUserError.notAuthenticated))
            return
        }
        
        DispatchQueue.global(qos: .background).async {
            let dispatchGroup = DispatchGroup()
            
            var localDescriptors = [any SHAssetDescriptor]()
            var remoteDescriptors = [any SHAssetDescriptor]()
            var descriptorsByGlobalIdentifier = [GlobalIdentifier: any SHAssetDescriptor]()
            var localError: Error? = nil
            var remoteError: Error? = nil
            
            ///
            /// Get all asset descriptors associated with this user from the server.
            /// Descriptors serve as a manifest to determine what to download.
            ///
            dispatchGroup.enter()
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
                dispatchGroup.leave()
            }
            
            ///
            /// Get all the local descriptors.
            /// These need to be filtered out. Syncing will take care of updating descriptors that are present locally
            ///
            dispatchGroup.enter()
            self.serverProxy.getLocalAssetDescriptors { localResult in
                switch localResult {
                case .success(let descriptors):
                    localDescriptors = descriptors
                case .failure(let err):
                    self.log.error("failed to fetch descriptors from LOCAL server when syncing: \(err.localizedDescription)")
                    localError = err
                }
                dispatchGroup.leave()
            }
            
            dispatchGroup.notify(queue: .global(qos: .background)) {
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
                /// **CREATES**
                /// Given the descriptors that are only on REMOTE, determine what needs to be downloaded
                ///
                
                self.log.debug("original descriptors: \(remoteOnlyDescriptors.count)")
                
                dispatchGroup.enter()
                self.processDescriptors(Array(remoteOnlyDescriptors), priority: .background) { descResult in
                    switch descResult {
                    case .failure(let err):
                        self.log.error("failed to download descriptors: \(err.localizedDescription)")
                        processingError = err
                    case .success(let val):
                        descriptorsByGlobalIdentifier = val
                    }
                    dispatchGroup.leave()
                    
                    let end = CFAbsoluteTimeGetCurrent()
                    self.log.debug("[PERF] it took \(CFAbsoluteTime(end - start)) to fetch \(remoteDescriptors.count) descriptors")
                }
                
                dispatchGroup.notify(queue: .global(qos: .background)) {
                    guard processingError == nil else {
                        completionHandler(.failure(processingError!))
                        return
                    }
                    
                    dispatchGroup.enter()
                    
#if DEBUG
                    let delta = Set(remoteOnlyDescriptors.map({ $0.globalIdentifier })).subtracting(descriptorsByGlobalIdentifier.keys)
                    self.log.debug("after processing: \(descriptorsByGlobalIdentifier.count). delta=\(delta)")
#endif
                    
                    var successfullyDownloadedAssets = [(any SHDecryptedAsset, any SHAssetDescriptor)]()
                    
                    self.processAssetsInDescriptors(
                        descriptorsByGlobalIdentifier: descriptorsByGlobalIdentifier
                    ) { descAssetResult in
                        switch descAssetResult {
                        case .failure(let err):
                            self.log.error("failed to process assets in descriptors: \(err.localizedDescription)")
                            processingError = err
                            dispatchGroup.leave()
                        case .success(let descriptorsReadyToDownload):
#if DEBUG
                            let delta1 = Set(descriptorsByGlobalIdentifier.keys).subtracting(descriptorsReadyToDownload.map({ $0.globalIdentifier }))
                            let delta2 = Set(descriptorsReadyToDownload.map({ $0.globalIdentifier })).subtracting(descriptorsByGlobalIdentifier.keys)
                            self.log.debug("ready for download: \(descriptorsReadyToDownload.count). onlyInProcessed=\(delta1) onlyInToDownload=\(delta2)")
#endif
                            self.downloadAssets(for: descriptorsReadyToDownload) { downloadResult in
                                switch downloadResult {
                                case .success(let list):
                                    successfullyDownloadedAssets = list
                                case .failure(let error):
                                    processingError = error
                                }
                                dispatchGroup.leave()
                            }
                        }
                    }
                    
                    ///
                    /// **UPDATES and DELETES**
                    /// Given the whole remote descriptors set, sync local and remote state
                    ///
                    let syncOperation = SHSyncOperation(
                        user: self.user as! SHAuthenticatedLocalUser,
                        assetsDelegates: self.assetsSyncDelegates,
                        threadsDelegates: self.threadsSyncDelegates
                    )
                    dispatchGroup.enter()
                    syncOperation.sync(
                        remoteDescriptors: Array(remoteAndLocalDescriptors),
                        localDescriptors: localDescriptors
                    ) { syncResult in
                        dispatchGroup.leave()
                    }
        
                    ///
                    /// Given the whole remote descriptors set (to retrieve threads and groups), sync the interactions
                    ///
                    dispatchGroup.enter()
                    syncOperation.syncInteractions(
                        remoteDescriptors: remoteDescriptors
                    ) { syncInteractionsResult in
                        dispatchGroup.leave()
                    }
                    
                    dispatchGroup.notify(queue: .global(qos: .background)) {
                        guard processingError == nil else {
                            completionHandler(.failure(processingError!))
                            return
                        }
                        
                        completionHandler(.success(successfullyDownloadedAssets))
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
            let downloaderDelegates = self.downloaderDelegates
            self.delegatesQueue.async {
                downloaderDelegates.forEach({
                    $0.didCompleteDownloadCycle(with: result)
                })
            }
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
