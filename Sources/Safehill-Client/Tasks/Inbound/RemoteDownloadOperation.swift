import Foundation
import KnowledgeBase
import os
import Photos

protocol SHDownloadOperation {}


///
/// This pipeline operation is responsible for keeping assets in the remote server and local server in sync.
/// It **ignores all assets in the local server**, except for when determining the set it needs to operate on.
/// The restoration delegates are notified about successful uploads and shares from this user. If these items are not present in the history queues, they are created and persisted there.
///
///
/// The steps are:
/// 1. `fetchDescriptorsForItemsToDownload(filteringAssets:filteringGroups:completionHandler:)` : the descriptors are fetched from both the remote and local servers to determine the ones to operate on, namely the ones ONLY on remote
/// 2. `processDescriptors(_:fromRemote:qos:completionHandler:)` : descriptors are filtered based on blacklisting of users or assets, "retrievability" of users, or if the asset upload status is neither `.started` nor `.partial`.
/// 3. `processAssetsInDescriptors(descriptorsByGlobalIdentifier:qos:completionHandler:)` : descriptors are merged with the local photos library based on localIdentifier, calling the delegate for the matches (`didIdentify(globalToLocalAssets:`). Then for the ones not in the photos library:
///     - for the assets shared by _this_ user, local server assets and queue items are created when missing, and the restoration delegate is called
///     - for the assets shared by from _other_ users, the assets ready for download are returned
/// 4. `downloadAssets(for:completionHandler:)`  : for the remainder, download and decrypt them
///
/// The pipeline sequence is:
/// ```
/// 1 -->   2 -->    3 -->    4
/// ```
///
public class SHRemoteDownloadOperation: Operation, SHBackgroundOperationProtocol, SHDownloadOperation {
    
    internal static var lastFetchDate: Date? = nil
    
    public let log = Logger(subsystem: "com.safehill", category: "BG-DOWNLOAD")
    
    let delegatesQueue = DispatchQueue(label: "com.safehill.download.delegates")
    
    public let limit: Int?
    let user: SHLocalUserProtocol
    
    let downloaderDelegates: [SHAssetDownloaderDelegate]
    let restorationDelegate: SHAssetActivityRestorationDelegate
    
    let photoIndexer: SHPhotosIndexer
    
    public init(user: SHLocalUserProtocol,
                downloaderDelegates: [SHAssetDownloaderDelegate],
                restorationDelegate: SHAssetActivityRestorationDelegate,
                photoIndexer: SHPhotosIndexer,
                limitPerRun limit: Int? = nil) {
        self.user = user
        self.limit = limit
        self.downloaderDelegates = downloaderDelegates
        self.restorationDelegate = restorationDelegate
        self.photoIndexer = photoIndexer
    }
    
    var serverProxy: SHServerProxy { self.user.serverProxy }
    
    internal func fetchDescriptorsForItemsToDownload(
        filteringAssets globalIdentifiers: [GlobalIdentifier]? = nil,
        filteringGroups groupIds: [String]? = nil,
        completionHandler: @escaping (Result<[any SHAssetDescriptor], Error>) -> Void
    ) {
        ///
        /// Get all asset descriptors associated with this user from the server.
        /// Descriptors serve as a manifest to determine what to download.
        ///
        self.serverProxy.getRemoteAssetDescriptors(
            for: (globalIdentifiers?.isEmpty ?? true) ? nil : globalIdentifiers!,
            filteringGroups: groupIds,
            after: Self.lastFetchDate
        ) { remoteResult in
            switch remoteResult {
            case .success(let remoteDescriptors):
                ///
                /// Get all the corresponding local descriptors.
                /// The extra ones (to be DELETED) will be removed by the sync operation
                /// The ones that changed (to be UPDATED) will be changed by the sync operation
                ///
                self.serverProxy.getLocalAssetDescriptors(
                    for: remoteDescriptors.map { $0.globalIdentifier }
                ) { localResult in
                    switch localResult {
                    case .success(let localDescriptors):
                        completionHandler(.success(remoteDescriptors.filter({
                            remoteDesc in
                            localDescriptors.contains(where: { $0.globalIdentifier == remoteDesc.globalIdentifier }) == false
                        })))
                        
                    case .failure(let err):
                        self.log.error("[\(type(of: self))] failed to fetch descriptors from LOCAL server when syncing: \(err.localizedDescription)")
                        completionHandler(.failure(err))
                    }
                }
                
            case .failure(let err):
                self.log.error("[\(type(of: self))] failed to fetch descriptors from REMOTE server when syncing: \(err.localizedDescription)")
                completionHandler(.failure(err))
            }
        }
    }
    
    internal func getUsers(
        withIdentifiers userIdentifiers: [UserIdentifier],
        completionHandler: @escaping (Result<[UserIdentifier: any SHServerUser], Error>) -> Void
    ) {
        SHUsersController(localUser: self.user).getUsers(withIdentifiers: userIdentifiers, completionHandler: completionHandler)
    }
    
    ///
    /// Takes the full list of descriptors from server (remote or local, depending on whether it's running as part of the
    /// `SHLocalActivityRestoreOperation` or the `SHDownloadOperation`.
    ///
    ///
    /// Filters out
    /// - the ones referencing blacklisted assets (items that have been tried to download too many times),
    /// - the ones where any of the users referenced can't be retrieved
    /// - the ones for which the upload hasn't started
    ///
    /// Call the delegate with the full manifest of assets shared by OTHER users, regardless of the limit on the task config) for the assets.
    /// Returns the full set of descriptors fetched from the server, keyed by global identifier, limiting the result based on the task config.
    ///
    /// - Parameters:
    ///   - descriptors: the descriptors to process
    ///   - priority: the task priority.  Usually`.high` for initial restoration at start, `.background` for background downloads
    ///   - completionHandler: the callback
    ///
    internal func processDescriptors(
        _ descriptors: [any SHAssetDescriptor],
        fromRemote: Bool,
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<[GlobalIdentifier: any SHAssetDescriptor], Error>
        ) -> Void
    ) {
        
        ///
        /// Filter out the ones:
        /// - whose assets were blacklisted
        /// - whose users were blacklisted
        /// - haven't started upload (`.notStarted` is only relevant for the `SHLocalActivityRestoreOperation`)
        ///
        Task(priority: qos.toTaskPriority()) {
            let globalIdentifiers = Array(Set(descriptors.map({ $0.globalIdentifier })))
            let blacklistedAssets = await SHDownloadBlacklist.shared.areBlacklisted(
                assetGlobalIdentifiers: globalIdentifiers
            )
            
            let filteredDescriptors = descriptors.filter {
                (blacklistedAssets[$0.globalIdentifier] ?? false) == false
                && $0.uploadState == .completed || $0.uploadState == .partial
            }
            
            guard filteredDescriptors.count > 0 else {
                let downloaderDelegates = self.downloaderDelegates
                self.delegatesQueue.async {
                    if fromRemote {
                        downloaderDelegates.forEach({
                            $0.didReceiveRemoteAssetDescriptors([], referencing: [:])
                        })
                    } else {
                        downloaderDelegates.forEach({
                            $0.didReceiveLocalAssetDescriptors([], referencing: [:])
                        })
                    }
                }
                completionHandler(.success([:]))
                return
            }
            
            ///
            /// Fetch from server users information (`SHServerUser` objects)
            /// for all user identifiers found in all descriptors shared by OTHER known users
            ///
            var userIdentifiers = Set(filteredDescriptors.flatMap { $0.sharingInfo.sharedWithUserIdentifiersInGroup.keys })
            userIdentifiers.formUnion(filteredDescriptors.compactMap { $0.sharingInfo.sharedByUserIdentifier })
            
            self.getUsers(withIdentifiers: Array(userIdentifiers)) { result in
                switch result {
                case .failure(let error):
                    self.log.error("[\(type(of: self))] unable to fetch users from local server: \(error.localizedDescription)")
                    completionHandler(.failure(error))
                case .success(let usersDict):
                    ///
                    /// Filter out the descriptor that reference any user that could not be retrieved
                    ///
                    let filteredDescriptorsFromRetrievableUsers = filteredDescriptors.filter { desc in
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
                    /// Call the delegate with the full manifest of whitelisted assets.
                    /// The ones shared by THIS user will be restored through the restoration delegate.
                    ///
                    let userDictsImmutable = usersDict
                    let downloaderDelegates = self.downloaderDelegates
                    self.delegatesQueue.async {
                        if fromRemote {
                            downloaderDelegates.forEach({
                                $0.didReceiveRemoteAssetDescriptors(
                                    filteredDescriptorsFromRetrievableUsers,
                                    referencing: userDictsImmutable
                                )
                            })
                        } else {
                            downloaderDelegates.forEach({
                                $0.didReceiveLocalAssetDescriptors(
                                    filteredDescriptorsFromRetrievableUsers,
                                    referencing: userDictsImmutable
                                )
                            })
                        }
                    }
                    
                    var descriptorsByGlobalIdentifier = [String: any SHAssetDescriptor]()
                    for descriptor in filteredDescriptorsFromRetrievableUsers {
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
        }
    }
    
    ///
    /// Notifies the delegates about matches between the assets on the server and the local library.
    ///
    ///
    /// It retrieves apple photos identifiers in the local library corresponding to the assets in the descriptors having a local identifier.
    /// - If they exist, then do not decrypt them but just serve the local asset.
    /// - If they don't, return them in the callback method so they can be decrypted.
    ///
    /// - Parameters:
    ///   - descriptorsByGlobalIdentifier: all the descriptors by local identifier
    ///   - completionHandler: the callback method
    internal func filterOutMatchesInPhotoLibrary(
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
        
        self.photoIndexer.fetchAllAssets(withFilters: [.withLocalIdentifiers(localIdentifiersInDescriptors)]) {
            result in
            switch result {
            case .failure(let error):
                completionHandler(.failure(error))
                return
            case .success(let fetchResult):
                fetchResult?.enumerateObjects { phAsset, _, _ in
                    phAssetsByLocalIdentifier[phAsset.localIdentifier] = phAsset
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
        }
    }
    
    /// Given a list of descriptors determines which ones need to be dowloaded, authorized, or marked as backed up in the library.
    /// Returns the list of descriptors for the assets that are ready to be downloaded
    /// - Parameters:
    ///   - descriptorsByGlobalIdentifier: the descriptors, keyed by asset global identifier
    ///   - completionHandler: the callback, returning the assets to be downloaded, or an error
    internal func processAssetsInDescriptors(
        descriptorsByGlobalIdentifier: [GlobalIdentifier: any SHAssetDescriptor],
        qos: DispatchQoS.QoSClass,
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
        self.filterOutMatchesInPhotoLibrary(
            descriptorsByGlobalIdentifier: descriptorsByGlobalIdentifier,
            filteringKeys: sharedBySelfGlobalIdentifiers
        ) { result in
            switch result {
            case .success(let nonLocalPhotoLibraryGlobalIdentifiers):
                let start = CFAbsoluteTimeGetCurrent()
                
                ///
                /// Start download and decryption for the encrypted for the ones not found in the apple photos library
                ///
                self.processForDownload(
                    descriptorsByGlobalIdentifier: descriptorsByGlobalIdentifier,
                    nonApplePhotoLibrarySharedBySelfGlobalIdentifiers: nonLocalPhotoLibraryGlobalIdentifiers,
                    sharedBySelfGlobalIdentifiers: sharedBySelfGlobalIdentifiers,
                    sharedByOthersGlobalIdentifiers: sharedByOthersGlobalIdentifiers,
                    qos: qos
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
    
    private func recreateLocalAssetsAndQueueItems(
        descriptorsByGlobalIdentifier original: [GlobalIdentifier: any SHAssetDescriptor],
        filteringKeys globalIdentifiersSharedBySelf: [GlobalIdentifier],
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        guard original.count > 0 else {
            completionHandler(.success(()))
            return
        }
        
        guard globalIdentifiersSharedBySelf.count > 0 else {
            completionHandler(.success(()))
            return
        }
        
        let descriptorsByGlobalIdentifier = original.filter({
            globalIdentifiersSharedBySelf.contains($0.value.globalIdentifier)
            && $0.value.localIdentifier != nil
        })
        
        self.log.debug("[\(type(of: self))] recreating local assets and queue items for \(globalIdentifiersSharedBySelf)")
        
        ///
        /// Get the `.lowResolution` assets data from the remote server
        ///
        self.serverProxy.remoteServer.getAssets(
            withGlobalIdentifiers: globalIdentifiersSharedBySelf,
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
                    case .success:
                        ///
                        /// Re-create the successful upload and share queue items
                        ///
                        self.restoreAndRecreateQueueItems(
                            descriptorsByGlobalIdentifier: descriptorsByGlobalIdentifier,
                            qos: qos,
                            completionHandler: completionHandler
                        )
                    case .failure(let error):
                        self.log.error("[\(type(of: self))] failed to create assets in local server. Assets in the local library but uploaded will not be marked as such. \(error.localizedDescription)")
                        completionHandler(.failure(error))
                    }
                }
            case .failure(let error):
                self.log.error("[\(type(of: self))] failed to fetch assets from remote server. Assets in the local library but uploaded will not be marked as such. This operation will be attempted again. \(error.localizedDescription)")
                completionHandler(.failure(error))
            }
        }
    }
    
    ///
    /// For all the descriptors whose originator user is _this_ user:
    /// - create the items in the queue
    /// - notify the restoration delegate about the change
    /// Uploads and shares will be reported separately, according to the contract in the delegate.
    ///
    /// - Parameters:
    ///   - descriptorsByGlobalIdentifier: all the descriptors keyed by asset global identifier
    ///   - qos: the quality of service
    ///   - completionHandler: the callback method
    ///
    private func restoreAndRecreateQueueItems(
        descriptorsByGlobalIdentifier: [GlobalIdentifier: any SHAssetDescriptor],
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        guard descriptorsByGlobalIdentifier.count > 0 else {
            completionHandler(.success(()))
            return
        }
        
        var allUserIdsInDescriptors = Set<UserIdentifier>()
        for descriptor in descriptorsByGlobalIdentifier.values {
            for recipientId in descriptor.sharingInfo.sharedWithUserIdentifiersInGroup.keys {
                allUserIdsInDescriptors.insert(recipientId)
            }
        }
        
        self.getUsers(withIdentifiers: Array(allUserIdsInDescriptors)) { getUsersResult in
            switch getUsersResult {
                
            case .failure(let error):
                completionHandler(.failure(error))
                
            case .success(let usersDict):
                
                let (
                    groupIdToUploadItems,
                    groupIdToShareItems
                ) = self.createHistoryItems(
                    from: Array(descriptorsByGlobalIdentifier.values),
                    usersDict: usersDict
                )
                    
                for groupIdToUploadItem in groupIdToUploadItems.values {
                    for (uploadItem, timestamp) in groupIdToUploadItem {
                        if (try? uploadItem.insert(
                            in: BackgroundOperationQueue.of(type: .successfulUpload),
                            at: timestamp
                        )) == nil {
                            self.log.warning("[\(type(of: self))] unable to enqueue successful upload item groupId=\(uploadItem.groupId), localIdentifier=\(uploadItem.localIdentifier)")
                        }
                    }
                }
                
                for groupIdToShareItem in groupIdToShareItems.values {
                    for (shareItem, timestamp) in groupIdToShareItem {
                        if (try? shareItem.insert(
                            in: BackgroundOperationQueue.of(type: .successfulShare),
                            at: timestamp
                        )) == nil {
                            self.log.warning("[\(type(of: self))] unable to enqueue successful share item groupId=\(shareItem.groupId), localIdentifier=\(shareItem.localIdentifier)")
                        }
                    }
                }

                let restorationDelegate = self.restorationDelegate
                self.delegatesQueue.async {
                    restorationDelegate.restoreUploadQueueItems(from: groupIdToUploadItems)
                    restorationDelegate.restoreShareQueueItems(from: groupIdToShareItems)
                }
                
                completionHandler(.success(()))
            }
        }
    }
    
    /// For each descriptor (there's one per asset), create in-memory representation of:
    /// - `SHUploadHistoryItem`s, aka the upload event (by this user)
    /// - `SHShareHistoryItem`s, aka all share events for the asset (performed by this user)
    ///
    /// Such representation is keyed by groupId.
    /// In other words, for each group there is as many history items as many assets were shared in the group.
    ///
    /// - Parameters:
    ///   - descriptors: the descriptors to process
    ///   - usersDict: the users mentioned in the descriptors, keyed by identifier
    ///
    internal func createHistoryItems(
        from descriptors: [any SHAssetDescriptor],
        usersDict: [UserIdentifier: any SHServerUser]
    ) -> (
        [String: [(SHUploadHistoryItem, Date)]],
        [String: [(SHShareHistoryItem, Date)]]
    ) {
        let myUser = self.user
        
        var groupIdToUploadItems = [String: [(SHUploadHistoryItem, Date)]]()
        var groupIdToShareItems = [String: [(SHShareHistoryItem, Date)]]()
        
        for descriptor in descriptors {
            
            guard let localIdentifier = descriptor.localIdentifier else {
                continue
            }
            
            var otherUserIdsSharedWith = [String: [(with: any SHServerUser, at: Date)]]()
            
            for (recipientUserId, groupId) in descriptor.sharingInfo.sharedWithUserIdentifiersInGroup {
                
                let groupCreationDate = descriptor.sharingInfo.groupInfoById[groupId]!.createdAt!
                
                if recipientUserId == myUser.identifier {
                    
                    let item = SHUploadHistoryItem(
                        localAssetId: localIdentifier,
                        globalAssetId: descriptor.globalIdentifier,
                        versions: [.lowResolution, .hiResolution],
                        groupId: groupId,
                        eventOriginator: myUser,
                        sharedWith: [],
                        isPhotoMessage: false, // TODO: We should fetch this information from server, instead of assuming it's false
                        isBackground: false
                    )
                    
                    if groupIdToUploadItems[groupId] == nil {
                        groupIdToUploadItems[groupId] = [(item, groupCreationDate)]
                    } else {
                        groupIdToUploadItems[groupId]!.append((item, groupCreationDate))
                    }
                    
                } else {
                    guard let recipient = usersDict[recipientUserId] else {
                        self.log.critical("[\(type(of: self))] inconsistency between user ids referenced in descriptors and user objects returned from server")
                        continue
                    }
                    if otherUserIdsSharedWith[groupId] == nil {
                        otherUserIdsSharedWith[groupId] = [(with: recipient, at: groupCreationDate)]
                    } else {
                        otherUserIdsSharedWith[groupId]?.append(
                            missingContentsFrom: [(with: recipient, at: groupCreationDate)],
                            compareUsing: { $0.with.identifier == $1.with.identifier }
                        )
                    }
                }
            }
            
            for (groupId, shareInfo) in otherUserIdsSharedWith {
                let item = SHShareHistoryItem(
                    localAssetId: localIdentifier,
                    globalAssetId: descriptor.globalIdentifier,
                    versions: [.lowResolution, .hiResolution],
                    groupId: groupId,
                    eventOriginator: myUser,
                    sharedWith: shareInfo.map({ $0.with }),
                    isPhotoMessage: false, // TODO: We should fetch this information from server, instead of assuming it's false
                    isBackground: false
                )
                
                let maxDate: Date = shareInfo.reduce(Date.distantPast) { 
                    (currentMax, tuple) in
                    if currentMax.compare(tuple.at) == .orderedAscending {
                        return tuple.at
                    }
                    return currentMax
                }
                
                if groupIdToShareItems[groupId] == nil {
                    groupIdToShareItems[groupId] = [(item, maxDate)]
                } else {
                    groupIdToShareItems[groupId]!.append((item, maxDate))
                }
            }
        }
        
        return (
            groupIdToUploadItems,
            groupIdToShareItems
        )
    }
    
    internal func processForDownload(
        descriptorsByGlobalIdentifier: [GlobalIdentifier: any SHAssetDescriptor],
        nonApplePhotoLibrarySharedBySelfGlobalIdentifiers: [GlobalIdentifier],
        sharedBySelfGlobalIdentifiers: [GlobalIdentifier],
        sharedByOthersGlobalIdentifiers: [GlobalIdentifier],
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<[any SHAssetDescriptor], Error>) -> Void
    ) {
        
        let group = DispatchGroup()
        var errors = [Error]()
        
        ///
        /// FOR THE ASSETS SHARED BY THIS USER
        /// Because assets that are already in the local server are filtered out by the time this method 
        /// is called (by `fetchDescriptorsForItemsToDownload(filteringAssets:filteringGroups:)`,
        /// we only deal with assets that:
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
        
        /// 
        /// SUBCASE (1)
        ///
        
        var restorationError: Error? = nil
        group.enter()
        self.recreateLocalAssetsAndQueueItems(
            descriptorsByGlobalIdentifier: descriptorsByGlobalIdentifier,
            filteringKeys: sharedBySelfGlobalIdentifiers,
            qos: qos
        ) {
            result in
            switch result {
            case .success:
                break
            case .failure(let error):
                restorationError = error
            }
            group.leave()
        }
        
        group.notify(queue: .global(qos: qos)) {
            guard restorationError == nil else {
                completionHandler(.failure(restorationError!))
                return
            }
            
            ///
            /// SUBCASE (2)
            ///
            
            /// Collect all items ready to download, starting from the ones shared by self
            var descriptorsReadyToDownload = nonApplePhotoLibrarySharedBySelfGlobalIdentifiers.compactMap({ descriptorsByGlobalIdentifier[$0] })
            
            ///
            /// FOR THE ONES SHARED BY OTHER USERS
            /// Queue for download and decryption assets:
            /// - not shared by self
            /// - not in the Apple Photos Library
            ///
            /// The descriptors ready to download will be returned to the caller to start download and decryption. In particular:
            /// - When run on a `SHRemoteDownloadOperation`, the caller eventually calls `downloadAssets(for:completionHandler:)` using this list to dowload them from remote server and decrypt.
            /// - When run on a `SHLocalDownloadOperation`, the caller calls `decryptFromLocalStore(descriptorsByGlobalIdentifier:filteringKeys:completionHandler:)`
            /// which is performing the decryption from the local store (downloading from local doesn't make sense)
            ///
            
            descriptorsReadyToDownload.append(
                contentsOf: sharedByOthersGlobalIdentifiers.compactMap {
                    descriptorsByGlobalIdentifier[$0]
                }
            )
            
            completionHandler(.success(descriptorsReadyToDownload))
        }
    }
    
    func downloadAssets(
        for descriptors: [any SHAssetDescriptor],
        completionHandler: @escaping (Result<[(any SHDecryptedAsset, any SHAssetDescriptor)], Error>) -> Void
    ) {
        guard descriptors.count > 0 else {
            completionHandler(.success([]))
            return
        }
        
        guard let authedUser = self.user as? SHAuthenticatedLocalUser else {
            completionHandler(.failure(SHLocalUserError.notAuthenticated))
            return
        }
        
        ///
        /// Get all asset descriptors associated with this user from the server.
        /// Descriptors serve as a manifest to determine what to download
        ///
        var successfullyDownloadedAssetsAndDescriptors = [(any SHDecryptedAsset, any SHAssetDescriptor)]()
        var downloadError: Error? = nil
        let dispatchGroup = DispatchGroup()
        
        for descriptor in descriptors {
            guard !self.isCancelled else {
                log.info("[\(type(of: self))] download task cancelled. Finishing")
                return
            }
            
            guard let groupId = descriptor.sharingInfo.sharedWithUserIdentifiersInGroup[self.user.identifier] else {
                log.critical("[\(type(of: self))] malformed descriptor. Missing groupId for user \(self.user.identifier) for assetId \(descriptor.globalIdentifier)")
                completionHandler(.failure(SHBackgroundOperationError.fatalError("malformed descriptor. Missing groupId for user \(self.user.identifier) for assetId \(descriptor.globalIdentifier)")))
                return
            }
            
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
            SHAssetsDownloadManager(user: authedUser).downloadAsset(for: descriptor) {
                result in
                
                switch result {
                
                case .failure(let error):
                    self.log.error("[\(type(of: self))] failed to download asset \(descriptor.globalIdentifier)")
                    
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
                        self.log.info("[\(type(of: self))] skipping item \(descriptor.globalIdentifier) because it was attempted too many times")
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
    
    private func process(
        _ descriptorsForItemsToDownload: [any SHAssetDescriptor],
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<[(any SHDecryptedAsset, any SHAssetDescriptor)], Error>) -> Void
    ) {
        ///
        /// This processing takes care of the **CREATES**, namely the new assets on the server not present locally
        /// Given the descriptors that are only on REMOTE, determine what needs to be downloaded after filtering
        ///
        
        self.log.debug("[\(type(of: self))] original descriptors: \(descriptorsForItemsToDownload.count)")
        
        self.processDescriptors(
            descriptorsForItemsToDownload,
            fromRemote: true,
            qos: qos
        ) { descResult in
            
            switch descResult {
                
            case .failure(let err):
                self.log.error("[\(type(of: self))] failed to download descriptors: \(err.localizedDescription)")
                completionHandler(.failure(err))
                
            case .success(let filteredDescriptorsByAssetGid):
                
#if DEBUG
                let delta = Set(descriptorsForItemsToDownload.map({ $0.globalIdentifier })).subtracting(filteredDescriptorsByAssetGid.keys)
                self.log.debug("[\(type(of: self))] after processing: \(filteredDescriptorsByAssetGid.count). delta=\(delta)")
#endif
                
                self.processAssetsInDescriptors(
                    descriptorsByGlobalIdentifier: filteredDescriptorsByAssetGid,
                    qos: qos
                ) { descAssetResult in
                    switch descAssetResult {
                        
                    case .failure(let err):
                        self.log.error("[\(type(of: self))] failed to process assets in descriptors: \(err.localizedDescription)")
                        completionHandler(.failure(err))
                        
                    case .success(let descriptorsReadyToDownload):
#if DEBUG
                        let delta1 = Set(filteredDescriptorsByAssetGid.keys).subtracting(descriptorsReadyToDownload.map({ $0.globalIdentifier }))
                        let delta2 = Set(descriptorsReadyToDownload.map({ $0.globalIdentifier })).subtracting(filteredDescriptorsByAssetGid.keys)
                        self.log.debug("[\(type(of: self))] ready for download: \(descriptorsReadyToDownload.count). onlyInProcessed=\(delta1) onlyInToDownload=\(delta2)")
#endif
                        
                        guard descriptorsReadyToDownload.isEmpty == false else {
                            completionHandler(.success([]))
                            return
                        }
                        
                        do {
                            try SHKGQuery.ingest(descriptorsReadyToDownload, receiverUserId: self.user.identifier)
                        } catch {
                            completionHandler(.failure(error))
                            return
                        }
                        
                        self.downloadAssets(for: descriptorsReadyToDownload) { downloadResult in
                            switch downloadResult {
                            
                            case .success(let list):
                                completionHandler(.success(list))
                            
                            case .failure(let error):
                                completionHandler(.failure(error))
                            }
                        }
                    }
                }
            }
        }
    }
    
    public func run(
        for assetGlobalIdentifiers: [GlobalIdentifier]?,
        filteringGroups groupIds: [String]?,
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<[(any SHDecryptedAsset, any SHAssetDescriptor)], Error>) -> Void
    ) {
        let fetchStartedAt = Date()
        
        let handleResult = { (result: Result<[(any SHDecryptedAsset, any SHAssetDescriptor)], Error>) in
            let downloaderDelegates = self.downloaderDelegates
            self.delegatesQueue.async {
                switch result {
                case .failure(let error):
                    downloaderDelegates.forEach({
                        $0.didFailDownloadCycle(with: error)
                    })
                    
                case .success(let tuples):
                    downloaderDelegates.forEach({
                        $0.didCompleteDownloadCycle(
                            remoteAssetsAndDescriptors: tuples
                        )
                    })
                }
            }
            completionHandler(result)
            
            if case .success(let tuples) = result, tuples.count > 0 {
                Self.lastFetchDate = fetchStartedAt
            }
        }
        
        guard self.user is SHAuthenticatedLocalUser else {
            handleResult(.failure(SHLocalUserError.notAuthenticated))
            return
        }
        
        DispatchQueue.global(qos: qos).async {
            self.fetchDescriptorsForItemsToDownload(
                filteringAssets: assetGlobalIdentifiers,
                filteringGroups: groupIds
            ) {
                (result: Result<([any SHAssetDescriptor]), Error>) in
                switch result {
                case .success(let descriptorsForItemsToDownload):
                    self.process(
                        descriptorsForItemsToDownload,
                        qos: qos,
                        completionHandler: handleResult
                    )
                case .failure(let error):
                    handleResult(.failure(error))
                }
            }
        }
    }
    
    internal func runOnce(
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<[(any SHDecryptedAsset, any SHAssetDescriptor)], Error>) -> Void
    ) {
        self.run(for: nil, filteringGroups: nil, qos: qos, completionHandler: completionHandler)
    }
    
    public func run(
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        self.runOnce(qos: qos) { result in
            if case .failure(let failure) = result {
                completionHandler(.failure(failure))
            } else {
                completionHandler(.success(()))
            }
        }
    }
}

public let RemoteDownloadPipelineProcessor = SHBackgroundOperationProcessor<SHRemoteDownloadOperation>()
