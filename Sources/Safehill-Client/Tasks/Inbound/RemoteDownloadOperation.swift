import Foundation
import KnowledgeBase
import os
import Photos

protocol SHDownloadOperation {}


///
/// This pipeline operation is responsible for keeping assets in the remote server and local server in sync.
/// It **ignores all assets in the local server**, except for when determining the set it needs to operate on.
///
/// The steps are:
/// 1. `fetchDescriptors(for:completionHandler:)` : the descriptors are fetched from both the remote and local servers to determine the ones to operate on, namely the ones ONLY on remote
/// 2. `processDescriptors(_:qos:completionHandler:)` : descriptors are filtered based on blacklisting of users or assets, "retrievability" of users, or if the asset upload status is neither `.started` nor `.partial`. Both known and unknwon users are included, but the delegate method `didReceiveAssetDescriptors(_:referencing:)` is called for the ones from "known" users. A known user is a user that is present in the knowledge graph and is also "retrievable", namely _this_ user can fetch its details from the server. This further segmentation is required because the delegate method `didReceiveAuthorizationRequest(for:referencing:)` is called for the "unknown" (aka still unauthorized) users
/// 3. `processAssetsInDescriptors(descriptorsByGlobalIdentifier:qos:completionHandler:)` : descriptors are merged with the local photos library based on localIdentifier, calling the delegate for the matches (`didIdentify(globalToLocalAssets:`). Then for the ones not in the photos library:
///     - for the assets shared by _this_ user, local server assets and queue items are created when missing, and the restoration delegate is called
///     - for the assets shared by from _other_ users, the authorization is requested for the "unknown" users, and the remaining assets ready for download are returned
/// 4. `downloadAssets(for:completionHandler:)`  : for the remainder, download and decrypt the ones from "known" authorized users and append to the unauthorized queue for the "uknown" users
/// 5. `sync(remoteDescriptors:localDescriptors:qos:completionHandler:)` : similarly to how step 2.1 takes care of creating assets that do not exist on local server, this step takes care of assets that are both in local and remote server as well as the ones only in local. Local server is updated according to remote server information.
/// 6. `syncGroupInteractions(remoteDescriptors:qos:completionHandler:)` : From the remote descriptors determine all group identifiers and sync the interactions (reactions and comments) for these groups by updating the local interactions store
///
/// The pipeline sequence is:
/// ```
/// 1 -->   2 -->    3 -->    4
///   \->   5
///   \->   6
/// ```
///
public class SHRemoteDownloadOperation: SHAbstractBackgroundOperation, SHBackgroundOperationProtocol, SHDownloadOperation {
    
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
        SHRemoteDownloadOperation(
            user: self.user,
            downloaderDelegates: self.downloaderDelegates,
            assetsSyncDelegates: self.assetsSyncDelegates,
            threadsSyncDelegates: self.threadsSyncDelegates,
            restorationDelegate: self.restorationDelegate,
            limitPerRun: self.limit,
            photoIndexer: self.photoIndexer
        )
    }
    
    internal func fetchDescriptors(
        for assetGlobalIdentifiers: [GlobalIdentifier]? = nil,
        filteringGroups: [String]? = nil,
        completionHandler: @escaping (
            Result<(
                allRemote: [any SHAssetDescriptor],
                allLocal: [any SHAssetDescriptor],
                remoteOnly: [any SHAssetDescriptor],
                localAndRemote: [any SHAssetDescriptor]
            ), Error>
        ) -> Void
    ) {
        
        let dispatchGroup = DispatchGroup()
        
        var localDescriptors = [any SHAssetDescriptor]()
        var remoteDescriptors = [any SHAssetDescriptor]()
        var localError: Error? = nil
        var remoteError: Error? = nil
        
        ///
        /// Get all asset descriptors associated with this user from the server.
        /// Descriptors serve as a manifest to determine what to download.
        ///
        dispatchGroup.enter()
        self.serverProxy.getRemoteAssetDescriptors(
            for: (assetGlobalIdentifiers?.isEmpty ?? true) ? nil : assetGlobalIdentifiers!,
            filteringGroups: filteringGroups
        ) { remoteResult in
            switch remoteResult {
            case .success(let descriptors):
                remoteDescriptors = descriptors
            case .failure(let err):
                self.log.error("[\(type(of: self))] failed to fetch descriptors from REMOTE server when syncing: \(err.localizedDescription)")
                remoteError = err
            }
            dispatchGroup.leave()
        }
        
        ///
        /// Get all the local descriptors.
        /// These need to be filtered out. Syncing will take care of updating descriptors that are present locally
        ///
        dispatchGroup.enter()
        self.serverProxy.getLocalAssetDescriptors(
            for: (assetGlobalIdentifiers?.isEmpty ?? true) ? nil : assetGlobalIdentifiers!,
            filteringGroups: filteringGroups
        ) { localResult in
            switch localResult {
            case .success(let descriptors):
                localDescriptors = descriptors
            case .failure(let err):
                self.log.error("[\(type(of: self))] failed to fetch descriptors from LOCAL server when syncing: \(err.localizedDescription)")
                localError = err
            }
            dispatchGroup.leave()
        }
        
        dispatchGroup.notify(queue: .global()) {
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
            
            completionHandler(.success((
                allRemote: remoteDescriptors,
                allLocal: localDescriptors,
                remoteOnly: Array(remoteOnlyDescriptors),
                localAndRemote: Array(remoteAndLocalDescriptors)
            )))
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
    /// Filters out the blacklisted assets and users, as well as the non-completed uploads.
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
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<(
                fromRetrievableUsers: [GlobalIdentifier: any SHAssetDescriptor],
                fromKnownUsers: [GlobalIdentifier]
            ), Error>
        ) -> Void
    ) {
        
        ///
        /// Filter out the ones:
        /// - whose assets were blacklisted
        /// - whose users were blacklisted
        /// - haven't started upload (`.notStarted` is only relevant for the `SHLocalActivityRestoreOperation`)
        ///
        Task(priority: qos.toTaskPriority()) {
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
                completionHandler(.success((fromRetrievableUsers: [:], fromKnownUsers: [])))
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
                    
                    /// When calling the delegate method `didReceiveAssetDescriptors(_:referencing:)`
                    /// filter out the ones whose sender is unknown.
                    /// The delegate method `didReceiveAuthorizationRequest(for:referencing:)` will take care of those.
                    senderIds = filteredDescriptorsFromRetrievableUsers.map({ $0.sharingInfo.sharedByUserIdentifier })
                    let knownUsers: [UserIdentifier: Bool]
                    do { knownUsers = try SHKGQuery.areUsersKnown(withIdentifiers: senderIds) }
                    catch {
                        self.log.error("[\(type(of: self))] failed to read from the graph to fetch \"known user\" information. Terminating download operation early. \(error.localizedDescription)")
                        completionHandler(.failure(error))
                        return
                    }
                    
                    let descriptorsFromKnownUsers = filteredDescriptorsFromRetrievableUsers.filter {
                        knownUsers[$0.sharingInfo.sharedByUserIdentifier] ?? false
                    }
                    
                    ///
                    /// Call the delegate with the full manifest of whitelisted assets **ONLY for the assets shared by other known users**.
                    /// The ones shared by THIS user will be restored through the restoration delegate.
                    ///
                    let userDictsImmutable = usersDict
                    let downloaderDelegates = self.downloaderDelegates
                    self.delegatesQueue.async {
                        downloaderDelegates.forEach({
                            $0.didReceiveAssetDescriptors(descriptorsFromKnownUsers,
                                                          referencing: userDictsImmutable)
                        })
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
                    completionHandler(.success((
                        fromRetrievableUsers: descriptorsByGlobalIdentifier,
                        fromKnownUsers: descriptorsFromKnownUsers.map({ $0.globalIdentifier })
                    )))
                }
            }
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
    private func waitForAuthorization(
        _ unauthorizedDownloadDescriptors: [any SHAssetDescriptor],
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        guard unauthorizedDownloadDescriptors.count > 0 else {
            completionHandler(.success(()))
            return
        }
        
        let downloadsManager = SHAssetsDownloadManager(user: self.user)
        downloadsManager.waitForDownloadAuthorization(forDescriptors: unauthorizedDownloadDescriptors) { result in
            switch result {
            case .failure(let error):
                if case SHBackgroundOperationError.alreadyProcessed = error {
                    // TODO: When running on a cron, avoid notifying delegates about authorizations over and over
                    break
                } else {
                    self.log.critical("[\(type(of: self))] failed to enqueue unauthorized download for \(unauthorizedDownloadDescriptors.count) descriptors. \(error.localizedDescription). This operation will be attempted again")
                }
            default:
                break
            }
            
            var userIdentifiers = Set(unauthorizedDownloadDescriptors.flatMap { $0.sharingInfo.sharedWithUserIdentifiersInGroup.keys })
            userIdentifiers.formUnion(Set(unauthorizedDownloadDescriptors.compactMap { $0.sharingInfo.sharedByUserIdentifier }))
            
            SHUsersController(localUser: self.user).getUsers(
                withIdentifiers: Array(userIdentifiers)
            ) { result in
                switch result {
                case .failure(let error):
                    self.log.error("[\(type(of: self))] unable to fetch users from local server: \(error.localizedDescription)")
                    completionHandler(.failure(error))
                case .success(let usersDict):
                    let downloaderDelegates = self.downloaderDelegates
                    self.delegatesQueue.async {
                        downloaderDelegates.forEach({
                            $0.didReceiveAuthorizationRequest(
                                for: unauthorizedDownloadDescriptors,
                                referencing: usersDict
                            )
                        })
                    }
                    completionHandler(.success(()))
                }
            }
        }
    }
    
    ///
    /// Request authorization for the ones that need authorization, and return the assets that are authorized to start downloading.
    /// A download needs explicit authorization from the user if the sender has never shared an asset with this user before.
    /// Once the link is established, all other downloads won't need authorization.
    ///
    /// - Parameter descriptorsByGlobalIdentifier: the descriptors keyed by their global identifier
    /// - Parameter completionHandler: the callback method, returning the descriptors that are ready to be downloaded
    private func checkIfAuthorizationRequired(
        forAssetsIn descriptors: [any SHAssetDescriptor],
        completionHandler: @escaping (Result<[any SHAssetDescriptor], Error>) -> Void
    ) {
        guard descriptors.count > 0 else {
            completionHandler(.success([]))
            return
        }
        
        /// 
        /// This method is called for every download cycle.
        /// Once a user that is "uknown" is enqueued for authorization, we don't need to re-add them to the queue (although it would be replaced, not appended)
        /// For known users, that don't need authorization, nothing will be filtered out by the following filter if the user is not in the unauthorized queue.
        /// In other words, we expect descriptors for a "known" user to always pass the condition below.
        ///
        let notEnqueuedAsUnauthorized = descriptors.filter { descriptor in
            (try? SHAssetsDownloadManager.unauthorizedDownloads(for: descriptor.sharingInfo.sharedByUserIdentifier))?.isEmpty ?? true
            // If current unauth status can't be feched safely re-add them to the unauthorized queue as needed (hence, default to true)
        }
        self.log.info("[\(type(of: self))] of \(descriptors.count) \(notEnqueuedAsUnauthorized.count) have already been enqueued as unauthorized")
        
        guard notEnqueuedAsUnauthorized.count > 0 else {
            completionHandler(.success([]))
            return
        }
        
        /// 
        /// For the ones that are not already marked as unauthorized (all descriptors - unauthorized)
        /// - check if the user is known (if so, add them to the unauthorized queue)
        /// - return the rest so they can be downloaded
        ///
        let senderIds = notEnqueuedAsUnauthorized.map({ $0.sharingInfo.sharedByUserIdentifier })
        let knownUsers: [UserIdentifier: Bool]
        do { knownUsers = try SHKGQuery.areUsersKnown(withIdentifiers: senderIds) }
        catch {
            log.error("[\(type(of: self))] failed to read from the graph to fetch \"known user\" information. Terminating authorization request operation early. \(error.localizedDescription)")
            completionHandler(.failure(error))
            return
        }
        
        var mutableDescriptors = notEnqueuedAsUnauthorized
        let partitionIndex = mutableDescriptors.partition {
            knownUsers[$0.sharingInfo.sharedByUserIdentifier] ?? false
        }
        let unauthorizedDownloadDescriptors = Array(mutableDescriptors[..<partitionIndex])
        let authorizedDownloadDescriptors = Array(mutableDescriptors[partitionIndex...])
        
        self.log.info("[\(type(of: self))] of \(notEnqueuedAsUnauthorized.count) need to authorize \(unauthorizedDownloadDescriptors.count), can download \(authorizedDownloadDescriptors.count)")
        
        self.waitForAuthorization(unauthorizedDownloadDescriptors) { result in
            switch result {
            case .failure(let error):
                completionHandler(.failure(error))
            case .success:
                ///
                /// For downloads that don't need authorization:
                /// - the delegate method `handleDownloadAuthorization(ofDescriptors:users:)` is called
                /// - descriptors are returned
                /// - the index of assets to authorized per user is updated (userStore, keyed by `auth-<USER_ID>`)
                ///
                completionHandler(.success(authorizedDownloadDescriptors))
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
        self.mergeDescriptorsWithApplePhotosAssets(
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
        descriptorsByGlobalIdentifier: [GlobalIdentifier: any SHAssetDescriptor],
        filteringKeys globalIdentifiers: [GlobalIdentifier],
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        guard globalIdentifiers.count > 0 else {
            completionHandler(.success(()))
            return
        }
        
        let descriptorsByGlobalIdentifier = descriptorsByGlobalIdentifier.filter({
            globalIdentifiers.contains($0.value.globalIdentifier)
        })
        
        guard descriptorsByGlobalIdentifier.count > 0 else {
            completionHandler(.success(()))
            return
        }
        
        self.log.debug("[\(type(of: self))] recreating local assets and queue items for \(globalIdentifiers)")
        
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
        let myUser = self.user
        
        let descriptorsByGlobalIdentifier = descriptorsByGlobalIdentifier.filter({
            $0.value.sharingInfo.sharedByUserIdentifier == myUser.identifier
        })
        
        guard descriptorsByGlobalIdentifier.count > 0 else {
            completionHandler(.success(()))
            return
        }
        
        let group = DispatchGroup()
        
        var otherUsersById = [String: any SHServerUser]()
        var getUserError: Error? = nil
        
        group.enter()
        self.serverProxy.getUsers(inAssetDescriptors: Array(descriptorsByGlobalIdentifier.values)) {
            getUsersResult in
            switch getUsersResult {
            case .success(let otherUsers):
                otherUsersById = otherUsers.reduce([:]) { partialResult, user in
                    var result = partialResult
                    result[user.identifier] = user
                    return result
                }
                
                
            case .failure(let error):
                getUserError = error
            }
            group.leave()
        }
            
        group.notify(queue: .global(qos: qos)) {
            guard getUserError == nil else {
                completionHandler(.failure(getUserError!))
                return
            }
            
            let remoteServerDescriptorByAssetGid = descriptorsByGlobalIdentifier.filter({
                $0.value.localIdentifier != nil
            })
            
            let restorationDelegate = self.restorationDelegate
            self.delegatesQueue.async {
                restorationDelegate.didStartRestoration()
            }
            
            var userIdsInvolvedInRestoration = Set<UserIdentifier>()
            
            for remoteDescriptor in remoteServerDescriptorByAssetGid.values {
                
                var uploadLocalAssetIdByGroupId = [String: Set<LocalIdentifier>]()
                var shareLocalAssetIdsByGroupId = [String: Set<LocalIdentifier>]()
                var groupIdToUploadItem = [String: (SHUploadHistoryItem, Date)]()
                var groupIdToShareItem = [String: (SHShareHistoryItem, Date)]()
                
                for (recipientUserId, groupId) in remoteDescriptor.sharingInfo.sharedWithUserIdentifiersInGroup {
                    let localIdentifier = remoteDescriptor.localIdentifier!
                    let groupCreationDate = remoteDescriptor.sharingInfo.groupInfoById[groupId]!.createdAt!
                    
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
                            self.log.critical("[\(type(of: self))] inconsistency between user ids referenced in descriptors and user objects returned from server")
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
                        self.log.warning("[\(type(of: self))] unable to enqueue successful upload item groupId=\(uploadItem.groupId), localIdentifier=\(uploadItem.localIdentifier)")
                    }
                }
                for (shareItem, timestamp) in groupIdToShareItem.values {
                    if (try? shareItem.insert(
                        in: BackgroundOperationQueue.of(type: .successfulShare),
                        at: timestamp
                    )) == nil {
                        self.log.warning("[\(type(of: self))] unable to enqueue successful share item groupId=\(shareItem.groupId), localIdentifier=\(shareItem.localIdentifier)")
                    }
                }

                self.log.debug("[\(type(of: self))] upload local asset identifiers by group \(uploadLocalAssetIdByGroupId)")
                self.log.debug("[\(type(of: self))] share local asset identifiers by group \(shareLocalAssetIdsByGroupId)")

                self.delegatesQueue.async {
                    for (groupId, localIdentifiers) in uploadLocalAssetIdByGroupId {
                        restorationDelegate.restoreUploadQueueItems(
                            forLocalIdentifiers: Array(localIdentifiers),
                            in: groupId
                        )
                    }
                    
                    for (groupId, localIdentifiers) in shareLocalAssetIdsByGroupId {
                        restorationDelegate.restoreShareQueueItems(
                            forLocalIdentifiers: Array(localIdentifiers),
                            in: groupId
                        )
                    }
                }
            }
            
            self.delegatesQueue.async {
                restorationDelegate.didCompleteRestoration(userIdsInvolvedInRestoration: Array(userIdsInvolvedInRestoration))
            }
            
            completionHandler(.success(()))
        }
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
        /// Because assets that are already in the local server are filtered out by when this method is called (by `fetchDescriptors` of `SHRemoteDownloadOperation`, we only deal with assets that:
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
        /// (1)
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
            /// (2)
            ///
            
            /// Collect all items ready to download, starting from the ones shared by self
            var descriptorsReadyToDownload = nonApplePhotoLibrarySharedBySelfGlobalIdentifiers.compactMap({ descriptorsByGlobalIdentifier[$0] })
            
            ///
            /// FOR THE ONES SHARED BY OTHER USERS
            /// Queue for download and decryption assets:
            /// - not shared by self
            /// - not in the Apple Photos Library
            ///
            /// If any of them is from an "unknown" user (a user you never received anything from) they require authorization.
            /// If they do, the `checkIfAuthorizationRequired(forAssetsIn:completionHandler:)` adds them
            /// to a special queue for authorization.
            /// For the rest (from "known" users), this method returns their descriptor as "ready to download".
            /// Those ready to download will be sent back to the caller to start download and decryption. In particular:
            /// - When run on a `SHRemoteDownloadOperation`, the caller eventually calls `downloadAssets(for:completionHandler:)` using this list to dowload them from remote server and decrypt.
            /// - When run on a `SHLocalDownloadOperation`, the caller calls
            /// `decryptFromLocalStore(descriptorsByGlobalIdentifier:filteringKeys:completionHandler:)`
            /// which is performing the decryption from the local store (downloading from local doesn't make sense)
            ///
            
            group.enter()
            self.checkIfAuthorizationRequired(
                forAssetsIn: sharedByOthersGlobalIdentifiers.compactMap({ descriptorsByGlobalIdentifier[$0] })
            ) { result in
                switch result {
                case .success(let descs):
                    /// Add the ones shared by others that can be downloaded to the "ready to download" set
                    descriptorsReadyToDownload.append(contentsOf: descs)
                case .failure(let failure):
                    errors.append(failure)
                }
                group.leave()
            }
            
            group.notify(queue: DispatchQueue.global(qos: qos)) {
                if errors.count > 0 {
                    self.log.error("[\(type(of: self))] failed downloading assets with errors: \(errors.map({ $0.localizedDescription }).joined(separator: ","))")
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
        guard descriptors.count > 0 else {
            completionHandler(.success([]))
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
                state = .finished
                break
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
            SHAssetsDownloadManager(user: self.user).downloadAsset(for: descriptor) {
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
        _ descriptors: (
            allRemote: [any SHAssetDescriptor],
            allLocal: [any SHAssetDescriptor],
            remoteOnly: [any SHAssetDescriptor],
            localAndRemote: [any SHAssetDescriptor]
        ),
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<[(any SHDecryptedAsset, any SHAssetDescriptor)], Error>) -> Void
    ) {
        var filteredDescriptorsByAssetGid = [GlobalIdentifier: any SHAssetDescriptor]()
        
        let start = CFAbsoluteTimeGetCurrent()
        var processingError: Error? = nil
        
        ///
        /// **CREATES**
        /// Given the descriptors that are only on REMOTE, determine what needs to be downloaded after filtering
        ///
        
        self.log.debug("[\(type(of: self))] original descriptors: \(descriptors.remoteOnly.count)")
        
        let dispatchGroup = DispatchGroup()
        
        dispatchGroup.enter()
        self.processDescriptors(descriptors.remoteOnly, qos: qos) { descResult in
            switch descResult {
            case .failure(let err):
                self.log.error("[\(type(of: self))] failed to download descriptors: \(err.localizedDescription)")
                processingError = err
            case .success(let val):
                filteredDescriptorsByAssetGid = val.fromRetrievableUsers
            }
            dispatchGroup.leave()
            
            let end = CFAbsoluteTimeGetCurrent()
            self.log.debug("[PERF] it took \(CFAbsoluteTime(end - start)) to fetch \(descriptors.remoteOnly.count) descriptors")
        }
        
        dispatchGroup.notify(queue: .global(qos: qos)) {
            guard processingError == nil else {
                completionHandler(.failure(processingError!))
                return
            }
            
            dispatchGroup.enter()
            
#if DEBUG
            let delta = Set(descriptors.remoteOnly.map({ $0.globalIdentifier })).subtracting(filteredDescriptorsByAssetGid.keys)
            self.log.debug("[\(type(of: self))] after processing: \(filteredDescriptorsByAssetGid.count). delta=\(delta)")
#endif
            
            var successfullyDownloadedAssets = [(any SHDecryptedAsset, any SHAssetDescriptor)]()
            
            self.processAssetsInDescriptors(
                descriptorsByGlobalIdentifier: filteredDescriptorsByAssetGid,
                qos: qos
            ) { descAssetResult in
                switch descAssetResult {
                case .failure(let err):
                    self.log.error("[\(type(of: self))] failed to process assets in descriptors: \(err.localizedDescription)")
                    processingError = err
                    dispatchGroup.leave()
                case .success(let descriptorsReadyToDownload):
#if DEBUG
                    let delta1 = Set(filteredDescriptorsByAssetGid.keys).subtracting(descriptorsReadyToDownload.map({ $0.globalIdentifier }))
                    let delta2 = Set(descriptorsReadyToDownload.map({ $0.globalIdentifier })).subtracting(filteredDescriptorsByAssetGid.keys)
                    self.log.debug("[\(type(of: self))] ready for download: \(descriptorsReadyToDownload.count). onlyInProcessed=\(delta1) onlyInToDownload=\(delta2)")
#endif
                    
                    do {
                        try SHKGQuery.ingest(descriptorsReadyToDownload, receiverUserId: self.user.identifier)
                    } catch {
                        processingError = error
                        dispatchGroup.leave()
                        return
                    }
                    
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
                remoteAndLocalDescriptors: descriptors.localAndRemote,
                localDescriptors: descriptors.allLocal,
                qos: qos
            ) { syncResult in
                dispatchGroup.leave()
            }

            ///
            /// Given the whole remote descriptors set (to retrieve threads and groups),
            /// sync the comments and reactions in posts
            ///
            dispatchGroup.enter()
            syncOperation.syncGroupInteractions(
                remoteDescriptors: descriptors.allRemote,
                qos: qos
            ) { syncInteractionsResult in
                dispatchGroup.leave()
            }
            
            dispatchGroup.notify(queue: .global(qos: qos)) {
                guard processingError == nil else {
                    completionHandler(.failure(processingError!))
                    return
                }
                
                completionHandler(.success(successfullyDownloadedAssets))
            }
        }
    }
    
    public func runOnce(
        for assetGlobalIdentifiers: [GlobalIdentifier]?,
        filteringGroups groupIds: [String]?,
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<[(any SHDecryptedAsset, any SHAssetDescriptor)], Error>) -> Void
    ) {
        let handleResult = { (result: Result<[(any SHDecryptedAsset, any SHAssetDescriptor)], Error>) in
            let downloaderDelegates = self.downloaderDelegates
            self.delegatesQueue.async {
                downloaderDelegates.forEach({
                    $0.didCompleteDownloadCycle(with: result)
                })
            }
            completionHandler(result)
        }
        
        guard self.user is SHAuthenticatedLocalUser else {
            handleResult(.failure(SHLocalUserError.notAuthenticated))
            return
        }
        
        DispatchQueue.global(qos: qos).async {
            self.fetchDescriptors(
                for: assetGlobalIdentifiers,
                filteringGroups: groupIds
            ) {
                (result: Result<(
                    allRemote: [any SHAssetDescriptor],
                    allLocal: [any SHAssetDescriptor],
                    remoteOnly: [any SHAssetDescriptor],
                    localAndRemote: [any SHAssetDescriptor]
                ), Error>) in
                switch result {
                case .success(let descriptors):
                    self.process(descriptors, qos: qos) { result in
                        handleResult(result)
                    }
                case .failure(let error):
                    handleResult(.failure(error))
                }
            }
        }
    }
    
    public func runOnce(
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<[(any SHDecryptedAsset, any SHAssetDescriptor)], Error>) -> Void
    ) {
        self.runOnce(for: nil, filteringGroups: nil, qos: qos, completionHandler: completionHandler)
    }
    
    public override func run(
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        self.runOnce(qos: .background) { result in
            if case .failure(let failure) = result {
                completionHandler(.failure(failure))
            } else {
                completionHandler(.success(()))
            }
        }
    }
}

// MARK: - Download Operation Processor

public class SHRemoteDownloadPipelineProcessor : SHBackgroundOperationProcessor<SHRemoteDownloadOperation> {
    
    public static var shared = SHRemoteDownloadPipelineProcessor(
        delayedStartInSeconds: 0,
        dispatchIntervalInSeconds: 5
    )
    private override init(delayedStartInSeconds: Int,
                          dispatchIntervalInSeconds: Int? = nil) {
        super.init(delayedStartInSeconds: delayedStartInSeconds, dispatchIntervalInSeconds: dispatchIntervalInSeconds)
    }
}
