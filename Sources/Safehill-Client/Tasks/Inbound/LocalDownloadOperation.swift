import Foundation
import Safehill_Crypto
import KnowledgeBase
import os


///
/// This pipeline operation deals with assets present the LocalServer.
/// **It does not** deal with remote server or remote descriptors. See `SHRemoteDownloadOperation` for remote descriptor processing.
/// The local pipeline is responsible for identifying the mapping between the assets in the photos library and the assets in the local server. The ones that don't have a mapping to the local photo library,  are decrypted from the local server and served via the `SHAssetDownloaderDelegate`.
/// The restoration delegates are notified about successful uploads and shares from this user. It assumes the history queues on disk are in sync with the local server, so it doesn't try to create history items like the `SHRemoteDownloadOperation`.
///
///
/// The steps are:
/// 1. `fetchDescriptorsFromServer()` : the descriptors are fetched from the local server
/// 2. `processDescriptors(_:fromRemote:qos:completionHandler:)` : descriptors are filtered based on blacklisting of users or assets, "retrievability" of users, or if the asset upload status is neither `.started` nor `.partial`. Both known and unknwon users are included, but the delegate method `didReceiveLocalAssetDescriptors(_:referencing:)` is called for the ones from "known" users. A known user is a user that is present in the knowledge graph and is also "retrievable", namely _this_ user can fetch its details from the server. This pipeline **does not** deal with use authorization, because assets in the local server are expected to have been previously authorized.
/// 3. `processAssetsInDescriptors(descriptorsByGlobalIdentifier:qos:completionHandler:)` : descriptors are merged with the local photos library based on localIdentifier, calling the delegate for the matches (`didIdentify(globalToLocalAssets:`). Then for the ones not in the photos library:
///     - for the assets shared by _this_ user, the restoration delegate is called to restore them
///     - the assets shared by from _other_ users are returned so they can be decrypted
/// 4. `decryptFromLocalStore` : for the remainder, the decryption step runs and passes the decrypted asset to the delegates
///
/// Usually in the lifecycle of the application, the decryption happens only once.
/// The delegate is responsible for keeping these decrypted assets in memory, or call the `SHServerProxy` to retrieve them again if disposed of.
/// Any new asset that needs to be downloaded from the server while the app is running is taken care of by the sister processor, the `SHRemoteDownloadOperation`.
///
/// The pipeline sequence is:
/// ```
/// 1 -->   2 -->    3 -->    4
/// ```
///
public class SHLocalDownloadOperation: SHRemoteDownloadOperation {
    
    private static var alreadyProcessed = Set<GlobalIdentifier>()
    
    static func markAsAlreadyProcessed(_ gids: [GlobalIdentifier]) {
        for gid in gids {
            Self.alreadyProcessed.insert(gid)
        }
    }
    
    @available(*, unavailable)
    public override init(
        user: SHLocalUserProtocol,
        downloaderDelegates: [SHAssetDownloaderDelegate],
        restorationDelegate: SHAssetActivityRestorationDelegate,
        photoIndexer: SHPhotosIndexer,
        limitPerRun limit: Int? = nil
    ) {
        fatalError("Not supported")
    }
    
    public init(
        user: SHLocalUserProtocol,
        delegates: [SHAssetDownloaderDelegate],
        restorationDelegate: SHAssetActivityRestorationDelegate,
        photoIndexer: SHPhotosIndexer
    ) {
        super.init(
            user: user,
            downloaderDelegates: delegates,
            restorationDelegate: restorationDelegate,
            photoIndexer: photoIndexer
        )
    }
    
    /// This method overrides the behavior of the `SHDownloadOperation` to make the descriptor fetch
    /// happen against the local server (instead of the remote server)
    /// - Returns: the list of descriptors
    internal func fetchDescriptorsFromLocalServer(
        completionHandler: @escaping (Result<[any SHAssetDescriptor], Error>) -> Void
    ) {
        serverProxy.getLocalAssetDescriptors(after: nil) { result in
            switch result {
            case .success(let descs):
                let unprocessed = descs.filter({
                    Self.alreadyProcessed.contains($0.globalIdentifier) == false
                })
                completionHandler(.success(unprocessed))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    internal override func getUsers(
        withIdentifiers userIdentifiers: [UserIdentifier],
        completionHandler: @escaping (Result<[UserIdentifier: any SHServerUser], Error>) -> Void
    ) {
        Task {
            do {
                let usersDict = try await SHUsersController(localUser: self.user).getUsersOrCached(with: userIdentifiers)
                completionHandler(.success(usersDict))
            } catch {
                completionHandler(.failure(error))
            }
        }
    }
    
    ///
    /// For all the descriptors whose originator user is _this_ user, notify the restoration delegate
    /// about all groups that need to be restored.
    /// Uploads and shares will be reported separately, according to the contract in the delegate.
    /// Because these assets exist in the local server, the assumption is that they will also be present in the
    /// upload and history queues, so they don't need to be re-created.
    /// The recreation only happens in the sister method:
    /// `SHRemoteDownloadOperation::restoreQueueItems(descriptorsByGlobalIdentifier:qos:completionHandler:)`
    ///
    /// - Parameters:
    ///   - descriptorsByGlobalIdentifier: all the descriptors keyed by asset global identifier
    ///   - globalIdentifiersSharedBySelf: the asset global identifiers shared by self
    ///   - completionHandler: the callback method
    ///
    func restoreQueueItems(
        descriptorsByGlobalIdentifier original: [GlobalIdentifier: any SHAssetDescriptor],
        filteringKeys globalIdentifiersSharedBySelf: [GlobalIdentifier],
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
        
        let descriptors = original.values.filter({
            globalIdentifiersSharedBySelf.contains($0.globalIdentifier)
            && $0.localIdentifier != nil
        })
        
        guard descriptors.count > 0 else {
            completionHandler(.success(()))
            return
        }
        
        var allUserIdsInDescriptors = Set<UserIdentifier>()
        for descriptor in descriptors {
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
                    from: descriptors,
                    usersDict: usersDict
                )
                
                let restorationDelegate = self.restorationDelegate
                self.delegatesQueue.async {
                    restorationDelegate.restoreUploadQueueItems(from: groupIdToUploadItems)
                    restorationDelegate.restoreShareQueueItems(from: groupIdToShareItems)
                }
                
                completionHandler(.success(()))
            }
        }
    }
    
    
    internal func decryptFromLocalStore(
        descriptorsByGlobalIdentifier original: [GlobalIdentifier: any SHAssetDescriptor],
        filteringKeys: [GlobalIdentifier],
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<[(any SHDecryptedAsset, any SHAssetDescriptor)], Error>) -> Void
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
        
        self.log.debug("[\(type(of: self))] attempting to decrypt following assets from local store: \(Array(descriptorsByGlobalIdentifier.keys))")
        
        let localAssetsStore = SHLocalAssetStoreController(user: self.user)
        
        localAssetsStore.encryptedAssets(
            with: Array(descriptorsByGlobalIdentifier.keys),
            versions: [.lowResolution],
            cacheHiResolution: false,
            qos: qos
        ) {
            result in
            switch result {
            case .failure(let error):
                self.log.error("[\(type(of: self))] unable to fetch local assets: \(error.localizedDescription)")
                completionHandler(.failure(SHBackgroundOperationError.fatalError("unable to fetch local assets")))
            case .success(let encryptedAssets):
                var successfullyDecrypted = [(any SHDecryptedAsset, any SHAssetDescriptor)]()
                let dispatchGroup = DispatchGroup()
                
                for (globalAssetId, encryptedAsset) in encryptedAssets {
                    guard let descriptor = descriptorsByGlobalIdentifier[globalAssetId] else {
                        self.log.critical("[\(type(of: self))] malformed descriptorsByGlobalIdentifier")
                        completionHandler(.failure(SHBackgroundOperationError.fatalError("malformed descriptorsByGlobalIdentifier")))
                        return
                    }
                    
                    guard let groupId = descriptor.sharingInfo.sharedWithUserIdentifiersInGroup[self.user.identifier] else {
                        self.log.critical("malformed descriptor. Missing groupId for user \(self.user.identifier) for assetId \(descriptor.globalIdentifier)")
                        completionHandler(.failure(SHBackgroundOperationError.fatalError("malformed descriptor. Missing groupId for user \(self.user.identifier) for assetId \(descriptor.globalIdentifier)")))
                        return
                    }
                    
                    let downloaderDelegates = self.downloaderDelegates
                    self.delegatesQueue.async {
                        downloaderDelegates.forEach({
                            $0.didStartDownloadOfAsset(withGlobalIdentifier: globalAssetId,
                                                       descriptor: descriptor,
                                                       in: groupId)
                        })
                    }
                    
                    dispatchGroup.enter()
                    localAssetsStore.decryptedAsset(
                        encryptedAsset: encryptedAsset,
                        quality: .lowResolution,
                        descriptor: descriptor
                    ) { result in
                        switch result {
                        case .failure(let error):
                            self.log.error("[\(type(of: self))] unable to decrypt local asset \(globalAssetId): \(error.localizedDescription)")
                            
                            self.delegatesQueue.async {
                                downloaderDelegates.forEach({
                                    $0.didFailDownloadOfAsset(
                                        withGlobalIdentifier: encryptedAsset.globalIdentifier,
                                        in: groupId,
                                        with: error
                                    )
                                })
                            }
                        case .success(let decryptedAsset):
                            self.delegatesQueue.async {
                                downloaderDelegates.forEach({
                                    $0.didFetchLowResolutionAsset(decryptedAsset)
                                })
                                downloaderDelegates.forEach({
                                    $0.didCompleteDownloadOfAsset(
                                        withGlobalIdentifier: encryptedAsset.globalIdentifier,
                                        in: groupId
                                    )
                                })
                            }
                            
                            successfullyDecrypted.append((decryptedAsset, descriptor))
                        }
                        
                        dispatchGroup.leave()
                    }
                }
                
                dispatchGroup.notify(queue: .global(qos: qos)) {   
                    completionHandler(.success(successfullyDecrypted))
                }
            }
        }
    }
    
    override internal func processForDownload(
        descriptorsByGlobalIdentifier: [GlobalIdentifier: any SHAssetDescriptor],
        nonApplePhotoLibrarySharedBySelfGlobalIdentifiers: [GlobalIdentifier],
        sharedBySelfGlobalIdentifiers: [GlobalIdentifier],
        sharedByOthersGlobalIdentifiers: [GlobalIdentifier],
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<[any SHAssetDescriptor], Error>) -> Void
    ) {
        // TODO: This method does not re-create missing items from the history queues. If an item is missing it will not be restored
        
        ///
        /// FOR THE ONES SHARED BY THIS USER
        /// Notify the restoration delegates about the assets that need to be restored from the successful queues
        /// These can only reference assets shared by THIS user. All other descriptors are ignored.
        ///
        /// These assets are definitely in the local server (because the descriptors fetch by a `SHLocalDownloadOperation`
        /// contain only assets from the local server).
        /// However they may or may not be in the history queues. The method `restoreQueueItems(descriptors
        ///
        self.restoreQueueItems(
            descriptorsByGlobalIdentifier: descriptorsByGlobalIdentifier,
            filteringKeys: sharedBySelfGlobalIdentifiers
        ) { restoreResult in
            
            if case .failure(let err) = restoreResult {
                self.log.critical("failure while restoring queue items \(sharedBySelfGlobalIdentifiers): \(err.localizedDescription)")
            } else {
                Self.markAsAlreadyProcessed(sharedBySelfGlobalIdentifiers)
            }
            
            ///
            /// FOR THE ONES SHARED BY OTHER USERS
            /// Decrypt from the local store
            ///
            
            ///
            /// Remember: because the full manifest of descriptors (`descriptorsByGlobalIdentifier`)
            /// is fetched from LocalStore, all assets are guaranteed to be found
            ///
            completionHandler(.success(Array(
                descriptorsByGlobalIdentifier.values.filter({
                    sharedByOthersGlobalIdentifiers.contains($0.globalIdentifier)
                })
            )))
        }
    }
    
    ///
    /// Get all asset descriptors associated with this user from the server.
    /// Descriptors serve as a manifest to determine what to download.
    /// For assets that exist in the local Apple Photos Library, do not decrypt, but just serve the corresponding PHAsset
    ///
    /// ** Note that only .lowResolution assets are fetched from the local server here.**
    /// ** Higher resolutions are meant to be lazy loaded by the delegate.**
    ///
    /// - Parameter completionHandler: the callback method
    internal override func runOnce(
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<[(any SHDecryptedAsset, any SHAssetDescriptor)], Error>) -> Void
    ) {
        let handleFailure = { (error: Error) in
            let downloaderDelegates = self.downloaderDelegates
            self.delegatesQueue.async {
                downloaderDelegates.forEach({
                    $0.didFailDownloadCycle(with: error)
                })
            }
            completionHandler(.failure(error))
        }
        
        self.fetchDescriptorsFromLocalServer {
            result in
            switch result {
            case .failure(let error):
                self.log.error("[\(type(of: self))] failed to fetch local descriptors: \(error.localizedDescription)")
                handleFailure(error)
                return
            case .success(let fullDescriptorList):
                self.log.debug("[\(type(of: self))] original descriptors: \(fullDescriptorList.count)")
                self.processDescriptors(fullDescriptorList, fromRemote: false, qos: qos) { result in
                    switch result {
                    case .failure(let error):
                        self.log.error("[\(type(of: self))] failed to process descriptors: \(error.localizedDescription)")
                        handleFailure(error)
                    case .success(let filteredDescriptors):
                        let filteredDescriptorsFromKnownUsersByGid = filteredDescriptors.fromRetrievableUsers.filter({
                            filteredDescriptors.fromKnownUsers.contains($0.value.globalIdentifier)
                        })
#if DEBUG
                        ///
                        /// The `SHLocalDownloadOperation` doesn't deal with request authorizations.
                        /// The `SHRemoteDownloadOperation` is responsible for it.
                        /// The `didReceiveLocalAssetDescriptors(_:referencing:)` delegate method is called
                        /// with descriptors from known users only.
                        /// Hence, the decryption should only happen for those, and - in turn - we only want to call the downloader
                        /// delegate for assets that are not from known users. No assets not referenced in the call to
                        /// `didReceiveAssetLocalDescriptors(_:referencing:)` should be referenced in `didStartDownloadOfAsset`, `didCompleteDownloadOfAsset` or `didFailDownloadOfAsset`.
                        ///
                        /// The reason why the `didReceiveAssetLocalDescriptors` method is called with descriptors for known users
                        /// is because the `SHRemoteDownloadOperation` treats differently known (not needing authorization) and unknown users (needing authorization).
                        /// In order to avoid initiating/starting a download for an asset from an unknown user,
                        /// `didReceiveLocalAssetDescriptors` should never reference those.
                        ///
                        let delta = Set(fullDescriptorList.map({ $0.globalIdentifier })).subtracting(filteredDescriptorsFromKnownUsersByGid.keys)
                        self.log.debug("[\(type(of: self))] after processing: \(filteredDescriptorsFromKnownUsersByGid.count). delta=\(delta)")
#endif
                        self.processAssetsInDescriptors(
                            descriptorsByGlobalIdentifier: filteredDescriptorsFromKnownUsersByGid,
                            qos: qos
                        ) { secondResult in
                            
                            switch secondResult {
                            case .success(let descriptorsToDecrypt):
#if DEBUG
                                let delta1 = Set(filteredDescriptorsFromKnownUsersByGid.keys).subtracting(descriptorsToDecrypt.map({ $0.globalIdentifier }))
                                let delta2 = Set(descriptorsToDecrypt.map({ $0.globalIdentifier })).subtracting(filteredDescriptorsFromKnownUsersByGid.keys)
                                self.log.debug("[\(type(of: self))] ready for decryption: \(descriptorsToDecrypt.count). onlyInProcessed=\(delta1) onlyInToDecrypt=\(delta2)")
#endif
                                self.decryptFromLocalStore(
                                    descriptorsByGlobalIdentifier: filteredDescriptorsFromKnownUsersByGid,
                                    filteringKeys: descriptorsToDecrypt.map({ $0.globalIdentifier }),
                                    qos: qos
                                ) {
                                    thirdResult in
                                    
                                    let downloaderDelegates = self.downloaderDelegates
                                    self.delegatesQueue.async {
                                        switch thirdResult {
                                        case .failure(let error):
                                            downloaderDelegates.forEach({
                                                $0.didFailDownloadCycle(with: error)
                                            })
                                            
                                        case .success(let tuples):
                                            downloaderDelegates.forEach({
                                                $0.didCompleteDownloadCycle(
                                                    restored: tuples,
                                                    downloaded: []
                                                )
                                            })
                                        }
                                    }
                                    
                                    completionHandler(thirdResult)
                                    
                                    if case .success(let tuples) = thirdResult, tuples.count > 0 {
                                        Self.markAsAlreadyProcessed(tuples.map({ $0.1.globalIdentifier }))
                                    }
                                }
                                
                            case .failure(let error):
                                handleFailure(error)
                            }
                        }
                    }
                }
            }
        }
    }
}

public let LocalDownloadPipelineProcessor = SHBackgroundOperationProcessor<SHLocalDownloadOperation>()
