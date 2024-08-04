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
/// 1. `fetchDescriptorsForItemsToRestore()` : the descriptors are fetched from the local server
/// 2. `processDescriptors(_:fromRemote:qos:completionHandler:)` : descriptors are filtered our if
///     - the referenced asset is blacklisted (attemtped to download too many times),
///     - any user referenced in the descriptor is not "retrievabile", or
///     - the asset hasn't finished uploaing (upload status is neither `.notStarted` nor `.failed`)
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
    
    public override init(
        user: SHLocalUserProtocol,
        downloaderDelegates: [SHAssetDownloaderDelegate],
        restorationDelegate: SHAssetActivityRestorationDelegate,
        photoIndexer: SHPhotosIndexer
    ) {
        super.init(
            user: user,
            downloaderDelegates: downloaderDelegates,
            restorationDelegate: restorationDelegate,
            photoIndexer: photoIndexer
        )
    }
    
    /// This method overrides the behavior of the `SHDownloadOperation` to make the descriptor fetch
    /// happen against the local server (instead of the remote server)
    /// - Returns: the list of descriptors
    internal func fetchDescriptorsForItemsToRestore(
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
                ) = self.historyItems(
                    from: descriptors,
                    usersDict: usersDict
                )
                
                let restorationDelegate = self.restorationDelegate
                self.delegatesQueue.async {
                    restorationDelegate.restoreUploadHistoryItems(from: groupIdToUploadItems)
                    restorationDelegate.restoreShareHistoryItems(from: groupIdToShareItems)
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
            synchronousFetch: true,
            qos: qos
        ) {
            [weak self] result in
            
            guard let self = self else { return }
            
            switch result {
            case .failure(let error):
                self.log.error("[\(type(of: self))] unable to fetch local assets: \(error.localizedDescription)")
                completionHandler(.failure(SHBackgroundOperationError.fatalError("unable to fetch local assets")))
            case .success(let encryptedAssets):
                var unsuccessfullyDecryptedAssetGids = Set<GlobalIdentifier>()
                var successfullyDecrypted = [(any SHDecryptedAsset, any SHAssetDescriptor)]()
                let dispatchGroup = DispatchGroup()
                
                for (globalAssetId, descriptor) in descriptorsByGlobalIdentifier {
                    guard let groupId = descriptor.sharingInfo.sharedWithUserIdentifiersInGroup[self.user.identifier] else {
                        self.log.critical("malformed descriptor. Missing groupId for user \(self.user.identifier) for assetId \(descriptor.globalIdentifier)")
                        completionHandler(.failure(SHBackgroundOperationError.fatalError("malformed descriptor. Missing groupId for user \(self.user.identifier) for assetId \(descriptor.globalIdentifier)")))
                        return
                    }
                    
                    let downloaderDelegates = self.downloaderDelegates
                    
                    if let encryptedAsset = encryptedAssets[globalAssetId] {
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
                            versions: [.lowResolution],
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
                                        $0.didCompleteDownload(
                                            of: decryptedAsset,
                                            in: groupId
                                        )
                                    })
                                }
                                
                                successfullyDecrypted.append((decryptedAsset, descriptor))
                            }
                            
                            dispatchGroup.leave()
                        }
                    } else {
                        self.delegatesQueue.async {
                            downloaderDelegates.forEach({
                                $0.didFailDownloadOfAsset(
                                    withGlobalIdentifier: globalAssetId,
                                    in: groupId,
                                    with: SHAssetStoreError.failedToRetrieveLocalAsset
                                )
                            })
                        }
                        
                        ///
                        /// Asset encrypted data could not be retrieved, so remove the metadata too
                        /// so that the asset can be fetched from server again.
                        /// As long as the metadata is present in the local asset store, the RemoteDownloadOperation
                        /// will ignore it.
                        ///
                        unsuccessfullyDecryptedAssetGids.insert(globalAssetId)
                    }
                }
                
                dispatchGroup.notify(queue: .global(qos: qos)) {   
                    completionHandler(.success(successfullyDecrypted))
                }
                
                guard unsuccessfullyDecryptedAssetGids.isEmpty == false else {
                    return
                }
                
                self.log.warning("failed to decrypt assets with ids \(unsuccessfullyDecryptedAssetGids)")
                
                self.serverProxy.localServer.deleteAssets(
                    withGlobalIdentifiers: Array(unsuccessfullyDecryptedAssetGids)
                ) { [weak self] deleteResult in
                    if case .failure(let failure) = deleteResult {
                        self?.log.error("failed to delete fail-to-decrypt assets from the local server. The remote download operation will not attempt to re-download them as long as their metadata is stored locally. \(failure.localizedDescription)")
                    }
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
        guard !self.isCancelled else {
            log.info("[\(type(of: self))] download task cancelled. Finishing")
            completionHandler(.success([]))
            return
        }
        
        // TODO: This method does not re-create missing items from the history queues. If an item is missing it will not be restored
        
        ///
        /// FOR THE ONES SHARED BY THIS USER
        /// Notify the restoration delegates about the assets that need to be restored from the successful queues
        /// These can only reference assets shared by THIS user. All other descriptors are ignored.
        ///
        /// These assets are definitely in the local server (because the descriptors fetch by a `SHLocalDownloadOperation`
        /// contain only assets from the local server).
        /// However they may or may not be in the history queues. The method `restoreQueueItems(descriptorsByGlobalIdentifier:filteringKeys:)
        /// takes care of creating them
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
        
        self.fetchDescriptorsForItemsToRestore {
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
#if DEBUG
                        ///
                        /// The `didReceiveLocalAssetDescriptors(_:referencing:)` delegate method is called
                        /// with the descriptors.
                        ///
                        /// NOTE: no assets not referenced in the call to
                        /// ```didReceiveLocalAssetDescriptors(_:referencing:)```
                        /// should be referenced in `didStartDownloadOfAsset`, `didCompleteDownload` or `didFailDownloadOfAsset`.
                        ///
                        let delta = Set(fullDescriptorList.map({ $0.globalIdentifier })).subtracting(filteredDescriptors.keys)
                        self.log.debug("[\(type(of: self))] after processing: \(filteredDescriptors.count). delta=\(delta)")
#endif
                        self.processAssetsInDescriptors(
                            descriptorsByGlobalIdentifier: filteredDescriptors,
                            qos: qos
                        ) { processedAssetsResult in
                            
                            switch processedAssetsResult {
                            case .success(let descriptorsToDecrypt):
#if DEBUG
                                let delta1 = Set(filteredDescriptors.keys).subtracting(descriptorsToDecrypt.map({ $0.globalIdentifier }))
                                let delta2 = Set(descriptorsToDecrypt.map({ $0.globalIdentifier })).subtracting(filteredDescriptors.keys)
                                self.log.debug("[\(type(of: self))] ready for decryption: \(descriptorsToDecrypt.count). onlyInProcessed=\(delta1) onlyInToDecrypt=\(delta2)")
#endif
                                self.decryptFromLocalStore(
                                    descriptorsByGlobalIdentifier: filteredDescriptors,
                                    filteringKeys: descriptorsToDecrypt.map({ $0.globalIdentifier }),
                                    qos: qos
                                ) {
                                    decryptionResult in
                                    
                                    let downloaderDelegates = self.downloaderDelegates
                                    self.delegatesQueue.async {
                                        switch decryptionResult {
                                            
                                        case .failure(let error):
                                            downloaderDelegates.forEach({
                                                $0.didFailDownloadCycle(with: error)
                                            })
                                            
                                        case .success(let decryptedAssetsAndDescriptors):
                                            downloaderDelegates.forEach({
                                                $0.didCompleteDownloadCycle(
                                                    localAssetsAndDescriptors: decryptedAssetsAndDescriptors
                                                )
                                            })
                                        }
                                    }
                                    
                                    completionHandler(decryptionResult)
                                    
                                    if case .success(let decryptedAssetsAndDescriptors) = decryptionResult,
                                        decryptedAssetsAndDescriptors.count > 0 {
                                        Self.markAsAlreadyProcessed(decryptedAssetsAndDescriptors.map({ $0.1.globalIdentifier }))
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
