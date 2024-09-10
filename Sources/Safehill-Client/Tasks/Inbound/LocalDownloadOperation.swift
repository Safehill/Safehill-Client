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
/// 1. `fetchDescriptors(filteringAssets:filteringGroups:after:completionHandler:)`: the descriptors are fetched from the local server
/// 2. `processDescriptors(_:fromRemote:qos:completionHandler:)` : descriptors are filtered our if
///     - the referenced asset is blacklisted (attemtped to download too many times),
///     - any user referenced in the descriptor is not "retrievabile", or
///     - the asset hasn't finished uploaing (upload status is neither `.notStarted` nor `.failed`)
/// 3. `processAssetsInDescriptors(descriptorsByGlobalIdentifier:qos:completionHandler:)` : descriptors are merged with the local photos library based on localIdentifier, calling the delegate for the matches (`didIdentify(globalToLocalAssets:`). Then for the ones not in the photos library:
///     - for the assets shared by _this_ user, the restoration delegate is called to restore them
///     - the assets shared by from _other_ users are returned so they can be decrypted
/// 4. `decryptFromLocalStore` : for the remainder, the decryption step runs and passes the decrypted low resolution asset to the delegates
///
/// Ideally in the lifecycle of the application, the decryption of the low resolution happens only once.
/// The delegate is responsible for keeping these decrypted assets in memory, or call the `SHServerProxy` to retrieve them again if they are disposed.
///
/// Any new asset that needs to be downloaded from the server while the app is running is taken care of by the sister processor, the `SHRemoteDownloadOperation`.
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
    
    /// This method overrides the behavior of the `SHRemoteDownloadOperation` 
    /// to make the descriptor fetch happen against the local server (instead of the remote server)
    ///
    /// - Returns: the list of descriptors
    ///
    override internal func fetchDescriptors(
        filteringAssets globalIdentifiers: [GlobalIdentifier]? = nil,
        filteringGroups groupIds: [String]? = nil,
        after date: Date?,
        completionHandler: @escaping (Result<[any SHAssetDescriptor], Error>) -> Void
    ) {
        self.log.debug("[\(type(of: self))] fetchDescriptors for \(globalIdentifiers ?? []) filteringGroups=\(groupIds ?? []) after \(date?.iso8601withFractionalSeconds ?? "nil")")
        
        serverProxy.getLocalAssetDescriptors(after: date) { result in
            switch result {
            case .success(let descs):
                
                self.log.debug("[\(type(of: self))] fetched descriptors for gids \(descs.map({ $0.globalIdentifier }))")
                
                let unprocessed = descs.filter({
                    Self.alreadyProcessed.contains($0.globalIdentifier) == false
                })
                
                self.log.debug("[\(type(of: self))] unprocessed gids \(unprocessed.map({ $0.globalIdentifier }))")
                
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
    
    override internal func restore(
        descriptorsByGlobalIdentifier: [GlobalIdentifier: any SHAssetDescriptor],
        nonApplePhotoLibrarySharedBySelfGlobalIdentifiers: [GlobalIdentifier],
        sharedBySelfGlobalIdentifiers: [GlobalIdentifier],
        sharedByOthersGlobalIdentifiers: [GlobalIdentifier],
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        guard !self.isCancelled else {
            log.info("[\(type(of: self))] download task cancelled. Finishing")
            completionHandler(.success(()))
            return
        }
        
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
            
            completionHandler(.success(()))
        }
    }
    
    internal func decryptFromLocalStore(
        descriptorsByGlobalIdentifier original: [GlobalIdentifier: any SHAssetDescriptor],
        filteringKeys: [GlobalIdentifier],
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<[AssetAndDescriptor], Error>) -> Void
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
            
            let downloaderDelegates = self.downloaderDelegates
            
            switch result {
            case .failure(let error):
                self.log.error("[\(type(of: self))] unable to fetch local assets: \(error.localizedDescription)")
                completionHandler(.failure(SHBackgroundOperationError.fatalError("unable to fetch local assets")))
            case .success(let encryptedAssets):
                var errorsByAssetGlobalId = [GlobalIdentifier: Error]()
                let successfullyDecrypted = ThreadSafeAssetAndDescriptors(list: [])
                let dispatchGroup = DispatchGroup()
                
                for (globalAssetId, descriptor) in descriptorsByGlobalIdentifier {
                    
                    dispatchGroup.enter()
                    
                    guard let groupId = descriptor.sharingInfo.sharedWithUserIdentifiersInGroup[self.user.identifier] else {
                        errorsByAssetGlobalId[globalAssetId] = SHBackgroundOperationError.unexpectedData(descriptor.sharingInfo)
                        dispatchGroup.leave()
                        continue
                    }
                    
                    if let encryptedAsset = encryptedAssets[globalAssetId] {
                        self.delegatesQueue.async {
                            downloaderDelegates.forEach({
                                $0.didStartDownloadOfAsset(withGlobalIdentifier: globalAssetId,
                                                           descriptor: descriptor,
                                                           in: groupId)
                            })
                        }
                        
                        localAssetsStore.decryptedAsset(
                            encryptedAsset: encryptedAsset,
                            versions: [.lowResolution],
                            descriptor: descriptor
                        ) { result in
                            switch result {
                                
                            case .failure(let error):
                                self.log.error("[\(type(of: self))] unable to decrypt local asset \(globalAssetId): \(error.localizedDescription)")
                                ///
                                /// Asset could not be decrypted, remove data and metadata from the local asset store.
                                /// if the metadata is present in the local asset store, the RemoteDownloadOperation will ignore it.
                                /// So make sure the metadata isn't present.
                                ///
                                errorsByAssetGlobalId[globalAssetId] = error
                                dispatchGroup.leave()
                                
                            case .success(let decryptedAsset):
                                self.delegatesQueue.async {
                                    downloaderDelegates.forEach({
                                        $0.didCompleteDownload(
                                            of: decryptedAsset,
                                            in: groupId
                                        )
                                    })
                                }
                                
                                Task {
                                    let assetAndDescriptor = AssetAndDescriptor(asset: decryptedAsset, descriptor: descriptor)
                                    await successfullyDecrypted.add(assetAndDescriptor)
                                    dispatchGroup.leave()
                                }
                            }
                        }
                    } else {
                        self.log.error("[\(type(of: self))] unable to find asset \(globalAssetId) locally")
                        ///
                        /// Asset encrypted data could not be retrieved, so remove the metadata too
                        /// so that the asset can be fetched from server again.
                        /// if the metadata is present in the local asset store, the RemoteDownloadOperation will ignore it.
                        /// So make sure the metadata isn't present.
                        ///
                        errorsByAssetGlobalId[globalAssetId] = SHBackgroundOperationError.missingAssetInLocalServer(globalAssetId)
                        
                        dispatchGroup.leave()
                    }
                }
                
                dispatchGroup.notify(queue: .global(qos: qos)) { [weak self] in
                    
                    Task {
                        completionHandler(.success(await successfullyDecrypted.list))
                    }
                    
                    guard let self = self else { return }
                    
                    guard errorsByAssetGlobalId.isEmpty == false else {
                        return
                    }
                    
                    for (gid, error) in errorsByAssetGlobalId {
                        guard let groupId = descriptorsByGlobalIdentifier[gid]?.sharingInfo.sharedWithUserIdentifiersInGroup[self.user.identifier] else {
                            self.log.warning("will not notify delegates about asset decryption error for asset \(gid)")
                            continue
                        }
                        self.delegatesQueue.async {
                            downloaderDelegates.forEach({
                                $0.didFailDownloadOfAsset(
                                    withGlobalIdentifier: gid,
                                    in: groupId,
                                    with: error
                                )
                            })
                        }
                    }
                    
                    self.log.warning("failed to decrypt the following assets: \(errorsByAssetGlobalId)")
                    
                    self.serverProxy.localServer.deleteAssets(
                        withGlobalIdentifiers: Array(errorsByAssetGlobalId.keys)
                    ) { [weak self] deleteResult in
                        if case .failure(let failure) = deleteResult {
                            self?.log.error("failed to delete fail-to-decrypt assets from the local server. The remote download operation will not attempt to re-download them as long as their metadata is stored locally. \(failure.localizedDescription)")
                        }
                    }
                }
            }
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
    public override func run(
        startingFrom date: Date?,
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
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
        
        self.fetchDescriptors(after: date) {
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
                        let delta = Set(fullDescriptorList.map({ $0.globalIdentifier })).subtracting(filteredDescriptors.keys)
                        self.log.debug("[\(type(of: self))] after processing: \(filteredDescriptors.count). delta=\(delta)")
#endif
                        self.processAssetsInDescriptors(
                            descriptorsByGlobalIdentifier: filteredDescriptors,
                            qos: qos
                        ) { processedAssetsResult in
                            
                            switch processedAssetsResult {
                            case .success(let descriptorsReadyToDecrypt):
#if DEBUG
                                let delta1 = Set(filteredDescriptors.keys).subtracting(descriptorsReadyToDecrypt.map({ $0.globalIdentifier }))
                                let delta2 = Set(descriptorsReadyToDecrypt.map({ $0.globalIdentifier })).subtracting(filteredDescriptors.keys)
                                self.log.debug("[\(type(of: self))] ready for decryption: \(descriptorsReadyToDecrypt.count). onlyInProcessed=\(delta1) onlyInToDecrypt=\(delta2)")
#endif
                                
                                self.decryptFromLocalStore(
                                    descriptorsByGlobalIdentifier: filteredDescriptors,
                                    filteringKeys: descriptorsReadyToDecrypt.map({ $0.globalIdentifier }),
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
                                    
                                    if case .success(let decryptedAssetsAndDescriptors) = decryptionResult,
                                       decryptedAssetsAndDescriptors.count > 0 {
                                        Self.markAsAlreadyProcessed(decryptedAssetsAndDescriptors.map({ $0.asset.globalIdentifier }))
                                    }
                                    
                                    completionHandler(.success(()))
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
