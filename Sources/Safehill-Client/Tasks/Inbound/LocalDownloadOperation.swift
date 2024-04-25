import Foundation
import Safehill_Crypto
import KnowledgeBase
import os


///
/// This pipeline operation deals with assets in the LocalServer and the Apple Photos Library.
/// **IT DOES NOT** deal with remote server or remote descriptors. See `SHRemoteDwonloadOperation` for remote descriptor processing.
/// It is responsible for identifying the mapping between the assets in the two sets, and then decrypt the assets in the local store for the ones that don't have a mapping to the local photo library.
///
/// The steps are:
/// 1. `fetchDescriptorsFromServer()` : the descriptors are fetched from the local server
/// 2. `processDescriptors(_:qos:completionHandler:)` : descriptors are filtered based on blacklisting of users or assets, "retrievability" of users, or if the asset upload status is neither `.started` nor `.partial`. Both known and unknwon users are included, but the delegate method `didReceiveAssetDescriptors(_:referencing:)` is called for the ones from "known" users. A known user is a user that is present in the knowledge graph and is also "retrievable", namely _this_ user can fetch its details from the server. This further segmentation is required because the delegate method `didReceiveAuthorizationRequest(for:referencing:)` is called for the "unknown" (aka still unauthorized) users
/// 3. `processAssetsInDescriptors(descriptorsByGlobalIdentifier:qos:completionHandler:)` : descriptors are merged with the local photos library based on localIdentifier, calling the delegate for the matches (`didIdentify(globalToLocalAssets:`). Then for the ones not in the photos library:
///     - for the assets shared by _this_ user, the restoration delegate is called to restore them
///     - for the assets shared by from _other_ users, the authorization is requested for the "unknown" users, and the remaining assets ready for download are returned
/// 4. `decryptFromLocalStore` : for the remainder, the decryption step runs and passes the decrypted asset to the delegates
///
/// The `isRestoring` flag determines whether step 4 is run.
/// Usually in the lifecycle of the application, the decryption happens only once.
/// The delegate is responsible for keeping these decrypted assets in memory, or call the `SHServerProxy` to retrieve them again if disposed of.
/// Hence, it's advised to run it with that flag set to true once (at start) and then run it continously with that flag set to false.
/// Any new asset that needs to be downloaded from the server while the app is running is taken care of by the sister processor,
/// running `SHRemoteDownloadOperation`s.
///
/// The pipeline sequence is:
/// ```
/// 1 -->   2 -->    3 -->    4 (optional)
/// ```
///
public class SHLocalDownloadOperation: SHRemoteDownloadOperation {
    
    let isRestoring: Bool
    
    @available(*, unavailable)
    public override init(
        user: SHLocalUserProtocol,
        downloaderDelegates: [SHAssetDownloaderDelegate],
        assetsSyncDelegates: [SHAssetSyncingDelegate],
        threadsSyncDelegates: [SHThreadSyncingDelegate],
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
        isRestoring: Bool,
        photoIndexer: SHPhotosIndexer
    ) {
        self.isRestoring = isRestoring
        super.init(
            user: user,
            downloaderDelegates: delegates,
            assetsSyncDelegates: [],
            threadsSyncDelegates: [],
            restorationDelegate: restorationDelegate,
            photoIndexer: photoIndexer
        )
    }
    
    public override func clone() -> SHBackgroundOperationProtocol {
        SHLocalDownloadOperation(
            user: self.user,
            delegates: self.downloaderDelegates,
            restorationDelegate: self.restorationDelegate,
            isRestoring: self.isRestoring,
            photoIndexer: self.photoIndexer
        )
    }
    
    /// Clone as above, but override the `isRestoring` flag
    /// - Parameter isRestoring: the overriden value
    /// - Returns: the cloned object
    public func clone(isRestoring: Bool) -> SHBackgroundOperationProtocol {
        SHLocalDownloadOperation(
            user: self.user,
            delegates: self.downloaderDelegates,
            restorationDelegate: self.restorationDelegate,
            isRestoring: isRestoring,
            photoIndexer: self.photoIndexer
        )
    }
    
    /// This method overrides the behavior of the `SHDownloadOperation` to make the descriptor fetch
    /// happen against the local server (instead of the remote server)
    /// - Returns: the list of descriptors
    internal func fetchDescriptorsFromLocalServer(
        completionHandler: @escaping (Result<[any SHAssetDescriptor], Error>) -> Void
    ) {
        serverProxy.getLocalAssetDescriptors { result in
            switch result {
            case .success(let descs):
                completionHandler(.success(descs))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    internal override func getUsers(
        withIdentifiers userIdentifiers: [UserIdentifier],
        completionHandler: @escaping (Result<[UserIdentifier: any SHServerUser], Error>) -> Void
    ) {
        SHUsersController(localUser: self.user).getUsersOrCached(with: userIdentifiers, completionHandler: completionHandler)
    }
    
    ///
    /// For all the descriptors whose originator user is _this_ user, notify the restoration delegate
    /// about all groups that need to be restored.
    /// Uploads and shares will be reported separately, according to the contract in the delegate.
    /// Because these assets exist in the local server, the assumption is that they will also be present in the
    /// upload and history queues, so they don't need to be re-created, as it happens in the sister method
    /// `SHRemoteDownloadOperation::restoreQueueItems(descriptorsByGlobalIdentifier:qos:completionHandler:)`
    ///
    /// - Parameters:
    ///   - descriptorsByGlobalIdentifier: all the descriptors keyed by asset global identifier
    ///   - completionHandler: the callback method
    ///
    func restoreQueueItems(
        descriptorsByGlobalIdentifier original: [GlobalIdentifier: any SHAssetDescriptor],
        filteringKeys: [GlobalIdentifier],
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        guard original.count > 0 else {
            completionHandler(.success(()))
            return
        }
        
        let descriptors = original.values.filter({
            filteringKeys.contains($0.globalIdentifier)
            && $0.sharingInfo.sharedByUserIdentifier == self.user.identifier
            && $0.localIdentifier != nil
        })
        
        guard descriptors.count > 0 else {
            completionHandler(.success(()))
            return
        }
        
        let restorationDelegate = self.restorationDelegate
        self.delegatesQueue.async {
            restorationDelegate.didStartRestoration()
        }
        
        var uploadLocalAssetIdByGroupId = [String: Set<LocalIdentifier>]()
        var shareLocalAssetIdsByGroupId = [String: Set<LocalIdentifier>]()
        var userIdsInvolvedInRestoration = Set<UserIdentifier>()
        
        for descriptor in descriptors {
            
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
                
                if uploadLocalAssetIdByGroupId[groupId] == nil {
                    uploadLocalAssetIdByGroupId[groupId] = [localIdentifier]
                } else {
                    uploadLocalAssetIdByGroupId[groupId]!.insert(localIdentifier)
                }
                
                userIdsInvolvedInRestoration.insert(recipientUserId)
            } else {
                for (recipientUserId, groupId) in descriptor.sharingInfo.sharedWithUserIdentifiersInGroup {
                    if uploadLocalAssetIdByGroupId[groupId] == nil {
                        uploadLocalAssetIdByGroupId[groupId] = [localIdentifier]
                    } else {
                        uploadLocalAssetIdByGroupId[groupId]!.insert(localIdentifier)
                    }
                    if shareLocalAssetIdsByGroupId[groupId] == nil {
                        shareLocalAssetIdsByGroupId[groupId] = [localIdentifier]
                    } else {
                        shareLocalAssetIdsByGroupId[groupId]!.insert(localIdentifier)
                    }
                    
                    userIdsInvolvedInRestoration.insert(recipientUserId)
                }
            }
        }
        
        self.log.debug("[\(type(of: self))] upload local asset identifiers by group \(uploadLocalAssetIdByGroupId)")
        self.log.debug("[\(type(of: self))] share local asset identifiers by group \(shareLocalAssetIdsByGroupId)")
        
        self.delegatesQueue.async {
            for (groupId, localIdentifiers) in uploadLocalAssetIdByGroupId {
                restorationDelegate.restoreUploadQueueItems(forLocalIdentifiers: Array(localIdentifiers), in: groupId)
            }
            
            for (groupId, localIdentifiers) in shareLocalAssetIdsByGroupId {
                restorationDelegate.restoreShareQueueItems(forLocalIdentifiers: Array(localIdentifiers), in: groupId)
            }
            
            restorationDelegate.didCompleteRestoration(
                userIdsInvolvedInRestoration: Array(userIdsInvolvedInRestoration)
            )
        }
        
        completionHandler(.success(()))
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
        /// 
        /// Only call the restoration delegate for items to restore from the history queues
        /// when restoring. Otherwise the delegate will be called at every cycle
        /// with items that have already been restored
        ///
        guard isRestoring else {
            self.log.debug("[\(type(of: self))] skipping restoration and local decryption step")
            
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
        /// However they may or may not be in the history queues. The method `restoreQueueItems(descriptors
        ///
        self.restoreQueueItems(
            descriptorsByGlobalIdentifier: descriptorsByGlobalIdentifier,
            filteringKeys: sharedBySelfGlobalIdentifiers
        ) { _ in
            
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
    public override func runOnce(
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<[(any SHDecryptedAsset, any SHAssetDescriptor)], Error>) -> Void
    ) {
        let handleFailure = { (error: Error) in
            let downloaderDelegates = self.downloaderDelegates
            self.delegatesQueue.async {
                downloaderDelegates.forEach({
                    $0.didCompleteDownloadCycle(with: .failure(error))
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
                self.processDescriptors(fullDescriptorList, qos: qos) { result in
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
                        /// The `didReceiveAssetDescriptors(_:referencing:)` delegate method is called
                        /// with descriptors from known users only.
                        /// Hence, the decryption should only happen for those, and - in turn - we only want to call the downloader
                        /// delegate for assets that are not from known users. No assets not referenced in the call to
                        /// `didReceiveAssetDescriptors(_:referencing:)` should be referenced in `didStartDownloadOfAsset`, `didCompleteDownloadOfAsset` or `didFailDownloadOfAsset`.
                        ///
                        /// The reason why the `didReceiveAssetDescriptors` method is called with descriptors for known users
                        /// is because the `SHRemoteDownloadOperation` treats differently known (not needing authorization) and unknown users (needing authorization).
                        /// In order to avoid initiating/starting a download for an asset from an unknown user,
                        /// `didReceiveAssetDescriptors` should never reference those.
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
                                        downloaderDelegates.forEach({
                                            $0.didCompleteDownloadCycle(with: thirdResult)
                                        })
                                    }
                                    
                                    completionHandler(thirdResult)
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


// MARK: - Local Download Operation Processor

public class SHLocalDownloadPipelineProcessor : SHBackgroundOperationProcessor<SHLocalDownloadOperation> {
    
    public static var shared = SHLocalDownloadPipelineProcessor(
        delayedStartInSeconds: 0,
        dispatchIntervalInSeconds: 5
    )
    private override init(delayedStartInSeconds: Int,
                          dispatchIntervalInSeconds: Int? = nil) {
        super.init(delayedStartInSeconds: delayedStartInSeconds, dispatchIntervalInSeconds: dispatchIntervalInSeconds)
    }
}
