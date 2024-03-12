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
///     - for the assets shared by _this_ user, local server assets and queue items are created when missing
///     - for the assets shared by from _other_ users, the authorization is requested for the "unknown" users, and the remaining assets ready for download are returned
/// 4. `decryptFromLocalStore` : for the remainder, the decryption step runs and passes the decrypted asset to the delegates
///
/// The `decryptAssets` flag determines whether step 4 is run.
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
    
    let decryptAssets: Bool
    
    @available(*, unavailable)
    public override init(
        user: SHLocalUserProtocol,
        downloaderDelegates: [SHAssetDownloaderDelegate],
        assetsSyncDelegates: [SHAssetSyncingDelegate],
        threadsSyncDelegates: [SHThreadSyncingDelegate],
        restorationDelegate: SHAssetActivityRestorationDelegate,
        limitPerRun limit: Int? = nil,
        photoIndexer: SHPhotosIndexer? = nil
    ) {
        fatalError("Not supported")
    }
    
    public init(
        user: SHLocalUserProtocol,
        delegates: [SHAssetDownloaderDelegate],
        restorationDelegate: SHAssetActivityRestorationDelegate,
        decryptAssets: Bool
    ) {
        self.decryptAssets = decryptAssets
        super.init(
            user: user,
            downloaderDelegates: delegates,
            assetsSyncDelegates: [],
            threadsSyncDelegates: [],
            restorationDelegate: restorationDelegate
        )
    }
    
    public override func clone() -> SHBackgroundOperationProtocol {
        SHLocalDownloadOperation(
            user: self.user,
            delegates: self.downloaderDelegates,
            restorationDelegate: self.restorationDelegate,
            decryptAssets: self.decryptAssets
        )
    }
    
    /// Clone as above, but override the `decryptAssets` flag
    /// - Parameter decryptAssets: the overriden value
    /// - Returns: the cloned object
    public func clone(decryptAssets: Bool) -> SHBackgroundOperationProtocol {
        SHLocalDownloadOperation(
            user: self.user,
            delegates: self.downloaderDelegates,
            restorationDelegate: self.restorationDelegate,
            decryptAssets: decryptAssets
        )
    }
    
    /// This method overrides the behavior of the `SHDownloadOperation` to make the descriptor fetch
    /// happen against the local server (instead of the remote server)
    /// - Returns: the list of descriptors
    internal func fetchDescriptorsFromServer() throws -> [any SHAssetDescriptor] {
        let group = DispatchGroup()
        
        var descriptors = [any SHAssetDescriptor]()
        var error: Error? = nil
        
        group.enter()
        serverProxy.getLocalAssetDescriptors { result in
            switch result {
            case .success(let descs):
                descriptors = descs
            case .failure(let err):
                error = err
            }
            group.leave()
        }
        
        let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds))
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
        
        guard error == nil else {
            throw error!
        }
        
        return descriptors
    }
    
    internal override func getUsers(
        withIdentifiers userIdentifiers: [UserIdentifier]
    ) throws -> [UserIdentifier: any SHServerUser] {
        try SHUsersController(localUser: self.user).getCachedUsers(withIdentifiers: userIdentifiers)
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
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        // TODO: local queue items restoration is disabled
        /*
        guard original.count > 0 else {
            completionHandler(.success(()))
            return
        }
        
        let descriptorsByGlobalIdentifier = original.filter({ filteringKeys.contains($0.key) })
        
        guard descriptorsByGlobalIdentifier.count > 0 else {
            completionHandler(.success(()))
            return
        }
        
        self.restorationDelegate.didStartRestoration()
        
        var uploadLocalAssetIdByGroupId = [String: Set<String>]()
        var shareLocalAssetIdsByGroupId = [String: Set<String>]()
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
        
        self.log.debug("[localrestoration] upload local asset identifiers by group \(uploadLocalAssetIdByGroupId)")
        self.log.debug("[localrestoration] share local asset identifiers by group \(shareLocalAssetIdsByGroupId)")
        
        for (groupId, localIdentifiers) in uploadLocalAssetIdByGroupId {
            self.restorationDelegate.restoreUploadQueueItems(forLocalIdentifiers: Array(localIdentifiers), in: groupId)
        }
        
        for (groupId, localIdentifiers) in shareLocalAssetIdsByGroupId {
            self.restorationDelegate.restoreShareQueueItems(forLocalIdentifiers: Array(localIdentifiers), in: groupId)
        }
        
        self.restorationDelegate.didCompleteRestoration(
            userIdsInvolvedInRestoration: Array(userIdsInvolvedInRestoration)
        )
        
        completionHandler(.success(()))
        */
    }
    
    
    internal func decryptFromLocalStore(
        descriptorsByGlobalIdentifier original: [GlobalIdentifier: any SHAssetDescriptor],
        filteringKeys: [GlobalIdentifier],
        completionHandler: @escaping (Result<[(any SHDecryptedAsset, any SHAssetDescriptor)], Error>) -> Void
    ) {
        self.log.debug("[localrestoration] attempting to decrypt following assets from local store: \(Array(original.keys))")
        
        guard original.count > 0 else {
            completionHandler(.success([]))
            return
        }
        
        let descriptorsByGlobalIdentifier = original.filter({ filteringKeys.contains($0.key) })
        
        guard descriptorsByGlobalIdentifier.count > 0 else {
            completionHandler(.success([]))
            return
        }
        
        let localAssetsStore = SHLocalAssetStoreController(user: self.user)
        guard let encryptedAssets = try? localAssetsStore.encryptedAssets(
            with: Array(descriptorsByGlobalIdentifier.keys),
            versions: [.lowResolution],
            cacheHiResolution: false
        ) else {
            self.log.error("[localrestoration] unable to fetch local assets")
            completionHandler(.failure(SHBackgroundOperationError.fatalError("unable to fetch local assets")))
            return
        }
        
        var successfullyDecrypted = [(any SHDecryptedAsset, any SHAssetDescriptor)]()
        
        for (globalAssetId, encryptedAsset) in encryptedAssets {
            guard let descriptor = descriptorsByGlobalIdentifier[globalAssetId] else {
                log.critical("[localrestoration] malformed descriptorsByGlobalIdentifier")
                completionHandler(.failure(SHBackgroundOperationError.fatalError("malformed descriptorsByGlobalIdentifier")))
                return
            }
            
            guard let groupId = descriptor.sharingInfo.sharedWithUserIdentifiersInGroup[self.user.identifier] else {
                log.critical("malformed descriptor. Missing groupId for user \(self.user.identifier) for assetId \(descriptor.globalIdentifier)")
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
            
            do {
                let decryptedAsset = try localAssetsStore.decryptedAsset(
                    encryptedAsset: encryptedAsset,
                    quality: .lowResolution,
                    descriptor: descriptor
                )
                
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
            } catch {
                self.log.error("[localrestoration] unable to decrypt local asset \(globalAssetId): \(error.localizedDescription)")
                
                self.delegatesQueue.async {
                    downloaderDelegates.forEach({
                        $0.didFailDownloadOfAsset(
                            withGlobalIdentifier: encryptedAsset.globalIdentifier,
                            in: groupId,
                            with: error
                        )
                    })
                }
            }
        }
        
        completionHandler(.success(successfullyDecrypted))
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
        /// FOR THE ONES SHARED BY THIS USER
        /// Notify the restoration delegates about the assets that need to be restored from the successful queues
        /// These can only reference assets shared by THIS user. All other descriptors are ignored
        ///
        self.restoreQueueItems(
            descriptorsByGlobalIdentifier: descriptorsByGlobalIdentifier,
            filteringKeys: sharedBySelfGlobalIdentifiers
        ) { _ in }
        
        ///
        /// FOR THE ONES SHARED BY OTHER USERS + NOT IN THE APPLE PHOTOS LIBRARY
        /// Decrypt the remainder from the local store, namely assets:
        /// - not in the apple photos library (we'll use the apple photos version as long as it exists)
        /// - not shared by self (until multi-device is implemented, these items are expected to be in the apple photos library)
        
        // TODO: IMPLEMENT THIS because activities and assets that no longer exist in (or app has no longer access to from) the Apple photos library are problematic otherwise
        
        ///
        /// Remember: because the full manifest of descriptors (`descriptorsByGlobalIdentifier`)
        /// is fetched from LocalStore, all assets are guaranteed to be found
        ///
        let toDecryptFromLocalStoreGids = Set(nonApplePhotoLibrarySharedBySelfGlobalIdentifiers)
            .union(sharedByOthersGlobalIdentifiers)
        
        completionHandler(.success(Array(
            descriptorsByGlobalIdentifier.values.filter({
                toDecryptFromLocalStoreGids.contains($0.globalIdentifier)
            })
        )))
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
        for assetGlobalIdentifiers: [GlobalIdentifier]? = nil,
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<[(any SHDecryptedAsset, any SHAssetDescriptor)], Error>) -> Void
    ) {
        let fullDescriptorList: [any SHAssetDescriptor]
        do {
            fullDescriptorList = try self.fetchDescriptorsFromServer()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        self.log.debug("[localrestoration] original descriptors: \(fullDescriptorList.count)")
        self.processDescriptors(fullDescriptorList, qos: qos) { result in
            switch result {
            case .failure(let error):
                self.log.error("[localrestoration] failed to fetch local descriptors: \(error.localizedDescription)")
                let downloaderDelegates = self.downloaderDelegates
                self.delegatesQueue.async {
                    downloaderDelegates.forEach({
                        $0.didCompleteDownloadCycle(with: .failure(error))
                    })
                }
                completionHandler(.failure(error))
            case .success(let descriptorsByGlobalIdentifier):
#if DEBUG
                let delta = Set(fullDescriptorList.map({ $0.globalIdentifier })).subtracting(descriptorsByGlobalIdentifier.keys)
                self.log.debug("[localrestoration] after processing: \(descriptorsByGlobalIdentifier.count). delta=\(delta)")
#endif
                self.processAssetsInDescriptors(
                    descriptorsByGlobalIdentifier: descriptorsByGlobalIdentifier,
                    qos: qos
                ) { secondResult in
                    
                    switch secondResult {
                    case .success(let descriptorsToDecrypt):
#if DEBUG
                        let delta1 = Set(descriptorsByGlobalIdentifier.keys).subtracting(descriptorsToDecrypt.map({ $0.globalIdentifier }))
                        let delta2 = Set(descriptorsToDecrypt.map({ $0.globalIdentifier })).subtracting(descriptorsByGlobalIdentifier.keys)
                        self.log.debug("[localrestoration] ready for decryption: \(descriptorsToDecrypt.count). onlyInProcessed=\(delta1) onlyInToDecrypt=\(delta2)")
#endif
                        if self.decryptAssets {
                            self.decryptFromLocalStore(
                                descriptorsByGlobalIdentifier: descriptorsByGlobalIdentifier,
                                filteringKeys: descriptorsToDecrypt.map({ $0.globalIdentifier })
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
                        } else {
                            self.log.debug("[localrestoration] skipping decryption step")
                            
                            let downloaderDelegates = self.downloaderDelegates
                            self.delegatesQueue.async {
                                downloaderDelegates.forEach({
                                    $0.didCompleteDownloadCycle(with: .success([]))
                                })
                            }
                        }
                        
                    case .failure(let error):
                        completionHandler(.failure(error))
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
