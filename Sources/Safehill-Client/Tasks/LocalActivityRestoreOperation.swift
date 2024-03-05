import Foundation
import Safehill_Crypto
import KnowledgeBase
import os


public class SHLocalActivityRestoreOperation: SHDownloadOperation {
    
    @available(*, unavailable)
    public override init(user: SHLocalUserProtocol,
                         downloaderDelegates: [SHAssetDownloaderDelegate],
                         assetsSyncDelegates: [SHAssetSyncingDelegate],
                         threadsSyncDelegates: [SHThreadSyncingDelegate],
                         restorationDelegate: SHAssetActivityRestorationDelegate,
                         limitPerRun limit: Int? = nil,
                         photoIndexer: SHPhotosIndexer? = nil) {
        fatalError("Not supported")
    }
    
    public init(user: SHLocalUserProtocol,
                delegates: [SHAssetDownloaderDelegate],
                restorationDelegate: SHAssetActivityRestorationDelegate) {
        super.init(
            user: user,
            downloaderDelegates: delegates,
            assetsSyncDelegates: [],
            threadsSyncDelegates: [],
            restorationDelegate: restorationDelegate
        )
    }
    
    /// This method overrides the behavior of the `SHDownloadOperation` to make the descriptor fetch
    /// happen against the local server (instead of the remote server)
    /// - Returns: the list of descriptors
    internal override func fetchDescriptorsFromServer() throws -> [any SHAssetDescriptor] {
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
        
        self.log.debug("upload local asset identifiers by group \(uploadLocalAssetIdByGroupId)")
        self.log.debug("share local asset identifiers by group \(shareLocalAssetIdsByGroupId)")
        
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
            
            let groupId = descriptor.sharingInfo.sharedWithUserIdentifiersInGroup[self.user.identifier]!
            self.delegatesQueue.async { [weak self] in
                self?.downloaderDelegates.forEach({
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
                
                self.delegatesQueue.async { [weak self] in
                    self?.downloaderDelegates.forEach({
                        $0.didFetchLowResolutionAsset(decryptedAsset)
                    })
                    self?.downloaderDelegates.forEach({
                        $0.didCompleteDownloadOfAsset(
                            withGlobalIdentifier: encryptedAsset.globalIdentifier,
                            in: groupId
                        )
                    })
                }
                
                successfullyDecrypted.append((decryptedAsset, descriptor))
            } catch {
                self.log.error("[localrestoration] unable to decrypt local asset \(globalAssetId): \(error.localizedDescription)")
                
                self.delegatesQueue.async { [weak self] in
                    self?.downloaderDelegates.forEach({
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
    
    override func processForDownload(
        descriptorsByGlobalIdentifier: [GlobalIdentifier: any SHAssetDescriptor],
        nonApplePhotoLibrarySharedBySelfGlobalIdentifiers: [GlobalIdentifier],
        sharedBySelfGlobalIdentifiers: [GlobalIdentifier],
        sharedByOthersGlobalIdentifiers: [GlobalIdentifier],
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
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        let descriptors: [any SHAssetDescriptor]
        do {
            descriptors = try self.fetchDescriptorsFromServer()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        self.log.debug("[localrestoration] original descriptors: \(descriptors.count)")
        self.processDescriptors(descriptors, priority: .high) { result in
            switch result {
            case .failure(let error):
                self.log.error("[localrestoration] failed to fetch local descriptors: \(error.localizedDescription)")
                self.delegatesQueue.async { [weak self] in
                    self?.downloaderDelegates.forEach({
                        $0.didCompleteDownloadCycle(with: .failure(error))
                    })
                }
                completionHandler(.failure(error))
            case .success(let descriptorsByGlobalIdentifier):
#if DEBUG
                let delta = Set(descriptors.map({ $0.globalIdentifier })).subtracting(descriptorsByGlobalIdentifier.keys)
                self.log.debug("[localrestoration] after processing: \(descriptorsByGlobalIdentifier.count). delta=\(delta)")
#endif
                self.processAssetsInDescriptors(
                    descriptorsByGlobalIdentifier: descriptorsByGlobalIdentifier
                ) { secondResult in
                    
                    switch secondResult {
                    case .success(let descriptorsToDecrypt):
#if DEBUG
                        let delta1 = Set(descriptorsByGlobalIdentifier.keys).subtracting(descriptorsToDecrypt.map({ $0.globalIdentifier }))
                        let delta2 = Set(descriptorsToDecrypt.map({ $0.globalIdentifier })).subtracting(descriptorsByGlobalIdentifier.keys)
                        self.log.debug("[localrestoration] ready for decryption: \(descriptorsToDecrypt.count). onlyInProcessed=\(delta1) onlyInToDecrypt=\(delta2)")
#endif
                        
                        self.decryptFromLocalStore(
                            descriptorsByGlobalIdentifier: descriptorsByGlobalIdentifier,
                            filteringKeys: descriptorsToDecrypt.map({ $0.globalIdentifier })
                        ) {
                            thirdResult in
                            self.delegatesQueue.async { [weak self] in
                                self?.downloaderDelegates.forEach({
                                    $0.didCompleteDownloadCycle(with: thirdResult)
                                })
                            }
                            completionHandler(.success(()))
                        }
                        
                    case .failure(let error):
                        completionHandler(.failure(error))
                    }
                }
            }
        }
    }
}
