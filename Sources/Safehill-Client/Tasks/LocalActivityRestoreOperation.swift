import Foundation
import Safehill_Crypto
import KnowledgeBase
import os


public class SHLocalActivityRestoreOperation: SHDownloadOperation {
    
    @available(*, unavailable)
    public override init(user: SHLocalUser,
                         delegates: [SHAssetDownloaderDelegate],
                         restorationDelegate: SHAssetActivityRestorationDelegate,
                         limitPerRun limit: Int? = nil,
                         photoIndexer: SHPhotosIndexer? = nil) {
        fatalError("Not supported")
    }
    
    public init(user: SHLocalUser,
                delegates: [SHAssetDownloaderDelegate],
                restorationDelegate: SHAssetActivityRestorationDelegate) {
        super.init(user: user, delegates: delegates, restorationDelegate: restorationDelegate)
    }
    
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
    
    internal func decryptFromLocalStore(
        descriptorsByGlobalIdentifier original: [GlobalIdentifier: any SHAssetDescriptor],
        filteringKeys: [GlobalIdentifier],
        completionHandler: @escaping (Swift.Result<Void, Error>) -> Void
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
        
        let localAssetsStore = SHLocalAssetStoreController(user: self.user)
        guard let encryptedAssets = try? localAssetsStore.encryptedAssets(
            with: Array(descriptorsByGlobalIdentifier.keys),
            versions: [.lowResolution],
            cacheHiResolution: false
        ) else {
            self.log.error("unable to fetch local assets")
            completionHandler(.failure(SHBackgroundOperationError.fatalError("unable to fetch local assets")))
            return
        }
        
        for (globalAssetId, encryptedAsset) in encryptedAssets {
            guard let descriptor = descriptorsByGlobalIdentifier[globalAssetId] else {
                log.critical("malformed descriptorsByGlobalIdentifier")
                completionHandler(.failure(SHBackgroundOperationError.fatalError("malformed descriptorsByGlobalIdentifier")))
                return
            }
            
            for groupId in descriptor.sharingInfo.groupInfoById.keys {
                self.delegates.forEach({
                    $0.didStartDownloadOfAsset(withGlobalIdentifier: globalAssetId, in: groupId)
                })
            }
            
            do {
                let decryptedAsset = try localAssetsStore.decryptedAsset(
                    encryptedAsset: encryptedAsset,
                    quality: .lowResolution,
                    descriptor: descriptor
                )
                
                self.delegates.forEach({
                    $0.didFetchLowResolutionAsset(decryptedAsset)
                })
                for groupId in descriptor.sharingInfo.groupInfoById.keys {
                    self.delegates.forEach({
                        $0.didCompleteDownloadOfAsset(
                            withGlobalIdentifier: encryptedAsset.globalIdentifier,
                            in: groupId
                        )
                    })
                }
            } catch {
                self.log.error("unable to decrypt local asset \(globalAssetId): \(error.localizedDescription)")
                for groupId in descriptor.sharingInfo.groupInfoById.keys {
                    self.delegates.forEach({
                        $0.didFailDownloadOfAsset(
                            withGlobalIdentifier: encryptedAsset.globalIdentifier,
                            in: groupId,
                            with: error
                        )
                    })
                }
            }
        }
        
        completionHandler(.success(()))
    }
    
    override func processForDownload(
        descriptorsByGlobalIdentifier: [GlobalIdentifier: any SHAssetDescriptor],
        nonApplePhotoLibrarySharedBySelfGlobalIdentifiers: [GlobalIdentifier],
        sharedBySelfGlobalIdentifiers: [GlobalIdentifier],
        sharedByOthersGlobalIdentifiers: [GlobalIdentifier],
        completionHandler: @escaping (Swift.Result<Void, Error>) -> Void
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
        let toDecryptFromLocalStoreGids = Set(nonApplePhotoLibrarySharedBySelfGlobalIdentifiers).union(sharedByOthersGlobalIdentifiers)
        
        self.decryptFromLocalStore(
            descriptorsByGlobalIdentifier: descriptorsByGlobalIdentifier,
            filteringKeys: Array(toDecryptFromLocalStoreGids),
            completionHandler: completionHandler
        )
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
    public func runOnce(completionHandler: @escaping (Swift.Result<Void, Error>) -> Void) {
        self.processDescriptors { result in
            switch result {
            case .failure(let error):
                self.log.error("failed to fetch local descriptors: \(error.localizedDescription)")
                self.delegates.forEach({
                    $0.didCompleteDownloadCycle(with: .failure(error))
                })
                completionHandler(.failure(error))
            case .success(let descriptorsByGlobalIdentifier):
                self.processAssetsInDescriptors(descriptorsByGlobalIdentifier: descriptorsByGlobalIdentifier) {
                    secondResult in
                    self.delegates.forEach({
                        $0.didCompleteDownloadCycle(with: secondResult)
                    })
                    completionHandler(secondResult)
                }
            }
        }
    }
}
