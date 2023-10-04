import Foundation
import Safehill_Crypto
import KnowledgeBase
import os


public class SHLocalDownloadOperation: SHDownloadOperation {
    
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
        descriptorsByGlobalIdentifier: [GlobalIdentifier: any SHAssetDescriptor],
        completionHandler: @escaping (Swift.Result<Void, Error>) -> Void
    ) {
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
                fatalError("malformed descriptorsByGlobalIdentifier")
            }
            
            for groupId in descriptor.sharingInfo.groupInfoById.keys {
                self.delegate.didStart(globalIdentifier: globalAssetId,
                                       groupId: groupId)
            }
            
            do {
                let decryptedAsset = try localAssetsStore.decryptedAsset(
                    encryptedAsset: encryptedAsset,
                    quality: .lowResolution,
                    descriptor: descriptor
                )
                
                self.delegate.handleLowResAsset(decryptedAsset)
                for groupId in descriptor.sharingInfo.groupInfoById.keys {
                    self.delegate.completed(decryptedAsset.globalIdentifier, groupId: groupId)
                }
            } catch {
                self.log.error("unable to decrypt local asset \(globalAssetId): \(error.localizedDescription)")
                for groupId in descriptor.sharingInfo.groupInfoById.keys {
                    self.delegate.failed(encryptedAsset.globalIdentifier, groupId: groupId)
                }
            }
        }
        
        completionHandler(.success(()))
    }
    
    internal override func processAssetsInDescriptors(
        descriptorsByGlobalIdentifier: [GlobalIdentifier: any SHAssetDescriptor],
        completionHandler: @escaping (Swift.Result<Void, Error>) -> Void
    ) {
        ///
        /// Get all asset descriptors associated with this user from the server.
        /// Descriptors serve as a manifest to determine what to decrypt
        ///
        guard descriptorsByGlobalIdentifier.count > 0 else {
            completionHandler(.success(()))
            return
        }
        
        self.mergeDescriptorsWithLocalAssets(descriptorsByGlobalIdentifier: descriptorsByGlobalIdentifier) { result in
            switch result {
            case .success(let filteredDescriptorsByGlobalIdentifier):
                let start = CFAbsoluteTimeGetCurrent()
                
                ///
                /// Get the encrypted assets for the ones not found in the apple photos library to start decryption.
                ///
                self.decryptFromLocalStore(
                    descriptorsByGlobalIdentifier: filteredDescriptorsByGlobalIdentifier
                ) { result in
                    let end = CFAbsoluteTimeGetCurrent()
                    self.log.debug("[localDownload][PERF] it took \(CFAbsoluteTime(end - start)) to decrypt \(filteredDescriptorsByGlobalIdentifier.count) assets in the local asset store")
                    completionHandler(result)
                }
            case .failure(let error):
                completionHandler(.failure(error))
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
    public func runOnce(completionHandler: @escaping (Swift.Result<Void, Error>) -> Void) {
        self.processDescriptors { result in
            switch result {
            case .failure(let error):
                self.log.error("failed to fetch local descriptors: \(error.localizedDescription)")
                self.delegate.downloadOperationFinished(.failure(error))
                completionHandler(.failure(error))
            case .success(let descriptorsByGlobalIdentifier):
                self.processAssetsInDescriptors(descriptorsByGlobalIdentifier: descriptorsByGlobalIdentifier) {
                    secondResult in
                    self.delegate.downloadOperationFinished(secondResult)
                    completionHandler(secondResult)
                }
            }
        }
    }
}
