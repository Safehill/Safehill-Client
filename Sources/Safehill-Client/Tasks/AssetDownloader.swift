//
//  AssetDownloader.swift
//  Safehill-Client
//
//  Created by Gennaro Frazzingaro on 9/12/21.
//

import Foundation
import Safehill_Crypto

public protocol SHAssetDownloaderDelegate {
    func localIdentifiersInCache() -> [String]
    func globalIdentifiersInCache() -> [String]
    func handleAssetDescriptorResults(for: [SHAssetDescriptor])
    func handleAssetResults(for: [SHDecryptedAsset])
    func completionHandler(_: Swift.Result<Void, Error>) -> Void
}

public class SHDownloadOperation: SHAbstractBackgroundOperation, SHBackgroundOperationProtocol {
    
    let user: SHLocalUser
    let delegate: SHAssetDownloaderDelegate
    
    public init(user: SHLocalUser, delegate: SHAssetDownloaderDelegate) {
        self.user = user
        self.delegate = delegate
    }
    
    public func clone() -> SHBackgroundOperationProtocol {
        SHDownloadOperation(user: self.user, delegate: self.delegate)
    }
    
    public func runOnce(completionHandler: @escaping (Swift.Result<Void, Error>) -> Void) {
        let serverProxy = SHServerProxy(user: self.user)
        
        /// Fetching assets from the ServerProxy is a 2-step process
        /// 1. Get the descriptors (no data) to determine which assets to pull. This calls the delegate with Assets with empty `encryptedData` (resulting in `downloadInProgress` to be `true`
        /// 2. Get the assets data for the assets not already downloaded (based on the descriptors), and call the delegate with Assets with the low-rez `encryptedData` (resulting in `downloadInProgress` to be `false`)
        serverProxy.getAssetDescriptors { result in
            switch result {
            case .success(let descriptors):
                let globalIdentifiers = Set(descriptors.map { $0.globalIdentifier })
                let globalIdentifiersToDownload = Array(globalIdentifiers.symmetricDifference(self.delegate.globalIdentifiersInCache()))
                
                // Call the delegate using Assets with empty data, created based on their descriptor
                if descriptors.count > 0 {
                    self.delegate.handleAssetDescriptorResults(for: descriptors)
                }
                
                if globalIdentifiersToDownload.count == 0 {
                    completionHandler(.success(()))
                    return
                }
                
                // Call the delegate again using Assets, populating the data field this time
                serverProxy.getHiResAssets(withGlobalIdentifiers: globalIdentifiersToDownload) {
                    result in
                    switch result {
                    case .success(let assetsDict):
                        if assetsDict.count > 0 {
                            var decryptedAssets = [SHDecryptedAsset]()
                            for (_, asset) in assetsDict {
                                do {
                                    let descriptorIdx = descriptors.firstIndex { $0.globalIdentifier == asset.globalIdentifier }
                                    let descriptor = descriptors[descriptorIdx!]
                                    let user: SHServerUser
                                    if descriptor.sharedByUserIdentifier == self.user.identifier {
                                        user = self.user
                                    } else {
                                        // TODO: Get the keys for this user, either from local cache or from server
                                        user = self.user
                                    }
                                    let decryptedAsset = try self.user.decrypt(asset, receivedFrom: user)
                                    decryptedAssets.append(decryptedAsset)
                                } catch {
                                    print("failed to decrypt asset: \(error)")
                                }
                            }
                            self.delegate.handleAssetResults(for: decryptedAssets)
                        }
                        completionHandler(.success(()))
                    case .failure(let err):
                        print("Unable to download low rez assets \(globalIdentifiers) from server: \(err)")
                        completionHandler(.failure(err))
                    }
                }
            case .failure(let err):
                print("Unable to download descriptors from server: \(err)")
                completionHandler(.failure(err))
            }
        }
        
    }
    
    public override func main() {
        guard !self.isCancelled else {
            state = .finished
            return
        }
        
        state = .executing
        
        self.runOnce(completionHandler: self.delegate.completionHandler)
        
        state = .finished
    }
}


class SHAssetsDownloadQueueProcessor : SHOperationQueueProcessor<SHDownloadOperation> {
}
