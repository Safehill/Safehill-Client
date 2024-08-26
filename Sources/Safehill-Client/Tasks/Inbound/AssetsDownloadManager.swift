import Foundation
import KnowledgeBase
import Safehill_Crypto

public enum SHAssetDownloadError: Error {
    case assetIsBlacklisted(GlobalIdentifier)
}

public struct SHAssetsDownloadManager {
    let user: SHAuthenticatedLocalUser
    
    public init(user: SHAuthenticatedLocalUser) {
        self.user = user
    }
    
    private func fetchAsset(
        withGlobalIdentifier globalIdentifier: GlobalIdentifier,
        descriptor: any SHAssetDescriptor,
        completionHandler: @escaping (Result<any SHDecryptedAsset, Error>) -> Void
    ) {
        let start = CFAbsoluteTimeGetCurrent()
        
        /// Get only the low resolution. Mid or hi resolution are on-request (when an image is shown larger on screen)
        let versions: [SHAssetQuality] = [.lowResolution]
        
        user.serverProxy.getAssetsAndCache(
            withGlobalIdentifiers: [globalIdentifier],
            versions: versions,
            synchronousFetch: true
        )
        { result in
            switch result {
            case .success(let assetsDict):
                guard assetsDict.count > 0,
                      let encryptedAsset = assetsDict[globalIdentifier] else {
                    log.error("[downloadManager] failed to retrieve asset \(globalIdentifier) version \(versions)")
                    completionHandler(.failure(SHHTTPError.ClientError.notFound))
                    return
                }
                let localAssetStore = SHLocalAssetStoreController(
                    user: self.user
                )
                
                log.info("[downloadManager] retrieved asset with identifier \(globalIdentifier). Decrypting")
                localAssetStore.decryptedAsset(
                    encryptedAsset: encryptedAsset,
                    versions: versions,
                    descriptor: descriptor
                ) { result in
                    switch result {
                    case .failure(let error):
                        completionHandler(.failure(error))
                    case .success(let decryptedAsset):
                        completionHandler(.success(decryptedAsset))
                    }
                }
            case .failure(let err):
                log.critical("[downloadManager] unable to download assets \(globalIdentifier) version \(versions) from server: \(err.localizedDescription)")
                completionHandler(.failure(err))
            }
            let end = CFAbsoluteTimeGetCurrent()
            log.debug("[PERF] \(CFAbsoluteTime(end - start)) for versions \(versions)")
        }
    }
    
    /// Downloads the asset for the given descriptor, decrypts it, and returns the decrypted version, or an error.
    /// - Parameters:
    ///   - descriptor: the descriptor for the assets to download
    ///   - completionHandler: the callback
    func downloadAsset(
        for descriptor: any SHAssetDescriptor,
        completionHandler: @escaping (Result<any SHDecryptedAsset, Error>) -> Void
    ) {
        DispatchQueue.global().async {
            
            log.info("[downloadManager] downloading assets with identifier \(descriptor.globalIdentifier)")
            
            let globalIdentifier = descriptor.globalIdentifier
            
            // MARK: Get Low Res asset
            
            self.fetchAsset(
                withGlobalIdentifier: globalIdentifier,
                descriptor: descriptor,
                completionHandler: completionHandler
            )
        }
    }
}
