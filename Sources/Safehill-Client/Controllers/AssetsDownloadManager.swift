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
        
        user.serverProxy.getRemoteAssets(
            withGlobalIdentifiers: [globalIdentifier],
            versions: versions
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
            log.debug("[PERF] \(CFAbsoluteTime(end - start)) for version \(versions)")
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
        Task {
            
            log.info("[downloadManager] downloading assets with identifier \(descriptor.globalIdentifier)")
            
            let start = CFAbsoluteTimeGetCurrent()
            let globalIdentifier = descriptor.globalIdentifier
            
            // MARK: Start
            
            guard await SHDownloadBlacklist.shared.isBlacklisted(assetGlobalIdentifier: descriptor.globalIdentifier) == false else {
                do {
                    try SHKGQuery.removeAssets(with: [globalIdentifier])
                    completionHandler(.failure(SHAssetDownloadError.assetIsBlacklisted(globalIdentifier)))
                } catch {
                    log.error("[downloadManager] attempt to remove asset \(globalIdentifier) the knowledgeGraph because it's blacklisted FAILED. \(error.localizedDescription)")
                    completionHandler(.failure(error))
                }
                return
            }
            
            // MARK: Get Low Res asset
            
            self.fetchAsset(
                withGlobalIdentifier: globalIdentifier,
                descriptor: descriptor
            ) { result in
                
                switch result {
                case .success(let decryptedAsset):
                    completionHandler(.success(decryptedAsset))
                    
                    Task(priority: .low) {
                        await SHDownloadBlacklist.shared.removeFromBlacklist(assetGlobalIdentifier: globalIdentifier)
                    }
                case .failure(let error):
                    completionHandler(.failure(error))
                    
                    Task(priority: .low) {
                        if error is SHCypher.DecryptionError {
                            await SHDownloadBlacklist.shared.blacklist(globalIdentifier: globalIdentifier)
                        } else {
                            await SHDownloadBlacklist.shared.recordFailedAttempt(globalIdentifier: globalIdentifier)
                        }
                    }
                }
                
                let end = CFAbsoluteTimeGetCurrent()
                log.debug("[PERF] it took \(CFAbsoluteTime(end - start)) to download asset \(globalIdentifier)")
            }
        }
    }
}
