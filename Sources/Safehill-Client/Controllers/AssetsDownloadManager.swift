import Foundation
import KnowledgeBase
import Safehill_Crypto

public struct SHAssetDownloadAuthorizationResponse {
    public let descriptors: [any SHAssetDescriptor]
    public let users: [UserIdentifier: any SHServerUser]
}

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
        quality: SHAssetQuality,
        descriptor: any SHAssetDescriptor,
        completionHandler: @escaping (Result<any SHDecryptedAsset, Error>) -> Void
    ) {
        let start = CFAbsoluteTimeGetCurrent()
        
        log.info("downloading assets with identifier \(globalIdentifier) version \(quality.rawValue)")
        user.serverProxy.getAssets(
            withGlobalIdentifiers: [globalIdentifier],
            versions: [quality]
        )
        { result in
            switch result {
            case .success(let assetsDict):
                guard assetsDict.count > 0,
                      let encryptedAsset = assetsDict[globalIdentifier] else {
                    completionHandler(.failure(SHHTTPError.ClientError.notFound))
                    return
                }
                let localAssetStore = SHLocalAssetStoreController(
                    user: self.user
                )
                localAssetStore.decryptedAsset(
                    encryptedAsset: encryptedAsset,
                    quality: quality,
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
                log.critical("unable to download assets \(globalIdentifier) version \(quality.rawValue) from server: \(err)")
                completionHandler(.failure(err))
            }
            let end = CFAbsoluteTimeGetCurrent()
            log.debug("[PERF] \(CFAbsoluteTime(end - start)) for version \(quality.rawValue)")
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
                } catch {
                    log.warning("[downloadManager] Attempt to remove asset \(globalIdentifier) the knowledgeGraph because it's blacklisted FAILED. \(error.localizedDescription)")
                }
                
                completionHandler(.failure(SHAssetDownloadError.assetIsBlacklisted(globalIdentifier)))
                return
            }
            
            // MARK: Get Low Res asset
            
            self.fetchAsset(
                withGlobalIdentifier: globalIdentifier,
                quality: .lowResolution,
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
