import Foundation
import KnowledgeBase
import Safehill_Crypto

public struct SHAssetsDownloadManager {
    let user: SHAuthenticatedLocalUser
    
    public init(user: SHAuthenticatedLocalUser) {
        self.user = user
    }
    
    private func fetchAsset(
        withGlobalIdentifier globalIdentifier: GlobalIdentifier,
        descriptor: any SHAssetDescriptor
    ) async throws -> any SHDecryptedAsset
    {
        let start = CFAbsoluteTimeGetCurrent()
        
        /// Get only the low resolution. Mid or hi resolution are on-request (when an image is shown larger on screen)
        let versions: [SHAssetQuality] = [.lowResolution]
        
        return try await withUnsafeThrowingContinuation { continuation in
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
                        continuation.resume(throwing: SHHTTPError.ClientError.notFound)
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
                            continuation.resume(throwing: error)
                        case .success(let decryptedAsset):
                            continuation.resume(returning: decryptedAsset)
                        }
                    }
                case .failure(let err):
                    log.critical("[downloadManager] unable to download assets \(globalIdentifier) version \(versions) from server: \(err.localizedDescription)")
                    continuation.resume(throwing: err)
                }
                let end = CFAbsoluteTimeGetCurrent()
                log.debug("[PERF] \(CFAbsoluteTime(end - start)) for versions \(versions)")
            }
        }
    }
    
    /// Downloads the asset for the given descriptor, decrypts it, and returns the decrypted version, or an error.
    /// - Parameters:
    ///   - descriptor: the descriptor for the assets to download
    ///   - completionHandler: the callback
    func downloadAsset(
        for descriptor: any SHAssetDescriptor
    ) async throws -> any SHDecryptedAsset
    {
        log.info("[downloadManager] downloading assets with identifier \(descriptor.globalIdentifier)")
        
        let globalIdentifier = descriptor.globalIdentifier
        
        // MARK: Get Low Res asset
        
        return try await self.fetchAsset(
            withGlobalIdentifier: globalIdentifier,
            descriptor: descriptor
        )
    }
}
