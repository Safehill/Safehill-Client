import Foundation
import Photos

public protocol SHAssetDownloaderDelegate: SHInboundAssetOperationDelegate {
    
    /// The list of asset descriptors fetched from the server, filtering out what's already available locally (based on the 2 methods above)
    /// - Parameter descriptors: the descriptors fetched from local server
    /// - Parameter users: the `SHServerUser` objects for user ids mentioned in the descriptors
    /// - Parameter completionHandler: called when handling is complete
    func didReceiveLocalAssetDescriptors(_ descriptors: [any SHAssetDescriptor],
                                         referencing users: [UserIdentifier: any SHServerUser])
    
    /// The list of asset descriptors fetched from the server, filtering out what's already available locally (based on the 2 methods above)
    /// - Parameter descriptors: the descriptors fetched from remote server
    /// - Parameter users: the `SHServerUser` objects for user ids mentioned in the descriptors
    /// - Parameter completionHandler: called when handling is complete
    func didReceiveRemoteAssetDescriptors(_ descriptors: [any SHAssetDescriptor],
                                          referencing users: [UserIdentifier: any SHServerUser])
    
    /// Notifies about the start of a network download of a an asset from the CDN
    /// - Parameter downloadRequest: the request
    func didStartDownloadOfAsset(withGlobalIdentifier globalIdentifier: GlobalIdentifier,
                                 descriptor: any SHAssetDescriptor,
                                 in groupIds: [String])
    
    /// Notify about a failed attempt to download some assets
    /// - Parameters:
    ///   - globalIdentifier: the global identifier for the asset
    ///   - groupId: the group id of the request it belongs to
    ///   - error: the error
    func didFailDownloadOfAsset(withGlobalIdentifier: GlobalIdentifier,
                                in groupIds: [String],
                                with error: Error)
    
    /// Notifies about assets in the local library that are linked to one on the server (backed up)
    /// - Parameters:
    ///   - localToGlobal: The global identifier of the remote asset to the corresponding local `PHAsset` from the Apple Photos Library
    func didIdentify(globalToLocalAssets: [GlobalIdentifier: PHAsset])
    
    /// Notifies about the successful download operation for a specific asset.
    /// - Parameter decryptedAsset: the decrypted asset
    /// - Parameter groupId: the group id of the request it belongs to
    func didCompleteDownload(
        of decryptedAsset: any SHDecryptedAsset,
        in groupIds: [String]
    )
    
    /// One cycle of downloads has finished from local server
    /// - Parameter localDescriptors: The descriptors for the assets downloaded from local server
    func didCompleteDownloadCycle(
        localAssetsAndDescriptors: [AssetAndDescriptor]
    )
    
    /// One cycle of downloads has finished from remote server
    /// - Parameter localDescriptors: The descriptors for the assets downloaded from local server
    func didCompleteDownloadCycle(
        remoteAssetsAndDescriptors: [AssetAndDescriptor]
    )
    
    /// The download cycle failed
    /// - Parameter with: the error
    func didFailDownloadCycle(with: Error)
}
