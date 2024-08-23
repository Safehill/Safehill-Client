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
    
    /// Notifies about assets in the local library that are linked to one on the server (backed up)
    /// - Parameters:
    ///   - localToGlobal: The global identifier of the remote asset to the corresponding local `PHAsset` from the Apple Photos Library
    func didIdentify(globalToLocalAssets: [GlobalIdentifier: PHAsset])
    
    /// The download cycle failed
    /// - Parameter with: the error
    func didFailDownloadCycle(with: Error)
    
}
