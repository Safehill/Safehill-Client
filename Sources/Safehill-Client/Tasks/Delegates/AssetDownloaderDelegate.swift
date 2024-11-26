import Foundation

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
    
    /// One cycle of downloads has finished from local server
    /// - Parameter localDescriptors: The descriptors for the assets ready to download from local server
    func didCompleteDownloadCycle(
        forLocalDescriptors: [GlobalIdentifier: any SHAssetDescriptor]
    )
    
    /// One cycle of downloads has finished from remote server
    /// - Parameter localDescriptors: The descriptors for the assets ready to download from local server
    func didCompleteDownloadCycle(
        forRemoteDescriptors: [GlobalIdentifier: any SHAssetDescriptor]
    )
    
    /// The download cycle failed
    /// - Parameter with: the error
    func didFailDownloadCycle(with: Error)
}
