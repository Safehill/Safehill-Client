import Foundation

public protocol SHAssetDescriptorsChangeDelegate {
    func assetWasRemoved(globalIdentifier: String)
}

/// Inbound operation delegate.
public protocol SHInboundAssetOperationDelegate {}

public protocol SHAssetSyncingDelegate: SHInboundAssetOperationDelegate {
    func handleAssetDeletion(of assets: [SHRemoteAssetIdentifier])
    func shareHistoryQueueItemsChanged(withIdentifiers identifiers: [String])
    func shareHistoryQueueItemsRemoved(withIdentifiers identifiers: [String])
}

public protocol SHAssetDownloaderDelegate: SHInboundAssetOperationDelegate {
    func localIdentifiersInCache() -> [String]
    func globalIdentifiersInCache() -> [String]
    
    /// The list of asset descriptors fetched from the server, filtering out what's already available locally (based on the 2 methods above)
    /// - Parameter for: the descriptors
    func handleAssetDescriptorResults(for: [any SHAssetDescriptor], users: [SHServerUser])
    /// Notifies there are no assets to download at this time
    func noAssetsToDownload() -> Void
    
    /// Notifies about a local asset being backed up on the cloud
    /// - Parameter descriptorsByLocalIdentifier: the list of local asset identifier to server asset descriptor
    func markLocalAssetsAsUploaded(descriptorsByLocalIdentifier: [String: any SHAssetDescriptor])
    
    /// The download of a set of assets started
    /// - Parameter downloadRequest: the request
    func didStart(globalIdentifier: String, groupId: String)
    /// An attempt to download some assets failed
    /// - Parameters:
    ///   - errorsByAssetIdentifier: the list of asset global identifiers and associated error. When the item in the queue can't be read (hence the list of global identifiers is not available, this parameter is `nil`)
    func didFailDownloadAttempt(errorsByAssetIdentifier: [String: Error]?)
    /// Notifies about an unrecoverable error (usually an asset that couldn't be downloaded after many attempts)
    /// - Parameters:
    ///   - assetIdentifier: the global identifier of the asset
    ///   - groupId: the group id of the request it belongs to
    func unrecoverableDownloadFailure(for assetIdentifier: String, groupId: String)
    /// The low res was downloaded successfullly for some assets
    /// - Parameter _: the decrypted low res asset
    func handleLowResAsset(_: any SHDecryptedAsset)
    /// The hi res was downloaded successfullly for some assets
    /// - Parameters:
    /// - Parameter _: the decrypted hi res asset
    func handleHiResAsset(_: any SHDecryptedAsset)
    /// The download for this asset completed
    /// - Parameter assetIdentifier: the global identifier for the asset
    /// - Parameter groupId: the group id of the request it belongs to
    func completed(_ assetIdentifier: String, groupId: String)
    
    /// One cycle of downloads has finished
    /// - Parameter _:  a callback with an error if items couldn't be dequeued, or the descriptors couldn't be fetched
    func downloadOperationFinished(_: Swift.Result<Void, Error>)
}

public protocol SHOutboundAssetOperationDelegate {}

public protocol SHAssetFetcherDelegate: SHOutboundAssetOperationDelegate {
    func didStartFetching(queueItemIdentifier: String)
    func didCompleteFetching(queueItemIdentifier: String)
    func didFailFetching(queueItemIdentifier: String)
}

public protocol SHAssetEncrypterDelegate: SHOutboundAssetOperationDelegate {
    func didStartEncryption(queueItemIdentifier: String)
    func didCompleteEncryption(queueItemIdentifier: String)
    func didFailEncryption(queueItemIdentifier: String)
}


public protocol SHAssetUploaderDelegate: SHOutboundAssetOperationDelegate {
    func didStartUpload(queueItemIdentifier: String)
    func didCompleteUpload(queueItemIdentifier: String)
    func didFailUpload(queueItemIdentifier: String, error: Error)
}


public protocol SHAssetSharerDelegate: SHOutboundAssetOperationDelegate {
    func didStartSharing(queueItemIdentifier: String)
    func didCompleteSharing(queueItemIdentifier: String)
    func didFailSharing(queueItemIdentifier: String)
}

