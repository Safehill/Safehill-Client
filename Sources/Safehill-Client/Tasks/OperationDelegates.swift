import Foundation
import Photos

public protocol SHAssetDescriptorsChangeDelegate {
    func assetWasRemoved(globalIdentifier: String)
}

/// Inbound operation delegate.
public protocol SHInboundAssetOperationDelegate {}

public protocol SHAssetSyncingDelegate: SHInboundAssetOperationDelegate {
    func assetsWereDeleted(_ assets: [SHRemoteAssetIdentifier])
    func shareHistoryQueueItemsChanged(withIdentifiers identifiers: [String])
    func shareHistoryQueueItemsRemoved(withIdentifiers identifiers: [String])
    func usersAreConnectedAndVerified(_: [SHServerUser])
    func assetIdsAreSharedWithUser(_: [GlobalIdentifier])
}

public protocol SHAssetDownloaderDelegate: SHInboundAssetOperationDelegate {
    /// The list of asset descriptors fetched from the server, filtering out what's already available locally (based on the 2 methods above)
    /// - Parameter for: the descriptors
    /// - Parameter users: the `SHServerUser` objects for user ids mentioned in the descriptors
    /// - Parameter completionHandler: called when handling is complete
    func handleAssetDescriptorResults(for: [any SHAssetDescriptor],
                                      users: [SHServerUser],
                                      completionHandler: (() -> Void)?)
    /// Notifies there are no assets to download at this time
    func noAssetsToDownload() -> Void
    /// Notifies there are assets to download from unknown users
    func handleDownloadAuthorization(ofDescriptors: [any SHAssetDescriptor], users: [SHServerUser]) -> Void
    
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
    /// Notifies about an asset in the local library that is linked to one on the server (backed up)
    /// - Parameters:
    ///   - globalIdentifier: the global identifier of the asset
    ///   - phAsset: the local asset
    func handleAssetInLocalLibraryAndServer(globalIdentifier: GlobalIdentifier, phAsset: PHAsset)
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
    /// The download of this asset failed
    /// - Parameters:
    /// - Parameter assetIdentifier: the global identifier for the asset
    /// - Parameter groupId: the group id of the request it belongs to
    func failed(_ assetIdentifier: String, groupId: String)
    
    /// One cycle of downloads has finished
    /// - Parameter _:  a callback with an error if items couldn't be dequeued, or the descriptors couldn't be fetched
    func downloadOperationFinished(_: Swift.Result<Void, Error>)
}

public protocol SHOutboundAssetOperationDelegate {}

public protocol SHAssetFetcherDelegate: SHOutboundAssetOperationDelegate {
    func didStartFetchingForUpload(queueItemIdentifier: String)
    func didStartFetchingForSharing(queueItemIdentifier: String)
    func didCompleteFetchingForUpload(queueItemIdentifier: String)
    func didCompleteFetchingForSharing(queueItemIdentifier: String)
    func didFailFetchingForUpload(queueItemIdentifier: String)
    func didFailFetchingForSharing(queueItemIdentifier: String)
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

