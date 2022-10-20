import Foundation

public protocol SHAssetDownloaderDelegate {
    func localIdentifiersInCache() -> [String]
    func globalIdentifiersInCache() -> [String]
    
    /// The list of asset descriptors fetched from the server, filtering out what's already available locally (based on the 2 methods above)
    /// - Parameter for: the descriptors
    func handleAssetDescriptorResults(for: [SHAssetDescriptor], users: [SHServerUser])
    /// Notifies there are no assets to download at this time
    func noAssetsToDownload() -> Void
    
    /// Notifies about a local asset being backed up on the cloud
    /// - Parameter descriptorsByLocalIdentifier: the list of local asset identifier to server asset descriptor
    func markLocalAssetsAsDownloaded(descriptorsByLocalIdentifier: [String: SHAssetDescriptor])
    
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
    /// - Parameter groupId: the group id of the request it belongs to
    func handleLowResAsset(_: SHDecryptedAsset, groupId: String)
    /// The hi res was downloaded successfullly for some assets
    /// - Parameter for:
    /// - Parameter _: the decrypted hi res asset
    /// - Parameter groupId: the group id of the request it belongs to
    func handleHiResAsset(_: SHDecryptedAsset, groupId: String)
    
    /// One cycle of downloads has finished
    /// - Parameter _:  a callback with an error if items couldn't be dequeued, or the descriptors couldn't be fetched
    func downloadOperationFinished(_: Swift.Result<Void, Error>)
}

public protocol SHOutboundAssetOperationDelegate {}

public protocol SHAssetFetcherDelegate: SHOutboundAssetOperationDelegate {
    func didStartFetching(itemWithLocalIdentifier: String, groupId: String, sharedWith: [SHServerUser])
    func didCompleteFetching(itemWithLocalIdentifier: String, groupId: String, sharedWith: [SHServerUser])
    func didFailFetching(itemWithLocalIdentifier: String, groupId: String, sharedWith: [SHServerUser])
}

public protocol SHAssetEncrypterDelegate: SHOutboundAssetOperationDelegate {
    func didStartEncryption(itemWithLocalIdentifier: String, groupId: String)
    func didCompleteEncryption(itemWithLocalIdentifier: String, globalIdentifier: String, groupId: String)
    func didFailEncryption(itemWithLocalIdentifier: String, groupId: String)
}


public protocol SHAssetUploaderDelegate: SHOutboundAssetOperationDelegate {
    func didStartUpload(itemWithLocalIdentifier: String, globalIdentifier: String, groupId: String)
    func didCompleteUpload(itemWithLocalIdentifier: String, globalIdentifier: String, groupId: String)
    func didFailUpload(itemWithLocalIdentifier: String,
                       globalIdentifier: String,
                       groupId: String,
                       sharedWith users: [SHServerUser],
                       error: Error)
}


public protocol SHAssetSharerDelegate: SHOutboundAssetOperationDelegate {
    func didStartSharing(itemWithLocalIdentifier: String, groupId: String, with users: [SHServerUser])
    func didCompleteSharing(itemWithLocalIdentifier: String, globalIdentifier: String, groupId: String, with users: [SHServerUser])
    func didFailSharing(itemWithLocalIdentifier: String, globalIdentifier: String, groupId: String, with users: [SHServerUser])
}

