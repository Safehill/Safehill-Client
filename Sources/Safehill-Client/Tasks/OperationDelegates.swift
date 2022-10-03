import Foundation


public protocol SHAssetDownloaderDelegate {
    func didStartDownload(of assetIdentifiers: [String])
    func didFailDownload(of assetIdentifiers: [String], errorsByAssetIdentifier: [String: Error]?)
    func didCompleteDownload(of assetIdentifiers: [String])
    func localIdentifiersInCache() -> [String]
    func globalIdentifiersInCache() -> [String]
    func handleAssetDescriptorResults(for: [SHAssetDescriptor])
    func handleLowResAssetResults(for: [SHDecryptedAsset])
    func handleHiResAssetResults(for: [SHDecryptedAsset])
    func markLocalAssetsAsDownloaded(descriptorsByLocalIdentifier: [String: SHAssetDescriptor])
    func completionHandler(_: Swift.Result<Void, Error>) -> Void
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
    func didFailUpload(itemWithLocalIdentifier: String, globalIdentifier: String, groupId: String, sharedWith users: [SHServerUser])
}


public protocol SHAssetSharerDelegate: SHOutboundAssetOperationDelegate {
    func didStartSharing(itemWithLocalIdentifier: String, groupId: String, with users: [SHServerUser])
    func didCompleteSharing(itemWithLocalIdentifier: String, globalIdentifier: String, groupId: String, with users: [SHServerUser])
    func didFailSharing(itemWithLocalIdentifier: String, globalIdentifier: String, groupId: String, with users: [SHServerUser])
}

