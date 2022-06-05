import Foundation


public protocol SHAssetDownloaderDelegate {
    func didStartDownload(of items: [String])
    func localIdentifiersInCache() -> [String]
    func globalIdentifiersInCache() -> [String]
    func handleAssetDescriptorResults(for: [SHAssetDescriptor])
    func handleLowResAssetResults(for: [SHDecryptedAsset])
    func handleHiResAssetResults(for: [SHDecryptedAsset])
    func markLocalAssetsAsDownloaded(localToGlobalIdentifiers: [String: String])
    func completionHandler(_: Swift.Result<Void, Error>) -> Void
}

public protocol SHOutboundAssetOperationDelegate {}


public protocol SHAssetEncrypterDelegate: SHOutboundAssetOperationDelegate {
    func didStartEncryption(itemWithLocalIdentifier: String, groupId: String)
    func didCompleteEncryption(itemWithLocalIdentifier: String, globalIdentifier: String, groupId: String)
    func didFailEncryption(itemWithLocalIdentifier: String, groupId: String)
}


public protocol SHAssetUploaderDelegate: SHOutboundAssetOperationDelegate {
    func didStartUpload(itemWithLocalIdentifier: String, globalIdentifier: String, groupId: String)
    func didCompleteUpload(itemWithLocalIdentifier: String, globalIdentifier: String, groupId: String)
    func didFailUpload(itemWithLocalIdentifier: String, globalIdentifier: String, groupId: String)
}


public protocol SHAssetSharerDelegate: SHOutboundAssetOperationDelegate {
    func didStartSharing(itemWithLocalIdentifier: String, groupId: String, newGroupId: String, with users: [SHServerUser])
    func didCompleteSharing(itemWithLocalIdentifier: String, globalIdentifier: String, groupId: String, with users: [SHServerUser])
    func didFailSharing(itemWithLocalIdentifier: String, globalIdentifier: String, groupId: String, with users: [SHServerUser])
}

