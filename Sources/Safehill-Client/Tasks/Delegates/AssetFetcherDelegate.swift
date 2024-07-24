import Foundation

public protocol SHAssetFetcherDelegate: SHOutboundAssetOperationDelegate {
    func didStartFetchingForUpload(ofAsset: LocalIdentifier, in groupId: String)
    func didStartFetchingForSharing(ofAsset: LocalIdentifier, in groupId: String)
    func didCompleteFetchingForUpload(ofAsset: LocalIdentifier, in groupId: String)
    func didCompleteFetchingForSharing(ofAsset: LocalIdentifier, in groupId: String)
    func didFailFetchingForUpload(ofAsset: LocalIdentifier, in groupId: String, error: Error)
    func didFailFetchingForSharing(ofAsset: LocalIdentifier, in groupId: String, error: Error)
}
