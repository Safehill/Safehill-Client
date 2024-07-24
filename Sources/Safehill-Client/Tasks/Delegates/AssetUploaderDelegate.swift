import Foundation

public protocol SHAssetUploaderDelegate: SHOutboundAssetOperationDelegate {
    func didStartUpload(ofAsset: LocalIdentifier, in groupId: String)
    func didCompleteUpload(ofAsset: LocalIdentifier, in groupId: String)
    func didFailUpload(ofAsset: LocalIdentifier, in groupId: String, error: Error)
}
