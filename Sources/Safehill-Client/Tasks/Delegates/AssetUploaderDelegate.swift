import Foundation

public protocol SHAssetUploaderDelegate: SHOutboundAssetOperationDelegate {
    func didStartUpload(ofAsset: GlobalIdentifier, in groupId: String)
    func didCompleteUpload(ofAsset: GlobalIdentifier, in groupId: String)
    func didFailUpload(ofAsset: GlobalIdentifier, in groupId: String, error: Error)
}
