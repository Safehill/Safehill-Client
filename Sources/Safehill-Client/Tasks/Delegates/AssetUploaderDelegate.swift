import Foundation

public protocol SHAssetUploaderDelegate: SHOutboundAssetOperationDelegate {
    func didStartUpload(
        ofAsset: SHBackedUpAssetIdentifier,
        in groupId: String
    )
    func didCompleteUpload(
        ofAsset: SHBackedUpAssetIdentifier,
        in groupId: String
    )
    func didFailUpload(
        ofAsset: SHBackedUpAssetIdentifier,
        in groupId: String,
        error: Error
    )
}
