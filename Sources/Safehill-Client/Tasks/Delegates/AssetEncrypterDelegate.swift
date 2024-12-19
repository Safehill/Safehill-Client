import Foundation

public protocol SHAssetEncrypterDelegate: SHOutboundAssetOperationDelegate {
    func didStartEncryption(
        ofAsset: SHBackedUpAssetIdentifier,
        in groupId: String
    )
    func didCompleteEncryption(
        ofAsset: SHBackedUpAssetIdentifier,
        in groupId: String
    )
    func didFailEncryption(
        ofAsset: SHBackedUpAssetIdentifier,
        in groupId: String,
        error: Error
    )
}
