import Foundation

public protocol SHAssetSharerDelegate: SHOutboundAssetOperationDelegate {
    func didStartSharing(ofAsset: LocalIdentifier, in groupId: String)
    func didCompleteSharing(ofAsset: LocalIdentifier, in groupId: String)
    func didFailSharing(ofAsset: LocalIdentifier, in groupId: String, error: Error)
}
