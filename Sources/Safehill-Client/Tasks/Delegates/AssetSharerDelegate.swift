import Foundation

public protocol SHAssetSharerDelegate: SHOutboundAssetOperationDelegate {
    func didStartSharing(ofAsset: LocalIdentifier, with: [any SHServerUser], in groupId: String)
    func didCompleteSharing(ofAsset: LocalIdentifier, with: [any SHServerUser], in groupId: String)
    func didFailSharing(ofAsset: LocalIdentifier, with: [any SHServerUser], in groupId: String, error: Error)
}
