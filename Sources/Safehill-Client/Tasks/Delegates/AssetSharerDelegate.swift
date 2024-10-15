import Foundation

public protocol SHAssetSharerDelegate: SHOutboundAssetOperationDelegate {
    func didStartSharing(ofAsset: GlobalIdentifier, with: [any SHServerUser], in groupId: String)
    func didCompleteSharing(ofAsset: GlobalIdentifier, with: [any SHServerUser], in groupId: String)
    func didFailSharing(ofAsset: GlobalIdentifier, with: [any SHServerUser], in groupId: String, error: Error)
    
    func didFailInviting(phoneNumbers: [String], to groupId: String, error: Error)
}
