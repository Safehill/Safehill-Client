import Foundation

public protocol SHAssetEncrypterDelegate: SHOutboundAssetOperationDelegate {
    func didStartEncryption(ofAsset: LocalIdentifier, in groupId: String)
    func didCompleteEncryption(ofAsset: LocalIdentifier, in groupId: String)
    func didFailEncryption(ofAsset: LocalIdentifier, in groupId: String, error: Error)
}
