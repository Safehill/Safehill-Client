import Foundation

public protocol SHAssetEncrypterDelegate: SHOutboundAssetOperationDelegate {
    func didStartEncryption(ofAsset: GlobalIdentifier, in groupId: String)
    func didCompleteEncryption(ofAsset: GlobalIdentifier, in groupId: String)
    func didFailEncryption(ofAsset: GlobalIdentifier, in groupId: String, error: Error)
}
