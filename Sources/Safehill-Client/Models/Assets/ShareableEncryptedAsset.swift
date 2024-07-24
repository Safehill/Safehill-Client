import Foundation

/// The interface representing a locally encrypted asset ready to be shared
public protocol SHShareableEncryptedAsset {
    var globalIdentifier: GlobalIdentifier { get }
    var sharedVersions: [SHShareableEncryptedAssetVersion] { get }
    var groupId: String { get }
}
