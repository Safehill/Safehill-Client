import Foundation

public struct SHGenericShareableEncryptedAsset : SHShareableEncryptedAsset {
    public let globalIdentifier: String
    public let sharedVersions: [SHShareableEncryptedAssetVersion]
    public let groupId: String

    public init(globalIdentifier: String,
                sharedVersions: [SHShareableEncryptedAssetVersion],
                groupId: String) {
        self.globalIdentifier = globalIdentifier
        self.sharedVersions = sharedVersions
        self.groupId = groupId
    }
}
