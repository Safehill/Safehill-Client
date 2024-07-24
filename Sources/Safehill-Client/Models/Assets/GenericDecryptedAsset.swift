import Foundation

public struct SHGenericDecryptedAsset : SHDecryptedAsset {
    public let globalIdentifier: String
    public var localIdentifier: String?
    public var decryptedVersions: [SHAssetQuality: Data]
    public let creationDate: Date?
}
