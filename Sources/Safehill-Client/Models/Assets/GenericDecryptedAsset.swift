import Foundation

public struct SHGenericDecryptedAsset : SHDecryptedAsset {
    public let globalIdentifier: GlobalIdentifier
    public var localIdentifier: LocalIdentifier?
    public var fingerprint: String
    public var decryptedVersions: [SHAssetQuality: Data]
    public let creationDate: Date?
    
    public init(
        globalIdentifier: GlobalIdentifier,
        localIdentifier: LocalIdentifier?,
        fingerprint: String,
        decryptedVersions: [SHAssetQuality : Data],
        creationDate: Date?
    ) {
        self.globalIdentifier = globalIdentifier
        self.localIdentifier = localIdentifier
        self.fingerprint = fingerprint
        self.decryptedVersions = decryptedVersions
        self.creationDate = creationDate
    }
}
