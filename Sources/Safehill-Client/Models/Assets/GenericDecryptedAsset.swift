import Foundation

public struct SHGenericDecryptedAsset : SHDecryptedAsset {
    public let globalIdentifier: GlobalIdentifier
    public var localIdentifier: LocalIdentifier?
    public var perceptualHash: PerceptualHash
    public var decryptedVersions: [SHAssetQuality: Data]
    public let creationDate: Date?
    
    public init(
        globalIdentifier: GlobalIdentifier,
        localIdentifier: LocalIdentifier?,
        perceptualHash: PerceptualHash,
        decryptedVersions: [SHAssetQuality : Data],
        creationDate: Date?
    ) {
        self.globalIdentifier = globalIdentifier
        self.localIdentifier = localIdentifier
        self.perceptualHash = perceptualHash
        self.decryptedVersions = decryptedVersions
        self.creationDate = creationDate
    }
}
