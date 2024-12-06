import Foundation

public struct SHGenericDecryptedAsset : SHDecryptedAsset {
    public let globalIdentifier: String
    public var localIdentifier: String?
    public var decryptedVersions: [SHAssetQuality: Data]
    public let creationDate: Date?
    
    public init(globalIdentifier: String, localIdentifier: String? = nil, decryptedVersions: [SHAssetQuality : Data], creationDate: Date?) {
        self.globalIdentifier = globalIdentifier
        self.localIdentifier = localIdentifier
        self.decryptedVersions = decryptedVersions
        self.creationDate = creationDate
    }
}
