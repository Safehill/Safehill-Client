import Foundation

/// The interface representing an asset after being decrypted
public protocol SHDecryptedAsset: SHBackedUpAssetIdentifiable {
    var localIdentifier: LocalIdentifier? { get set }
    var decryptedVersions: [SHAssetQuality: Data] { get set }
    var creationDate: Date? { get }
}
