import Foundation

/// The interface representing a locally encrypted asset
public protocol SHEncryptedAsset: SHBackedUpAssetIdentifiable {
    var fingerprint: PerceptualHash { get }
    var creationDate: Date? { get }
    var encryptedVersions: [SHAssetQuality: SHEncryptedAssetVersion] { get }
}
