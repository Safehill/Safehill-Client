import Foundation

/// The interface representing a locally encrypted version of an asset
public protocol SHEncryptedAssetVersion {
    var quality: SHAssetQuality { get }
    var encryptedData: Data { get }
    var encryptedSecret: Data { get }
    var publicKeyData: Data { get }
    var publicSignatureData: Data { get }
}
