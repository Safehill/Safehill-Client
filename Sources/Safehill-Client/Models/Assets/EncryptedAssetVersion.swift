import Foundation

/// The interface representing a locally encrypted version of an asset
public protocol SHEncryptedAssetVersion {
    var quality: SHAssetQuality { get }
    var encryptedData: Data { get }
    var encryptedSecret: Data { get }
    var publicKeyData: Data { get }
    var publicSignatureData: Data { get }
    /// The signature to use for verification during decryption.
    /// This is either the server's signature (for server-mediated re-encryption)
    /// or the sender's signature (for regular shares).
    var verificationSignatureData: Data { get }
}
