import Foundation

/// The interface representing a locally encrypted asset version ready to be shared, hence holding the secret for the user it's being shared with
public protocol SHShareableEncryptedAssetVersion {
    var quality: SHAssetQuality { get }
    var userPublicIdentifier: String { get }
    var encryptedSecret: Data { get }
    var ephemeralPublicKey: Data { get }
    var publicSignature: Data { get }
}
