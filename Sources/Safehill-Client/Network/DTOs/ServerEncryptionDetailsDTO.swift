import Foundation

public struct ServerEncryptionKeysDTO : Decodable {
    /// Server's public key for encryption (base64 encoded)
    let publicKey: String
    /// Server's public signature key (base64 encoded)
    let publicSignature: String
    /// Static protocol salt (base64 encoded)
    let encryptionProtocolSalt: String
}
