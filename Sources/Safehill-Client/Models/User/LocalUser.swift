import Foundation
import Safehill_Crypto


public enum SHLocalUserError: Error, LocalizedError {
    case invalidKeychainEntry
    case failedToRemoveKeychainEntry
    case missingProtocolSalt
    case notAuthenticated
    
    public var errorDescription: String? {
        switch self {
        case .invalidKeychainEntry:
            "Invalid entry in the keychain"
        case .failedToRemoveKeychainEntry:
            "Failed to remove keychain entry"
        case .missingProtocolSalt:
            "Encryption salt missing"
        case .notAuthenticated:
            "Not authenticated"
        }
    }
}


/// Manage encryption key pairs in the keychain, credentials (like SSO), and holds user details for the local user (name).
/// It also provides utilities to encrypt and decrypt assets using the encryption keys.
public struct SHLocalUser: SHLocalUserProtocol {
    
    public var name: String { "" }
    
    public let shUser: SHLocalCryptoUser
    
    public var identifier: String {
        self.shUser.identifier
    }
    
    public var publicKeyData: Data {
        self.shUser.publicKeyData
    }
    public var publicSignatureData: Data {
        self.shUser.publicSignatureData
    }
    
    public var serverProxy: SHServerProxy {
        SHServerProxy(user: self)
    }
    
    public let authToken: String?
    public let maybeEncryptionProtocolSalt: Data?
    
    public let keychainPrefix: String

    public static func saltKeychainLabel(keychainPrefix: String) -> String {
        "\(SHLocalUser.authKeychainLabel(keychainPrefix: keychainPrefix)).salt"
    }
    
    static func == (lhs: SHLocalUser, rhs: SHLocalUser) -> Bool {
        return lhs.publicKeyData == rhs.publicKeyData
        && lhs.publicSignatureData == rhs.publicSignatureData
    }
    
    public static func create(keychainPrefix: String) -> SHLocalUser {
        return SHLocalUser(
            shUser: SHLocalCryptoUser(),
            authToken: nil,
            maybeEncryptionProtocolSalt: nil,
            keychainPrefix: keychainPrefix
        )
    }
    
    /// Initializes a SHLocalUser and the corresponding keychain element.
    /// Creates a key pair if none exists in the keychain with label `keysKeychainLabel`,
    /// and pulls the authToken from the keychain with label `authKeychainLabel` if a value exists
    ///
    /// - Parameter keychainPrefix: the keychain prefix
    /// - Parameter synchronizable: see `kSecAttrSynchronizable`. Whether the item to retrieve in the keychain
    /// was stored to be synchronized to other devices through iCloud
    ///
    public static func restore(keychainPrefix: String, synchronizable: Bool) throws -> SHLocalUser {
        let keysKeychainLabel = SHLocalUser.keysKeychainLabel(keychainPrefix: keychainPrefix)
        let authTokenLabel = SHLocalUser.authTokenKeychainLabel(keychainPrefix: keychainPrefix)
        let saltKeychainLabel = SHLocalUser.saltKeychainLabel(keychainPrefix: keychainPrefix)
        
        // Bearer token
        let authToken: String?
        if let token = try? SHKeychain.retrieveValue(from: authTokenLabel) {
            authToken = token
        } else {
            authToken = nil
        }
        
        // Protocol SALT used for encryption
        let salt: Data?
        if let base64Salt = try? SHKeychain.retrieveValue(from: saltKeychainLabel),
           let s = Data(base64Encoded: base64Salt) {
            salt = s
        } else {
            salt = nil
        }
        
        return SHLocalUser(
            shUser: try SHLocalCryptoUser(
                usingKeychainEntryWithLabel: keysKeychainLabel,
                synchronizable: synchronizable
            ),
            authToken: authToken,
            maybeEncryptionProtocolSalt: salt,
            keychainPrefix: keychainPrefix
        )
    }
    
    fileprivate init(
        shUser: SHLocalCryptoUser,
        authToken: String?,
        maybeEncryptionProtocolSalt: Data?,
        keychainPrefix: String
    ) {
        self.shUser = shUser
        self.authToken = authToken
        self.maybeEncryptionProtocolSalt = maybeEncryptionProtocolSalt
        self.keychainPrefix = keychainPrefix
    }
    
    public func authenticate(
        _ user: SHServerUser,
        bearerToken: String,
        encryptionProtocolSalt: Data
    ) throws -> SHAuthenticatedLocalUser {
        
        let saltKeychainLabel = SHLocalUser.saltKeychainLabel(keychainPrefix: keychainPrefix)
        let authTokenLabel = SHLocalUser.authTokenKeychainLabel(keychainPrefix: keychainPrefix)
        
        do {
            try SHKeychain.storeValue(bearerToken, account: authTokenLabel)
            try SHKeychain.storeValue(encryptionProtocolSalt.base64EncodedString(), account: saltKeychainLabel)
        } catch {
            // Re-try after deleting items in the keychain
            try? Self.deleteAuthToken(keychainPrefix)
            try? Self.deleteProtocolSalt(keychainPrefix)
            
            try SHKeychain.storeValue(bearerToken, account: authTokenLabel)
            try SHKeychain.storeValue(encryptionProtocolSalt.base64EncodedString(), account: saltKeychainLabel)
        }
        
        return SHAuthenticatedLocalUser(
            localUser: self,
            name: user.name,
            encryptionProtocolSalt: encryptionProtocolSalt,
            authToken: bearerToken
        )
    }
}

///
/// `SHAuthenticationLocalUser` is not serializable because it's the authenticated version of a `SHLocalUser`.
/// Serializing an object of type `SHAuthenticationLocalUser` requires a conversion to a `SHLocalUser` first.
/// `authedUser.toLocalUser().shareableLocalUser()`
///

extension SHLocalUser: Codable {
    
    enum CodingKeys: String, CodingKey {
        case shUser
        case keychainPrefix
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        
        self.shUser = try container.decode(SHLocalCryptoUser.self, forKey: .shUser)
        self.keychainPrefix = try container.decode(String.self, forKey: .keychainPrefix)
        self.authToken = nil
        self.maybeEncryptionProtocolSalt = nil
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(self.shUser, forKey: .shUser)
        try container.encode(self.keychainPrefix, forKey: .keychainPrefix)
    }
    
    public func shareableLocalUser() throws -> Data {
        let encoder = JSONEncoder()
        encoder.outputFormatting = .sortedKeys
        return try encoder.encode(self)
    }

}

extension SHAuthenticatedLocalUser {
    
    public func toLocalUser() -> SHLocalUser {
        SHLocalUser.init(
            shUser: self.shUser,
            authToken: self.authToken,
            maybeEncryptionProtocolSalt: self.maybeEncryptionProtocolSalt,
            keychainPrefix: self.keychainPrefix
        )
    }

}
