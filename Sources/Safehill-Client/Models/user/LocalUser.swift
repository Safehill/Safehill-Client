import Foundation
import Safehill_Crypto


public enum SHLocalUserError: Error, LocalizedError {
    case invalidKeychainEntry
    case failedToRemoveKeychainEntry
    case missingProtocolSalt
    case notAuthenticated
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
        // TODO: Should we create a new one every time?
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
    
    /// Initializes a SHLocalUser and the corresponding keychain element.
    /// Creates a key pair if none exists in the keychain with label `keysKeychainLabel`,
    /// and pulls the authToken from the keychain with label `authKeychainLabel` if a value exists
    /// - Parameter keychainPrefix:the keychain prefix
    public init(keychainPrefix: String) {
        // Asymmetric keys
        self.keychainPrefix = keychainPrefix
        
        let keysKeychainLabel = SHLocalUser.keysKeychainLabel(keychainPrefix: keychainPrefix)
        let authTokenLabel = SHLocalUser.authTokenKeychainLabel(keychainPrefix: keychainPrefix)
        let identityTokenLabel = SHLocalUser.identityTokenKeychainLabel(keychainPrefix: keychainPrefix)
        let saltKeychainLabel = SHLocalUser.saltKeychainLabel(keychainPrefix: keychainPrefix)
        
        if let shUser = try? SHLocalCryptoUser(usingKeychainEntryWithLabel: keysKeychainLabel) {
            self.shUser = shUser
            
            // Bearer token
            do {
                self.authToken = try SHKeychain.retrieveValue(from: authTokenLabel)
            } catch {
                self.authToken = nil
            }
        } else {
            self.shUser = SHLocalCryptoUser()
            self.authToken = nil
            
            
            try? SHKeychain.deleteValue(account: identityTokenLabel)
            try? SHKeychain.deleteValue(account: authTokenLabel)
        }
        
        // Protocol SALT used for encryption
        do {
            if let base64Salt = try SHKeychain.retrieveValue(from: saltKeychainLabel) {
                if let salt = Data(base64Encoded: base64Salt) {
                    self.maybeEncryptionProtocolSalt = salt
                } else {
                    throw SHLocalUserError.invalidKeychainEntry
                }
            } else {
                self.maybeEncryptionProtocolSalt = nil
            }
        } catch {
            self.maybeEncryptionProtocolSalt = nil
        }
    }
    
    public func saveKeysToKeychain(withLabel label: String, force: Bool = false) throws {
        try self.shUser.saveKeysToKeychain(withLabel: label, force: force)
    }
    
    public func authenticate(
        _ user: SHServerUser,
        bearerToken: String,
        encryptionProtocolSalt: Data,
        ssoIdentifier: String?
    ) throws -> SHAuthenticatedLocalUser {
        
        let saltKeychainLabel = SHLocalUser.saltKeychainLabel(keychainPrefix: keychainPrefix)
        let authTokenLabel = SHLocalUser.authTokenKeychainLabel(keychainPrefix: keychainPrefix)
        let identityTokenLabel = SHLocalUser.identityTokenKeychainLabel(keychainPrefix: keychainPrefix)
        
        do {
            if let ssoIdentifier = ssoIdentifier {
                try SHKeychain.storeValue(ssoIdentifier, account: identityTokenLabel)
            }
            try SHKeychain.storeValue(bearerToken, account: authTokenLabel)
            try SHKeychain.storeValue(encryptionProtocolSalt.base64EncodedString(), account: saltKeychainLabel)
        } catch {
            // Re-try after deleting items in the keychain
            try? SHKeychain.deleteValue(account: identityTokenLabel)
            try? SHKeychain.deleteValue(account: authTokenLabel)
            try? SHKeychain.deleteValue(account: saltKeychainLabel)
            
            if let ssoIdentifier = ssoIdentifier {
                try SHKeychain.storeValue(ssoIdentifier, account: identityTokenLabel)
            }
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
        return try encoder.encode(self)
    }
}
