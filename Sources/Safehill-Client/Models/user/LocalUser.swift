import Foundation
import Safehill_Crypto


public enum SHLocalUserError: Error, LocalizedError {
    case invalidKeychainEntry
    case missingProtocolSalt
    case notAuthenticated
}


/// Manage encryption key pairs in the keychain, credentials (like SSO), and holds user details for the local user (name).
/// It also provides utilities to encrypt and decrypt assets using the encryption keys.
public struct SHLocalUser: SHLocalUserProtocol {
    
    public var shUser: SHLocalCryptoUser
    
    public var identifier: String {
        self.shUser.identifier
    }
    
    public var name: String = "" // Empty means unknown
    
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
    
    private var _ssoIdentifier: String?
    private var _authToken: String?
    private var _encryptionProtocolSalt: Data?
    public var ssoIdentifier: String? { get { _ssoIdentifier } }
    public var authToken: String? { get { _authToken } }
    public var maybeEncryptionProtocolSalt: Data? { get { _encryptionProtocolSalt } }
    
    private let keychainPrefix: String
    
    public static func keysKeychainLabel(withPrefix prefix: String) -> String {
        "\(prefix).keys"
    }
    
    public var keysKeychainLabel: String {
        SHLocalUser.keysKeychainLabel(withPrefix: keychainPrefix)
    }
    public var authKeychainLabel: String {
        "\(keychainPrefix).auth"
    }
    
    public var identityTokenKeychainLabel: String {
        "\(authKeychainLabel).identityToken"
    }
    public var authTokenKeychainLabel: String {
        "\(authKeychainLabel).token"
    }
    public var saltKeychainLabel: String {
        "\(authKeychainLabel).salt"
    }
    
    public var authenticatedUser: SHAuthenticatedLocalUser? {
        SHAuthenticatedLocalUser(localUser: self)
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
        let keysKeychainLabel = SHLocalUser.keysKeychainLabel(withPrefix: keychainPrefix)
        if let shUser = try? SHLocalCryptoUser(usingKeychainEntryWithLabel: keysKeychainLabel) {
            self.shUser = shUser
            
            // SSO identifier (if any)
            do {
                self._ssoIdentifier = try SHKeychain.retrieveValue(from: identityTokenKeychainLabel)
            } catch {
                try? SHKeychain.deleteValue(account: identityTokenKeychainLabel)
                self._ssoIdentifier = nil
            }
            
            // Bearer token
            do {
                self._authToken = try SHKeychain.retrieveValue(from: authTokenKeychainLabel)
            } catch {
                self._authToken = nil
            }
        } else {
            self.shUser = SHLocalCryptoUser()
            self._ssoIdentifier = nil
            self._authToken = nil
            
            try? SHKeychain.deleteValue(account: identityTokenKeychainLabel)
            try? SHKeychain.deleteValue(account: authTokenKeychainLabel)
        }
        
        // Protocol SALT used for encryption
        do {
            if let base64Salt = try SHKeychain.retrieveValue(from: saltKeychainLabel) {
                if let salt = Data(base64Encoded: base64Salt) {
                    self._encryptionProtocolSalt = salt
                } else {
                    throw SHLocalUserError.invalidKeychainEntry
                }
            }
        } catch {
            self._encryptionProtocolSalt = nil
        }
    }
    
    public func saveKeysToKeychain(withLabel label: String, force: Bool = false) throws {
        try self.shUser.saveKeysToKeychain(withLabel: label, force: force)
    }
    
    public func deleteKeysFromKeychain() throws {
        try shUser.deleteKeysInKeychain(withLabel: keysKeychainLabel)
    }
    
    public mutating func updateUserDetails(given user: SHServerUser?) {
        if let user = user {
            self.name = user.name
        } else {
            self.name = ""
        }
    }
    
    public mutating func authenticate(
        _ user: SHServerUser,
        bearerToken: String,
        encryptionProtocolSalt: Data,
        ssoIdentifier: String?
    ) throws -> SHAuthenticatedLocalUser {
        self.updateUserDetails(given: user)
        self._ssoIdentifier = ssoIdentifier
        self._authToken = bearerToken
        self._encryptionProtocolSalt = encryptionProtocolSalt
        
        do {
            if let ssoIdentifier = ssoIdentifier {
                try SHKeychain.storeValue(ssoIdentifier, account: identityTokenKeychainLabel)
            }
            try SHKeychain.storeValue(bearerToken, account: authTokenKeychainLabel)
            try SHKeychain.storeValue(encryptionProtocolSalt.base64EncodedString(), account: saltKeychainLabel)
        } catch {
            // Re-try after deleting items in the keychain
            try? SHKeychain.deleteValue(account: identityTokenKeychainLabel)
            try? SHKeychain.deleteValue(account: authTokenKeychainLabel)
            try? SHKeychain.deleteValue(account: saltKeychainLabel)
            
            if let ssoIdentifier = ssoIdentifier {
                try SHKeychain.storeValue(ssoIdentifier, account: identityTokenKeychainLabel)
            }
            try SHKeychain.storeValue(bearerToken, account: authTokenKeychainLabel)
            try SHKeychain.storeValue(encryptionProtocolSalt.base64EncodedString(), account: saltKeychainLabel)
        }
        
        return SHAuthenticatedLocalUser(localUser: self)!
    }
    
    public mutating func deauthenticate() {
        self._ssoIdentifier = nil
        self._authToken = nil
        
        guard (try? SHKeychain.deleteValue(account: identityTokenKeychainLabel)) != nil,
              (try? SHKeychain.deleteValue(account: authTokenKeychainLabel)) != nil
        else {
            log.fault("auth and identity token could not be removed from the keychain")
            return
        }
    }
}

extension SHLocalUser: Codable {
    
    enum CodingKeys: String, CodingKey {
        case shUser
        case name
        case keychainPrefix
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        
        self.name = try container.decode(String.self, forKey: .name)
        self.shUser = try container.decode(SHLocalCryptoUser.self, forKey: .shUser)
        self.keychainPrefix = try container.decode(String.self, forKey: .keychainPrefix)
        
        self._ssoIdentifier = nil
        self._authToken = nil
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(self.shUser, forKey: .shUser)
        try container.encode(self.name, forKey: .name)
        try container.encode(self.keychainPrefix, forKey: .keychainPrefix)
    }
    
    public func shareableLocalUser() throws -> Data {
        let encoder = JSONEncoder()
        return try encoder.encode(self)
    }
}
