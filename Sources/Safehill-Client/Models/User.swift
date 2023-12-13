import Foundation
import Safehill_Crypto

public enum SHLocalUserError: Error, LocalizedError {
    case invalidKeychainEntry
    case missingProtocolSalt
}

public protocol SHServerUser : SHCryptoUser {
    var identifier: String { get }
    var name: String { get }
}

public struct SHRemoteUser : SHServerUser, Codable {
    public let identifier: String
    public let name: String
    public let publicKeyData: Data
    public let publicSignatureData: Data
    
    enum CodingKeys: String, CodingKey {
        case identifier
        case name
        case publicKeyData = "publicKey"
        case publicSignatureData = "publicSignature"
    }
    
    init(identifier: String,
         name: String,
         publicKeyData: Data,
         publicSignatureData: Data) {
        self.identifier = identifier
        self.publicKeyData = publicKeyData
        self.publicSignatureData = publicSignatureData
        self.name = name
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        identifier = try container.decode(String.self, forKey: .identifier)
        name = try container.decode(String.self, forKey: .name)
        let publicKeyDataBase64 = try container.decode(String.self, forKey: .publicKeyData)
        publicKeyData = Data(base64Encoded: publicKeyDataBase64)!
        let publicSignatureDataBase64 = try container.decode(String.self, forKey: .publicSignatureData)
        publicSignatureData = Data(base64Encoded: publicSignatureDataBase64)!
    }
}

/// Manage encryption key pairs in the keychain, credentials (like SSO), and holds user details for the local user (name).
/// It also provides utilities to encrypt and decrypt assets using the encryption keys.
public struct SHLocalUser: SHServerUser {
    
    public var identifier: String {
        self.shUser.identifier
    }
    
    var shUser: SHLocalCryptoUser
    
    public var publicKeyData: Data {
        self.shUser.publicKeyData
    }
    public var publicSignatureData: Data {
        self.shUser.publicSignatureData
    }
        
    public var name: String = "" // Empty means unknown
    
    private var _ssoIdentifier: String?
    private var _authToken: String?
    private var _encryptionProtocolSalt: Data?
    
    public var ssoIdentifier: String? { get { _ssoIdentifier } }
    public var authToken: String? { get { _authToken } }
    public var encryptionProtocolSalt: Data? { get { _encryptionProtocolSalt } }
    
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
    
    static func == (lhs: SHLocalUser, rhs: SHLocalUser) -> Bool {
        return lhs.publicKeyData == rhs.publicKeyData
        && lhs.publicSignatureData == rhs.publicSignatureData
    }
    
    public init(cryptoUser: SHLocalCryptoUser) {
        self.keychainPrefix = ""
        self.shUser = cryptoUser
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
        } else {
            self.shUser = SHLocalCryptoUser()
            try? self.saveKeysToKeychain(withLabel: keysKeychainLabel)
        }
        
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
            self._authToken = nil
        }
    }
    
    public func saveKeysToKeychain(withLabel label: String, force: Bool = false) throws {
        try self.shUser.saveKeysToKeychain(withLabel: label, force: force)
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
    ) throws {
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
    
    public func createShareablePayload(from data: Data, toShareWith user: SHCryptoUser) throws -> SHShareablePayload {
        guard let salt = self.encryptionProtocolSalt else {
            throw SHLocalUserError.missingProtocolSalt
        }
        return try SHUserContext(user: self.shUser)
            .shareable(data: data, protocolSalt: salt, with: user)
    }
    
    public func decrypt(data: Data,
                        encryptedSecret: SHShareablePayload,
                        receivedFrom user: SHCryptoUser) throws -> Data {
        guard let salt = self.encryptionProtocolSalt else {
            throw SHLocalUserError.missingProtocolSalt
        }
        return try SHUserContext(user: self.shUser)
            .decrypt(data, usingEncryptedSecret: encryptedSecret, protocolSalt: salt, receivedFrom: user)
    }
    
    public mutating func regenerateKeys() throws {
        self.deauthenticate()
        self.shUser = SHLocalCryptoUser()
        try self.saveKeysToKeychain(withLabel: keysKeychainLabel, force: true)
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

// MARK: Initialize SHUserContext from SHLocalUser

public extension SHUserContext {
    init(localUser: SHLocalUser) {
        self.init(user: localUser.shUser)
    }
}

// MARK: Store auth token in the keychaing

extension SHKeychain {
    static func retrieveValue(from account: String) throws -> String? {
        // Seek a generic password with the given account.
        let query = [kSecClass: kSecClassGenericPassword,
                     kSecAttrAccount: account,
                     kSecUseDataProtectionKeychain: true,
                     kSecReturnData: true] as [String: Any]

        // Find and cast the result as data.
        var item: CFTypeRef?
        switch SecItemCopyMatching(query as CFDictionary, &item) {
        case errSecSuccess:
            guard let data = item as? Data else { return nil }
            return String(data: data, encoding: .utf8)
        case errSecItemNotFound: return nil
        case let status: throw SHKeychain.Error.unexpectedStatus(status)
        }
    }
    
    static func storeValue(_ token: String, account: String) throws {
        guard let tokenData = token.data(using: .utf8) else {
            throw SHKeychain.Error.generic("Unable to convert string to data.")
        }
        
        // Treat the key data as a generic password.
        let query = [kSecClass: kSecClassGenericPassword,
                     kSecAttrAccount: account,
                     kSecAttrAccessible: kSecAttrAccessibleWhenUnlocked,
                     kSecUseDataProtectionKeychain: true,
                     kSecValueData: tokenData] as [String: Any]

        // Add the key data.
        let status = SecItemAdd(query as CFDictionary, nil)
        guard status == errSecSuccess else {
            throw SHKeychain.Error.unexpectedStatus(status)
        }
        
        log.debug("Successfully saved value \(token, privacy: .private) in account \(account)")
    }
    
    static func deleteValue(account: String) throws {
        let query = [kSecClass: kSecClassGenericPassword,
                     kSecAttrAccount: account] as [String: Any]
        
        let status = SecItemDelete(query as CFDictionary)
        guard status == errSecSuccess || status == errSecItemNotFound else { throw SHKeychain.Error.unexpectedStatus(status) }
        
#if DEBUG
        log.info("Successfully deleted account \(account)")
#endif
    }
}
