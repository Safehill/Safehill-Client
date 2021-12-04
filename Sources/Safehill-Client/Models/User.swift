//
//  User.swift
//  
//
//  Created by Gennaro Frazzingaro on 9/22/21.
//

import Foundation
import Safehill_Crypto

public protocol SHServerUser : SHCryptoUser {
    var identifier: String { get }
    var name: String? { get }
    var email: String? { get }
}

public struct SHRemoteUser : SHServerUser, Codable {
    public let identifier: String
    public let name: String?
    public let email: String?
    public let publicKeyData: Data
    public let publicSignatureData: Data
    
    init(identifier: String,
         name: String,
         email: String,
         publicKeyData: Data,
         publicSignatureData: Data) throws {
        self.identifier = identifier
        self.publicKeyData = publicKeyData
        self.publicSignatureData = publicSignatureData
        self.name = name
        self.email = email
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        identifier = try container.decode(String.self, forKey: .identifier)
        name = try container.decode(String.self, forKey: .name)
        email = try container.decode(String.self, forKey: .email)
        let publicKeyDataBase64 = try container.decode(String.self, forKey: .publicKeyData)
        publicKeyData = Data(base64Encoded: publicKeyDataBase64)!
        let publicSignatureDataBase64 = try container.decode(String.self, forKey: .publicSignatureData)
        publicSignatureData = Data(base64Encoded: publicSignatureDataBase64)!
    }
}

public struct SHLocalUser: SHServerUser {
    public var identifier: String {
        SHHash.stringDigest(for: publicSignatureData)
    }
    
    var shUser: SHLocalCryptoUser
    public let publicKeyData: Data
    public let publicSignatureData: Data
        
    public var name: String?
    public var email: String?
    
    private var _ssoIdentifier: String?
    private var _authToken: String?
    
    public var ssoIdentifier: String? { get { _ssoIdentifier } }
    public var authToken: String? { get { _authToken } }
    
    private let keychainPrefix: String
    
    private static func keysKeychainLabel(withPrefix prefix: String) -> String {
        "\(prefix).keys"
    }
    public var keysKeychainLabel: String {
        SHLocalUser.keysKeychainLabel(withPrefix: keychainPrefix)
    }
    public var authKeychainLabel: String {
        "\(keychainPrefix).auth"
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
        if let shUser = try? SHLocalCryptoUser(usingKeychainEntryWithLabel: SHLocalUser.keysKeychainLabel(withPrefix: keychainPrefix)) {
            self.shUser = shUser
        } else {
            self.shUser = SHLocalCryptoUser()
            try? self.shUser.saveKeysToKeychain(withLabel: "\(keychainPrefix).keys")
        }
        
        self.publicKeyData = shUser.publicKeyData
        self.publicSignatureData = shUser.publicSignatureData
        
        // SSO identifier (if any)
        do {
            self._ssoIdentifier = try SHKeychain.retrieveValue(from: "\(authKeychainLabel).identityToken")
        } catch {
            try? SHKeychain.deleteValue(account: "\(authKeychainLabel).identityToken")
            // TODO: Do not swallow this log
            self._ssoIdentifier = nil
        }
        
        // Bearer token
        do {
            self._authToken = try SHKeychain.retrieveValue(from: "\(authKeychainLabel).token")
        } catch {
            try? SHKeychain.deleteValue(account: "\(authKeychainLabel).token")
            // TODO: Do not swallow this log
            self._authToken = nil
        }
    }
    
    public mutating func authenticate(_ user: SHServerUser, bearerToken: String, ssoIdentifier: String?) throws {
        self.name = user.name
        self.email = user.email
        self._ssoIdentifier = ssoIdentifier
        self._authToken = bearerToken
        
        do {
            if let ssoIdentifier = ssoIdentifier {
                try SHKeychain.storeValue(ssoIdentifier, account: self.authKeychainLabel + ".identityToken")
            }
            try SHKeychain.storeValue(bearerToken, account: self.authKeychainLabel + ".token")
        } catch {
            try SHKeychain.deleteValue(account: "\(authKeychainLabel).identityToken")
            try SHKeychain.deleteValue(account: "\(authKeychainLabel).token")
            
            throw error
        }
    }
    
    public mutating func deauthenticate() {
        self._ssoIdentifier = nil
        self._authToken = nil
        self.name = nil
        self.email = nil
    }
    
    public func shareable(data: Data, with user: SHCryptoUser) throws -> SHShareablePayload {
        try SHUserContext(user: self.shUser).shareable(data: data, with: user)
    }
    
    public func decrypted(data: Data, encryptedSecret: SHShareablePayload, receivedFrom user: SHCryptoUser) throws -> Data {
        try SHUserContext(user: self.shUser).decrypt(data, usingEncryptedSecret: encryptedSecret, receivedFrom: user)
    }
    
    public mutating func regenerateKeys() throws {
        // TODO: Should remove old?
        self.shUser = SHLocalCryptoUser()
        do {
            try self.shUser.saveKeysToKeychain(withLabel: keysKeychainLabel)
        } catch SHKeychain.Error.unexpectedStatus(let status) {
            print(status)
            if status == -25299 {
                // keychain item exists
                try self.shUser.updateKeysInKeychain(withLabel: keysKeychainLabel)
            } else {
                throw SHKeychain.Error.unexpectedStatus(status)
            }
        }
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
    }
    
    static func deleteValue(account: String) throws {
        let query = [kSecClass: kSecClassGenericPassword,
                     kSecAttrAccount: account,
                     kSecUseDataProtectionKeychain: true,
                     kSecReturnData: true] as [String: Any]
        
        let status = SecItemDelete(query as CFDictionary)
        guard status == errSecSuccess || status == errSecItemNotFound else { throw SHKeychain.Error.unexpectedStatus(status) }
    }
}
