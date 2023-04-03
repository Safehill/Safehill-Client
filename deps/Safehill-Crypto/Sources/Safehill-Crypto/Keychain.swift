//
//  Keychain.swift
//  
//
//  Created by Gennaro Frazzingaro on 8/29/21.
//

import Foundation
#if os(Linux)
@_exported import Crypto
#else
import CryptoKit
#endif

protocol SecKeyConvertible: CustomStringConvertible {
    /// Creates a key from an X9.63 representation.
    init<Bytes>(x963Representation: Bytes) throws where Bytes: ContiguousBytes
    
    /// An X9.63 representation of the key.
    var x963Representation: Data { get }
}

extension SecKeyConvertible {
    public var description: String {
        return String(describing: self)
    }
}

extension P256.Signing.PrivateKey: SecKeyConvertible {}
extension P256.KeyAgreement.PrivateKey: SecKeyConvertible {}
extension P384.Signing.PrivateKey: SecKeyConvertible {}
extension P384.KeyAgreement.PrivateKey: SecKeyConvertible {}
extension P521.Signing.PrivateKey: SecKeyConvertible {}
extension P521.KeyAgreement.PrivateKey: SecKeyConvertible {}


protocol GenericPasswordConvertible: CustomStringConvertible {
    /// Creates a key from a raw representation.
    init<D>(rawRepresentation data: D) throws where D: ContiguousBytes
    
    /// A raw representation of the key.
    var rawRepresentation: Data { get }
}

extension GenericPasswordConvertible {
    public var description: String {
        return String(describing: self)
    }
}

extension Curve25519.KeyAgreement.PrivateKey: GenericPasswordConvertible {}
extension Curve25519.Signing.PrivateKey: GenericPasswordConvertible {}

extension SymmetricKey: GenericPasswordConvertible {
    public init<D>(rawRepresentation data: D) throws where D: ContiguousBytes {
        self.init(data: data)
    }
    
    public var rawRepresentation: Data {
        // Contiguous bytes repackaged as a Data instance.
        return self.withUnsafeBytes { return Data(Array($0)) }
    }
}

#if !os(Linux)
extension SecureEnclave.P256.Signing.PrivateKey: GenericPasswordConvertible {
    init<D>(rawRepresentation data: D) throws where D: ContiguousBytes {
        // Contiguous bytes repackaged as a Data instance.
        let dataRepresentation = data.withUnsafeBytes { return Data(Array($0)) }
        try self.init(dataRepresentation: dataRepresentation)
    }
    
    var rawRepresentation: Data {
        return self.dataRepresentation  // Contiguous bytes repackaged as a Data instance.
    }
}

public struct SHKeychain {
    
    public enum Error: CustomNSError, LocalizedError {
        case generic(String)
        case notFound
        case unexpectedStatus(OSStatus)
    }
    
    static func storeKey<T: SecKeyConvertible>(_ key: T, label: String) throws {
        // Describe the key.
        let attributes = [kSecAttrKeyType: kSecAttrKeyTypeECSECPrimeRandom,
                          kSecAttrKeyClass: kSecAttrKeyClassPrivate] as [String: Any]

        // Get a SecKey representation.
        guard let secKey = SecKeyCreateWithData(key.x963Representation as CFData,
                                                attributes as CFDictionary,
                                                nil)
            else {
                throw SHKeychain.Error.generic("Unable to create SecKey representation.")
        }
        
        // Describe the add operation.
        let query = [kSecClass: kSecClassKey,
                     kSecAttrApplicationLabel: label,
                     kSecAttrAccessible: kSecAttrAccessibleWhenUnlocked,
                     kSecUseDataProtectionKeychain: true,
                     kSecValueRef: secKey] as [String: Any]

        // Add the key to the keychain.
        let status = SecItemAdd(query as CFDictionary, nil)
        guard status == errSecSuccess else {
            throw SHKeychain.Error.unexpectedStatus(status)
        }
    }
    
    static func storeKey<T: GenericPasswordConvertible>(_ key: T, account: String) throws {
        // Treat the key data as a generic password.
        let query = [kSecClass: kSecClassGenericPassword,
                     kSecAttrAccount: account,
                     kSecAttrAccessible: kSecAttrAccessibleWhenUnlocked,
                     kSecUseDataProtectionKeychain: true,
                     kSecValueData: key.rawRepresentation] as [String: Any]

        // Add the key data.
        let status = SecItemAdd(query as CFDictionary, nil)
        guard status == errSecSuccess else {
            throw SHKeychain.Error.unexpectedStatus(status)
        }
    }
    
    static func retrieveKey<T: SecKeyConvertible>(label: String) throws -> T? {
        // Seek an elliptic-curve key with a given label.
        let query = [kSecClass: kSecClassKey,
                     kSecAttrApplicationLabel: label,
                     kSecAttrKeyType: kSecAttrKeyTypeECSECPrimeRandom,
                     kSecUseDataProtectionKeychain: true,
                     kSecReturnRef: true] as [String: Any]

        // Find and cast the result as a SecKey instance.
        var item: CFTypeRef?
        var secKey: SecKey
        switch SecItemCopyMatching(query as CFDictionary, &item) {
        case errSecSuccess: secKey = item as! SecKey
        case errSecItemNotFound: return nil
        case let status: throw SHKeychain.Error.unexpectedStatus(status)
        }
        
        // Convert the SecKey into a CryptoKit key.
        var error: Unmanaged<CFError>?
        guard let data = SecKeyCopyExternalRepresentation(secKey, &error) as Data? else {
            throw SHKeychain.Error.generic(error.debugDescription)
        }
        return try T(x963Representation: data)
    }
    
    static func retrieveKey<T: GenericPasswordConvertible>(account: String) throws -> T? {
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
            return try T(rawRepresentation: data)  // Convert back to a key.
        case errSecItemNotFound: return nil
        case let status: throw SHKeychain.Error.unexpectedStatus(status)
        }
    }
    
    static func removeKey(withLabel label: String) throws {
        // Describe the remove operation.
        let query = [kSecClass: kSecClassKey,
                     kSecAttrApplicationLabel: label] as [String: Any]
        
        let status = SecItemDelete(query as CFDictionary)
        guard status != errSecItemNotFound else { throw SHKeychain.Error.notFound }
        guard status == errSecSuccess else {
            throw SHKeychain.Error.unexpectedStatus(status)
        }
    }
    
    static func removePassword(forAccount account: String) throws {
        // Describe the remove operation.
        let query = [kSecClass: kSecClassGenericPassword,
                     kSecAttrAccount: account]  as [String: Any]
        
        let status = SecItemDelete(query as CFDictionary)
        guard status != errSecItemNotFound else { throw SHKeychain.Error.notFound }
        guard status == errSecSuccess else {
            throw SHKeychain.Error.unexpectedStatus(status)
        }
    }
    
    /* TODO: The following methods don't seem to work
     
    static func updateKey<T: SecKeyConvertible>(_ key: T, label: String) throws {
        // Describe the key.
        let attributes = [kSecAttrKeyType: kSecAttrKeyTypeECSECPrimeRandom,
                          kSecAttrKeyClass: kSecAttrKeyClassPrivate] as [String: Any]
        
        // Get a SecKey representation.
        guard let secKey = SecKeyCreateWithData(key.x963Representation as CFData,
                                                attributes as CFDictionary,
                                                nil)
            else {
                throw SHKeychain.Error.generic("Unable to create SecKey representation.")
        }
        
        // Describe the keys to update
        let attributesToUpdate = [kSecValueRef: secKey] as [String: Any]
        
        // Describe the update operation.
        let query = [kSecClass: kSecClassKey,
                     kSecAttrApplicationLabel: label] as [String: Any]

        // Update the key to the keychain.
        let status = SecItemUpdate(query as CFDictionary, attributesToUpdate as CFDictionary)
        guard status != errSecItemNotFound else { throw SHKeychain.Error.notFound }
        guard status == errSecSuccess else {
            throw SHKeychain.Error.unexpectedStatus(status)
        }
    }
    
    static func updateKey<T: GenericPasswordConvertible>(_ key: T, account: String) throws {
        // Treat the key data as a generic password.
        let query = [kSecClass: kSecClassGenericPassword,
                     kSecAttrAccount: account] as [String: Any]
        
        // Describe the keys to update
        let attributesToUpdate = [
            kSecValueData: key.rawRepresentation
        ] as [String: AnyObject]
        
        // Update the key data.
        let status = SecItemUpdate(query as CFDictionary, attributesToUpdate as CFDictionary)
        guard status != errSecItemNotFound else { throw SHKeychain.Error.notFound }
        guard status == errSecSuccess else {
            throw SHKeychain.Error.unexpectedStatus(status)
        }
    }
     
    */
    
}

#endif
