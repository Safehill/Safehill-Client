//
//  User.swift
//  
//
//  Created by Gennaro Frazzingaro on 9/22/21.
//

import Foundation

#if os(Linux)
@_exported import Crypto
import Logging
let log = Logger(label: "SafehillCrypto")
#else
import CryptoKit
import os
internal let log = Logger(subsystem: "com.gf.safehill.crypto", category: "SafehillCrypto")
#endif


protocol _SHCryptoUser {
    var publicKey: P256.KeyAgreement.PublicKey { get }
    var signature: P256.Signing.PublicKey { get }
}

public protocol SHCryptoUser {
    var publicKeyData: Data { get }
    var publicSignatureData: Data { get }
}


/// An entity known for its public key and signature
public struct SHRemoteCryptoUser : _SHCryptoUser, SHCryptoUser {
    public let publicKeyData: Data
    public let publicSignatureData: Data
    
    var publicKey: P256.KeyAgreement.PublicKey {
        try! P256.KeyAgreement.PublicKey(derRepresentation: publicKeyData)
    }
    var signature: P256.Signing.PublicKey {
        try! P256.Signing.PublicKey(derRepresentation: publicKeyData)
    }
    
    public init(publicKeyData: Data, publicSignatureData: Data) throws {
        self.publicKeyData = publicKeyData
        self.publicSignatureData = publicSignatureData
    }
    
    public func isValidSignature(_ signature: P256.Signing.ECDSASignature, for data: Data) -> Bool {
        return self.signature.isValidSignature(signature, for: data)
    }
}


/// An entity whose private keys and signature are known.
/// Usually represents a user on the local device, as the private portion of the keys are never shared
public struct SHLocalCryptoUser : _SHCryptoUser, SHCryptoUser, Codable {
    
    enum InitializationError: Error {
        case invalidKey(Any)
        case invalidSignature(Any)
    }
    
    enum CodingKeys: String, CodingKey {
        case privateKeyData
        case privateSignatureData
    }
    
    public var identifier: String {
        SHHash.stringDigest(for: publicSignatureData)
    }
    
    fileprivate let privateSignature: P256.Signing.PrivateKey
    fileprivate let privateKey: P256.KeyAgreement.PrivateKey
    
    var publicKey: P256.KeyAgreement.PublicKey {
        self.privateKey.publicKey
    }
    
    var signature: P256.Signing.PublicKey {
        self.privateSignature.publicKey
    }
    
    public var privateKeyData: Data {
        self.privateKey.derRepresentation
    }
    public var publicKeyData: Data {
        self.privateKey.publicKey.derRepresentation
    }
    public var publicSignatureData: Data {
        self.signature.derRepresentation
    }
    
    static func generateKey() -> P256.KeyAgreement.PrivateKey {
        return P256.KeyAgreement.PrivateKey()
    }
    
    static func generateSignature() -> P256.Signing.PrivateKey {
        return P256.Signing.PrivateKey()
    }
    
    /// Generates a new set of keys for encryption and signing
    public init() {
        self.privateKey = SHLocalCryptoUser.generateKey()
        self.privateSignature = SHLocalCryptoUser.generateSignature()
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        
        let privateKeyDataBase64 = try container.decode(String.self, forKey: .privateKeyData)
        let privateSignatureDataBase64 = try container.decode(String.self, forKey: .privateSignatureData)
        
        guard let privateKeyData = Data(base64Encoded: privateKeyDataBase64) else {
            throw SHLocalCryptoUser.InitializationError.invalidKey(privateKeyDataBase64)
        }
        guard let privateSignatureData = Data(base64Encoded: privateSignatureDataBase64) else {
            throw SHLocalCryptoUser.InitializationError.invalidSignature(privateKeyDataBase64)
        }
        
        privateKey = try P256.KeyAgreement.PrivateKey(derRepresentation: privateKeyData)
        privateSignature = try P256.Signing.PrivateKey(derRepresentation: privateSignatureData)
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(privateKey.derRepresentation.base64EncodedString(), forKey: .privateKeyData)
        try container.encode(privateSignature.derRepresentation.base64EncodedString(), forKey: .privateSignatureData)
    }
    
    public init(key: P256.KeyAgreement.PrivateKey, signature: P256.Signing.PrivateKey) {
        self.privateKey = key
        self.privateSignature = signature
    }
    
#if !os(Linux)
    public init(usingKeychainEntryWithLabel label: String) throws {
        let privateKey = try SHKeychain.retrieveKey(label: label + ".key") as P256.KeyAgreement.PrivateKey?
        let privateSignature = try SHKeychain.retrieveKey(label: label + ".signature") as P256.Signing.PrivateKey?
        
        guard let pk = privateKey, let sig = privateSignature  else {
            if privateKey == nil {
                log.error("Couldn't find private key in keychain \(label)")
                throw SHKeychain.Error.generic("No entry in keychain for \(label).key")
            } else if privateSignature == nil {
                log.error("Couldn't find private signature in keychain \(label)")
                throw SHKeychain.Error.generic("No entry in keychain for \(label).signature")
            } else {
                log.error("Couldn't find private key and private signature in keychain \(label)")
                throw SHKeychain.Error.generic("No entry in keychain with label \(label)(.key|.signature)")
            }
        }
        
#if DEBUG
        let publicSignatureData = sig.publicKey.derRepresentation
        let identifier = SHHash.stringDigest(for: publicSignatureData)
        log.info("Found keys in keychain \(label). Derived user identifier is \(identifier))")
#endif
        
        self.init(key: pk, signature: sig)
    }
    
    public func saveKeysToKeychain(withLabel label: String, force: Bool = false) throws {
        do {
            try SHKeychain.storeKey(privateKey, label: label + ".key")
            try SHKeychain.storeKey(privateSignature, label: label + ".signature")
        } catch SHKeychain.Error.unexpectedStatus(let status) {
            if status == -25299 && force == true {
                try? self.deleteKeysInKeychain(withLabel: label)
                
                try SHKeychain.storeKey(privateKey, label: label + ".key")
                try SHKeychain.storeKey(privateSignature, label: label + ".signature")
            } else {
                throw SHKeychain.Error.unexpectedStatus(status)
            }
        }
#if DEBUG
        let publicSignatureData = privateSignature.publicKey.derRepresentation
        var identifier = SHHash.stringDigest(for: publicSignatureData)
        log.info("Saving keys in keychain \(label). Derived user identifier is \(identifier))")
        let retrievedPrivateSignature = try SHKeychain.retrieveKey(label: label + ".signature") as P256.Signing.PrivateKey?
        identifier = SHHash.stringDigest(for: retrievedPrivateSignature!.publicKey.derRepresentation)
        log.info("Derived user identifier for current item in keychain \(label) is \(identifier))")
#endif
    }
    
    public func deleteKeysInKeychain(withLabel label: String) throws {
        try SHKeychain.removeKey(withLabel: label + ".key")
        try SHKeychain.removeKey(withLabel: label + ".signature")
#if DEBUG
        log.info("Successfully deleted key in keychain \(label)")
        if (try? SHKeychain.retrieveKey(label: label + ".signature") as P256.Signing.PrivateKey?) != nil {
            fatalError("Key is still in the keychain")
        }
#endif
    }
#endif
    
    public func signature(for data: Data) throws -> P256.Signing.ECDSASignature {
        return try self.privateSignature.signature(for: data)
    }
}


public struct SHUserContext {
    let myUser: SHLocalCryptoUser
    
    public init(user: SHLocalCryptoUser) {
        self.myUser = user
    }
}


extension SHUserContext {
    public func shareable(data: Data, with user: SHCryptoUser) throws -> SHShareablePayload {
        log.info("encrypting data for user with public key \(user.publicKeyData.base64EncodedString()) public signature \(user.publicSignatureData.base64EncodedString())")
        let ephemeralKey = P256.KeyAgreement.PrivateKey()
        let userPublicKey = try P256.KeyAgreement.PublicKey(derRepresentation: user.publicKeyData)
        let encrypted = try SHCypher.encrypt(data,
                                             to: userPublicKey,
                                             using: ephemeralKey,
                                             signedBy: myUser.privateSignature)
        return SHShareablePayload(ephemeralPublicKeyData: ephemeralKey.publicKey.derRepresentation,
                                  cyphertext: encrypted.cyphertext,
                                  signature: encrypted.signature,
                                  recipient: user)
    }
    
    public func decrypt(_ data: Data,
                        usingEncryptedSecret encryptedSecret: SHShareablePayload,
                        receivedFrom sender: SHCryptoUser) throws -> Data {
        log.info("decrypting data received from sender with public key \(sender.publicKeyData.base64EncodedString()) public signature \(sender.publicSignatureData.base64EncodedString())")
        let senderPublicSignature = try P256.Signing.PublicKey(derRepresentation: sender.publicSignatureData)
        let secretData = try SHCypher.decrypt(encryptedSecret,
                                              using: myUser.privateKey,
                                              from: senderPublicSignature)
        let secret = try SymmetricKey(rawRepresentation: secretData)
        return try SHCypher.decrypt(data: data, using: secret)
    }
}
