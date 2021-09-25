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
    var phoneNumber: String? { get }
}

public struct SHRemoteUser : SHServerUser {
    public let identifier: String
    public let name: String?
    public let phoneNumber: String?
    public let publicKeyData: Data
    public let publicSignatureData: Data
    
    init(identifier: String, name: String, phoneNumber: String, publicKeyData: Data, publicSignatureData: Data) throws {
        self.identifier = identifier
        self.publicKeyData = publicKeyData
        self.publicSignatureData = publicSignatureData
        self.name = name
        self.phoneNumber = phoneNumber
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
    public var phoneNumber: String?
    
    static func == (lhs: SHLocalUser, rhs: SHLocalUser) -> Bool {
        return lhs.publicKeyData == rhs.publicKeyData
        && lhs.publicSignatureData == rhs.publicSignatureData
    }
    
    public init(keychainLabel: String) {
        if let shUser = try? SHLocalCryptoUser(usingKeychainEntryWithLabel: keychainLabel) {
            self.shUser = shUser
        } else {
            self.shUser = SHLocalCryptoUser()
            try? self.shUser.saveToKeychain(withLabel: keychainLabel)
        }
        
        self.publicKeyData = shUser.publicKeyData
        self.publicSignatureData = shUser.publicSignatureData
    }
    
    mutating func authenticate(name: String, phoneNumber: String) {
        self.name = name
        self.phoneNumber = phoneNumber
    }
    
    public func shareable(data: Data, with user: SHCryptoUser) throws -> SHShareablePayload {
        try SHUserContext(user: self.shUser).shareable(data: data, with: user)
    }
    
    public func decrypted(data: Data, encryptedSecret: SHShareablePayload, receivedFrom user: SHCryptoUser) throws -> Data {
        try SHUserContext(user: self.shUser).decrypt(data, usingEncryptedSecret: encryptedSecret, receivedFrom: user)
    }
    
    public mutating func regenerateKeys(savingToKeychainLabel keychainLabel: String) {
        // TODO: Should remove old?
        self.shUser = SHLocalCryptoUser()
        try? self.shUser.saveToKeychain(withLabel: keychainLabel)
    }
}

public extension SHUserContext {
    init(localUser: SHLocalUser) {
        self.init(user: localUser.shUser)
    }
}
