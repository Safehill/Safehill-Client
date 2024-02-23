import Foundation
import Safehill_Crypto

public protocol SHLocalUserProtocol : SHServerUser {
    var shUser: SHLocalCryptoUser { get }
    var maybeEncryptionProtocolSalt: Data? { get }
    var serverProxy: SHServerProxy { get }
    
    var keychainPrefix: String { get }
    
    static func authKeychainLabel(keychainPrefix: String) -> String
    static func authTokenKeychainLabel(keychainPrefix: String) -> String
    
    func deauthenticate() throws
    func destroy() throws
    
    func createShareablePayload(
        from data: Data,
        toShareWith user: SHCryptoUser
    ) throws -> SHShareablePayload
    
    func decrypt(data: Data,
                 encryptedSecret: SHShareablePayload,
                 receivedFrom user: SHCryptoUser
    ) throws -> Data
}

extension SHLocalUserProtocol {
    
    public static func keysKeychainLabel(keychainPrefix: String) -> String {
        "\(keychainPrefix).keys"
    }
    
    public static func authKeychainLabel(keychainPrefix: String) -> String {
        "\(keychainPrefix).auth"
    }
    public static func authTokenKeychainLabel(keychainPrefix: String) -> String {
        "\(authKeychainLabel(keychainPrefix: keychainPrefix)).token"
    }
    
    internal static func deleteAuthToken(
        _ keychainPrefix: String
    ) throws {
        let authTokenLabel = SHLocalUser.authTokenKeychainLabel(keychainPrefix: keychainPrefix)
        
        do {
            try SHKeychain.deleteValue(account: authTokenLabel)
        } catch {
            log.fault("auth token could not be removed from the keychain")
            throw SHLocalUserError.failedToRemoveKeychainEntry
        }
    }
    
    internal static func deleteProtocolSalt(
        _ keychainPrefix: String
    ) throws {
        /// Delete the protocol salt
        let saltKeychainLabel = SHLocalUser.saltKeychainLabel(keychainPrefix: keychainPrefix)
        try? SHKeychain.deleteValue(account: saltKeychainLabel)
    }
    
    internal static func deleteKeys(
        _ keychainPrefix: String
    ) throws {
        /// Delete the keys
        let keysKeychainLabel = Self.keysKeychainLabel(keychainPrefix: keychainPrefix)
        try SHLocalCryptoUser.deleteKeysInKeychain(withLabel: keysKeychainLabel)
    }
    
    public func deauthenticate() throws {
        try Self.deleteAuthToken(self.keychainPrefix)
    }
    
    public func destroy() throws {
        try Self.deleteAuthToken(self.keychainPrefix)
        try Self.deleteProtocolSalt(self.keychainPrefix)
        try Self.deleteKeys(self.keychainPrefix)
    }
}

extension SHLocalUserProtocol {
    public func createShareablePayload(
        from data: Data,
        toShareWith user: SHCryptoUser
    ) throws -> SHShareablePayload {
        guard let salt = self.maybeEncryptionProtocolSalt else {
            throw SHLocalUserError.missingProtocolSalt
        }
        return try SHUserContext(user: self.shUser)
            .shareable(data: data,
                       protocolSalt: salt,
                       with: user)
    }
    
    public func decrypt(data: Data,
                 encryptedSecret: SHShareablePayload,
                 receivedFrom user: SHCryptoUser
    ) throws -> Data {
        guard let salt = self.maybeEncryptionProtocolSalt else {
            throw SHLocalUserError.missingProtocolSalt
        }
        return try SHUserContext(user: self.shUser)
            .decrypt(data, usingEncryptedSecret: encryptedSecret,
                     protocolSalt: salt,
                     receivedFrom: user
            )
    }
}

extension SHLocalUserProtocol {
    func decrypt(_ asset: any SHEncryptedAsset, quality: SHAssetQuality, receivedFrom user: SHServerUser) throws -> any SHDecryptedAsset {
        guard let version = asset.encryptedVersions[quality] else {
            throw SHBackgroundOperationError.fatalError("No such version \(quality.rawValue) for asset=\(asset.globalIdentifier)")
        }
        
        let sharedSecret = SHShareablePayload(
            ephemeralPublicKeyData: version.publicKeyData,
            cyphertext: version.encryptedSecret,
            signature: version.publicSignatureData
        )
        let decryptedData = try self.decrypt(
            data: version.encryptedData,
            encryptedSecret: sharedSecret,
            receivedFrom: user
        )
        return SHGenericDecryptedAsset(
            globalIdentifier: asset.globalIdentifier,
            localIdentifier: asset.localIdentifier,
            decryptedVersions: [quality: decryptedData],
            creationDate: asset.creationDate
        )
    }
}
