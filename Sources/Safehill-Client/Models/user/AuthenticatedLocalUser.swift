import Foundation
import Safehill_Crypto

/// 
/// An immutable version of the `SHLocalUser` after it's been authenticated
/// where `encryptionProtocolSalt` and `authToken` is guaranteed to be set.
///
public struct SHAuthenticatedLocalUser: SHLocalUserProtocol {
    
    public var maybeEncryptionProtocolSalt: Data? {
        self.encryptionProtocolSalt
    }
    
    let authToken: String
    let encryptionProtocolSalt: Data
    public let shUser: SHLocalCryptoUser
    
    public let serverProxy: SHServerProxy
    
    public var identifier: String { self.shUser.identifier }
    public var name: String
    
    public var publicKeyData: Data { self.shUser.publicKeyData }
    public var publicSignatureData: Data { self.shUser.publicSignatureData }
    
    public let keychainPrefix: String
    
    public init?(localUser: SHLocalUser, name: String) {
        guard let encryptionProtocolSalt = localUser.maybeEncryptionProtocolSalt,
              let authToken = localUser.authToken
        else {
            return nil
        }
        
        self.authToken = authToken
        self.encryptionProtocolSalt = encryptionProtocolSalt
        self.shUser = localUser.shUser
        self.name = name
        self.keychainPrefix = localUser.keychainPrefix
        
        self.serverProxy = SHServerProxy(user: localUser)
    }
    
    internal init(
        localUser: SHLocalUser,
        name: String,
        encryptionProtocolSalt: Data,
        authToken: String
    ) {
        self.keychainPrefix = localUser.keychainPrefix
        self.authToken = authToken
        self.encryptionProtocolSalt = encryptionProtocolSalt
        self.shUser = localUser.shUser
        self.name = name
        
        self.serverProxy = SHServerProxy(user: localUser)
    }
    
    public func deauthenticate() throws -> SHLocalUser {
        try self.deleteAuthFromKeychain()
        return SHLocalUser(keychainPrefix: self.keychainPrefix)
    }
}

extension SHAuthenticatedLocalUser {
    
    /// Generate a shareable version of the asset, namely a structure that can be used by the sender
    /// to securely share an asset with the specified recipients.
    ///
    /// - Parameters:
    ///   - globalIdentifier: the asset global identifier
    ///   - versions: the versions to share (resulting in `SHShareableEncryptedAssetVersion` in the `SHShareableEncryptedAsset` returned)
    ///   - sender: the user wanting to share the asset
    ///   - recipients: the list of users the asset should be made shareable to
    ///   - groupId: the unique identifier of the share request
    /// - Returns: the `SHShareableEncryptedAsset`
    func shareableEncryptedAsset(globalIdentifier: String,
                                 versions: [SHAssetQuality],
                                 recipients: [any SHServerUser],
                                 groupId: String) throws -> any SHShareableEncryptedAsset {
        let localAssetStoreController = SHLocalAssetStoreController(user: self)
        let privateSecret = try localAssetStoreController.retrieveCommonEncryptionKey(for: globalIdentifier)
        var shareableVersions = [SHShareableEncryptedAssetVersion]()
        
        for recipient in recipients {
            ///
            /// Encrypt the secret using the recipient's public key
            /// so that it can be stored securely on the server
            ///
            let encryptedAssetSecret = try self.createShareablePayload(
                from: privateSecret,
                toShareWith: recipient
            )
            
            for quality in versions {
                let shareableVersion = SHGenericShareableEncryptedAssetVersion(
                    quality: quality,
                    userPublicIdentifier: recipient.identifier,
                    encryptedSecret: encryptedAssetSecret.cyphertext,
                    ephemeralPublicKey: encryptedAssetSecret.ephemeralPublicKeyData,
                    publicSignature: encryptedAssetSecret.signature
                )
                shareableVersions.append(shareableVersion)
            }
        }
        
        return SHGenericShareableEncryptedAsset(
            globalIdentifier: globalIdentifier,
            sharedVersions: shareableVersions,
            groupId: groupId
        )
    }
}
