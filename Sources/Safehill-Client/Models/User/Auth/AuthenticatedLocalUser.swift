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
    
    public var serverProxy: SHServerProxy {
        SHServerProxy(user: self)
    }
    
    public var identifier: String { self.shUser.identifier }
    public var name: String
    public var phoneNumber: String?
    
    public var publicKeyData: Data { self.shUser.publicKeyData }
    public var publicSignatureData: Data { self.shUser.publicSignatureData }
    
    public let keychainPrefix: String
    
    public init?(localUser: SHLocalUser, name: String, phoneNumber: String?) {
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
    }
    
    internal init(
        localUser: SHLocalUser,
        name: String,
        phoneNumber: String?,
        encryptionProtocolSalt: Data,
        authToken: String
    ) {
        self.keychainPrefix = localUser.keychainPrefix
        self.authToken = authToken
        self.encryptionProtocolSalt = encryptionProtocolSalt
        self.shUser = localUser.shUser
        self.name = name
        self.phoneNumber = phoneNumber
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
    func shareableEncryptedAsset(
        globalIdentifier: String,
        versions: [SHAssetQuality],
        createdBy: any SHServerUser,
        with recipients: [any SHServerUser],
        groupId: String,
        completionHandler: @escaping (Result<any SHShareableEncryptedAsset, Error>) -> Void
    ) {
        let localAssetStoreController = SHLocalAssetStoreController(user: self)
        
        localAssetStoreController.retrieveCommonEncryptionKey(
            for: globalIdentifier,
            signedBy: createdBy
        ) {
            result in
            switch result {
            case .failure(let error):
                completionHandler(.failure(error))
            case .success(let privateSecret):
                var shareableVersions = [SHShareableEncryptedAssetVersion]()
                
                for recipient in recipients {
                    do {
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
                        
                    } catch {
                        completionHandler(.failure(error))
                        return
                    }
                }
                
                let shareableEncryptedAsset = SHGenericShareableEncryptedAsset(
                    globalIdentifier: globalIdentifier,
                    sharedVersions: shareableVersions,
                    groupId: groupId
                )
                
                completionHandler(.success(shareableEncryptedAsset))
            }
        }
    }
}
