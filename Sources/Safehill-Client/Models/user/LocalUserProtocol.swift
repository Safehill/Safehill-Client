import Foundation
import Safehill_Crypto

public protocol SHLocalUserProtocol : SHServerUser {
    var shUser: SHLocalCryptoUser { get }
    var maybeEncryptionProtocolSalt: Data? { get }
    var serverProxy: SHServerProxy { get }
    
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
