//
//  Asset.swift
//  
//
//  Created by Gennaro Frazzingaro on 9/22/21.
//

import Foundation
import Safehill_Crypto

public protocol SHAssetDescriptor {
    var globalIdentifier: String { get }
    var localIdentifier: String? { get }
    var creationDate: Date? { get }
    var sharedByUserIdentifier: String { get }
    var sharedWithUserIdentifiers: [String] { get }
}

public struct SHGenericAssetDescriptor : SHAssetDescriptor {
    public let globalIdentifier: String
    public let localIdentifier: String?
    public let creationDate: Date?
    public let sharedByUserIdentifier: String
    public let sharedWithUserIdentifiers: [String]
}

public protocol SHDecryptedAsset {
    var globalIdentifier: String { get }
    var localIdentifier: String? { get }
    var decryptedData: Data { get }
    var creationDate: Date? { get }
}

public struct SHGenericDecryptedAsset : SHDecryptedAsset {
    public let globalIdentifier: String
    public let localIdentifier: String?
    public let decryptedData: Data
    public let creationDate: Date?
}

public protocol SHEncryptedAsset {
    var globalIdentifier: String { get }
    var localIdentifier: String? { get }
    var encryptedData: Data { get }
    var encryptedSecret: Data { get }
    var publicKeyData: Data { get }
    var publicSignatureData: Data { get }
    var creationDate: Date? { get }
}

public struct SHGenericEncryptedAsset : SHEncryptedAsset {
    public let globalIdentifier: String
    public let localIdentifier: String?
    public let encryptedData: Data
    public let encryptedSecret: Data
    public let publicKeyData: Data
    public let publicSignatureData: Data
    public let creationDate: Date?
    
    public init(globalIdentifier: String,
                localIdentifier: String?,
                encryptedData: Data,
                encryptedSecret: Data,
                publicKeyData: Data,
                publicSignatureData: Data,
                creationDate: Date?) {
        self.globalIdentifier = globalIdentifier
        self.localIdentifier = localIdentifier
        self.encryptedData = encryptedData
        self.encryptedSecret = encryptedSecret
        self.publicKeyData = publicKeyData
        self.publicSignatureData = publicSignatureData
        self.creationDate = creationDate
        
    }
    
    public static func fromDict(_ dict: [String: Any]) throws -> SHEncryptedAsset? {
        if let assetIdentifier = dict["assetIdentifier"] as? String,
           let phAssetIdentifier = dict["applePhotosAssetIdentifier"] as? String?,
           let encryptedData = dict["encryptedData"] as? Data,
           let encryptedSecret = dict["encryptedSecret"] as? Data,
           let publicKeyData = dict["publicKey"] as? Data,
           let publicSignatureData = dict["publicSignature"] as? Data,
           let creationDate = dict["creationDate"] as? Date? {
            return SHGenericEncryptedAsset(globalIdentifier: assetIdentifier,
                                           localIdentifier: phAssetIdentifier,
                                           encryptedData: encryptedData,
                                           encryptedSecret: encryptedSecret,
                                           publicKeyData: publicKeyData,
                                           publicSignatureData: publicSignatureData,
                                           creationDate: creationDate)
        }
        return nil
    }

}


extension SHLocalUser {
    func decrypt(_ asset: SHEncryptedAsset, receivedFrom: SHServerUser) throws -> SHDecryptedAsset {
        let sharedSecret = SHShareablePayload(ephemeralPublicKeyData: asset.publicKeyData,
                                              cyphertext: asset.encryptedSecret,
                                              signature: asset.publicSignatureData)
        
        let decryptedData = try self.decrypted(data: asset.encryptedData,
                                               encryptedSecret: sharedSecret,
                                               receivedFrom: receivedFrom)
        return SHGenericDecryptedAsset(globalIdentifier: asset.globalIdentifier,
                                       localIdentifier: asset.localIdentifier,
                                       decryptedData: decryptedData,
                                       creationDate: asset.creationDate)
    }
}

