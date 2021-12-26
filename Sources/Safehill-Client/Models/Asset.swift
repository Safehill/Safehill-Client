//
//  Asset.swift
//  
//
//  Created by Gennaro Frazzingaro on 9/22/21.
//

import Foundation
import Safehill_Crypto

public enum SHAssetQuality: String {
    case lowResolution = "low", hiResolution = "hi"
}

public protocol SHAssetDescriptor {
    var globalIdentifier: String { get }
    var localIdentifier: String? { get }
    var creationDate: Date? { get }
    var sharedByUserIdentifier: String { get }
    var sharedWithUserIdentifiers: [String] { get }
}

public struct SHGenericAssetDescriptor : SHAssetDescriptor, Codable {
    public let globalIdentifier: String
    public let localIdentifier: String?
    public let creationDate: Date?
    public let sharedByUserIdentifier: String
    public let sharedWithUserIdentifiers: [String]
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        globalIdentifier = try container.decode(String.self, forKey: .globalIdentifier)
        localIdentifier = try container.decode(String.self, forKey: .localIdentifier)
        sharedByUserIdentifier = try container.decode(String.self, forKey: .sharedByUserIdentifier)
        sharedWithUserIdentifiers = try container.decode([String].self, forKey: .sharedWithUserIdentifiers)
        let dateString = try container.decode(String.self, forKey: .creationDate)
        creationDate = dateString.iso8601withFractionalSeconds
    }
    
    public init(globalIdentifier: String,
                localIdentifier: String?,
                creationDate: Date?,
                sharedByUserIdentifier: String,
                sharedWithUserIdentifiers: [String]) {
        self.globalIdentifier = globalIdentifier
        self.localIdentifier = localIdentifier
        self.creationDate = creationDate
        self.sharedByUserIdentifier = sharedByUserIdentifier
        self.sharedWithUserIdentifiers = sharedWithUserIdentifiers
    }
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

public struct SHServerAssetVersion : Codable {
    let versionName: String
    let publicKeyData: Data
    let publicSignatureData: Data
    let encryptedSecret: Data
    let presignedURL: String
    let presignedURLExpiresInMinutes: Int
    
    enum CodingKeys: String, CodingKey {
        case versionName
        case publicKeyData = "publicKey"
        case publicSignatureData = "publicSignature"
        case encryptedSecret
        case presignedURL
        case presignedURLExpiresInMinutes
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        
        versionName = try container.decode(String.self, forKey: .versionName)
        let encryptedSecretBase64 = try container.decode(String.self, forKey: .encryptedSecret)
        encryptedSecret = Data(base64Encoded: encryptedSecretBase64)!
        let publicKeyDataBase64 = try container.decode(String.self, forKey: .publicKeyData)
        publicKeyData = Data(base64Encoded: publicKeyDataBase64)!
        let publicSignatureDataBase64 = try container.decode(String.self, forKey: .publicSignatureData)
        publicSignatureData = Data(base64Encoded: publicSignatureDataBase64)!
        
        presignedURL = try container.decode(String.self, forKey: .presignedURL)
        presignedURLExpiresInMinutes = try container.decode(Int.self, forKey: .presignedURLExpiresInMinutes)
    }
}

public struct SHServerAsset : Codable {
    let globalIdentifier: String
    let localIdentifier: String?
    let creationDate: Date?
    let versions: [SHServerAssetVersion]
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        
        globalIdentifier = try container.decode(String.self, forKey: .globalIdentifier)
        localIdentifier = try container.decode(String.self, forKey: .localIdentifier)
        let dateString = try container.decode(String.self, forKey: .creationDate)
        creationDate = dateString.iso8601withFractionalSeconds
        versions = try container.decode([SHServerAssetVersion].self, forKey: .versions)
    }
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

