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
    
    static var all: [SHAssetQuality] {
        return [.lowResolution, .hiResolution]
    }
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
    public let versionName: String
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
    
    public init(versionName: String, publicKeyData: Data, publicSignatureData: Data, encryptedSecret: Data, presignedURL: String, presignedURLExpiresInMinutes: Int) {
        self.versionName = versionName
        self.publicKeyData = publicKeyData
        self.publicSignatureData = publicSignatureData
        self.encryptedSecret = encryptedSecret
        self.presignedURL = presignedURL
        self.presignedURLExpiresInMinutes = presignedURLExpiresInMinutes
    }
}

public struct SHServerAsset : Codable {
    public let globalIdentifier: String
    public let localIdentifier: String?
    public let creationDate: Date?
    public let versions: [SHServerAssetVersion]
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        
        globalIdentifier = try container.decode(String.self, forKey: .globalIdentifier)
        localIdentifier = try container.decode(String.self, forKey: .localIdentifier)
        let dateString = try container.decode(String.self, forKey: .creationDate)
        creationDate = dateString.iso8601withFractionalSeconds
        versions = try container.decode([SHServerAssetVersion].self, forKey: .versions)
    }
    
    public init(globalIdentifier: String, localIdentifier: String?, creationDate: Date?, versions: [SHServerAssetVersion]) {
        self.globalIdentifier = globalIdentifier
        self.localIdentifier = localIdentifier
        self.creationDate = creationDate
        self.versions = versions
    }
}

public protocol SHEncryptedAssetVersion {
    var quality: SHAssetQuality { get }
    var encryptedData: Data { get }
    var encryptedSecret: Data { get }
    var publicKeyData: Data { get }
    var publicSignatureData: Data { get }
}

public protocol SHEncryptedAsset {
    var globalIdentifier: String { get }
    var localIdentifier: String? { get }
    var creationDate: Date? { get }
    var encryptedVersions: [SHEncryptedAssetVersion] { get }
}

public protocol SHShareableEncryptedAssetVersion {
    var quality: SHAssetQuality { get }
    var userPublicIdentifier: String { get }
    var encryptedSecret: Data { get }
}

public protocol SHShareableEncryptedAsset {
    var globalIdentifier: String { get }
    var sharedVersions: [SHShareableEncryptedAssetVersion] { get }
}

public struct SHGenericEncryptedAssetVersion : SHEncryptedAssetVersion {
    public let quality: SHAssetQuality
    public let encryptedData: Data
    public let encryptedSecret: Data
    public let publicKeyData: Data
    public let publicSignatureData: Data
    
    public init(quality: SHAssetQuality,
                encryptedData: Data,
                encryptedSecret: Data,
                publicKeyData: Data,
                publicSignatureData: Data) {
        self.quality = quality
        self.encryptedData = encryptedData
        self.encryptedSecret = encryptedSecret
        self.publicKeyData = publicKeyData
        self.publicSignatureData = publicSignatureData
    }
}

public struct SHGenericEncryptedAsset : SHEncryptedAsset {
    public let globalIdentifier: String
    public let localIdentifier: String?
    public let creationDate: Date?
    public let encryptedVersions: [SHEncryptedAssetVersion]
    
    public init(globalIdentifier: String,
                localIdentifier: String?,
                creationDate: Date?,
                encryptedVersions: [SHEncryptedAssetVersion]) {
        self.globalIdentifier = globalIdentifier
        self.localIdentifier = localIdentifier
        self.creationDate = creationDate
        self.encryptedVersions = encryptedVersions
        
    }
    
    public static func fromDict(_ dict: [String: Any]) throws -> SHEncryptedAsset? {
        if let qualityS = dict["quality"] as? String,
           let quality = SHAssetQuality(rawValue: qualityS),
           let assetIdentifier = dict["assetIdentifier"] as? String,
           let phAssetIdentifier = dict["applePhotosAssetIdentifier"] as? String?,
           let encryptedData = dict["encryptedData"] as? Data,
           let encryptedSecret = dict["senderEncryptedSecret"] as? Data,
           let publicKeyData = dict["publicKey"] as? Data,
           let publicSignatureData = dict["publicSignature"] as? Data,
           let creationDate = dict["creationDate"] as? Date? {
            let version = SHGenericEncryptedAssetVersion(
                quality: quality,
                encryptedData: encryptedData,
                encryptedSecret: encryptedSecret,
                publicKeyData: publicKeyData,
                publicSignatureData: publicSignatureData)
            return SHGenericEncryptedAsset(globalIdentifier: assetIdentifier,
                                           localIdentifier: phAssetIdentifier,
                                           creationDate: creationDate,
                                           encryptedVersions: [version])
        }
        return nil
    }
}

public struct SHGenericShareableEncryptedAssetVersion : SHShareableEncryptedAssetVersion {
    public let quality: SHAssetQuality
    public let userPublicIdentifier: String
    public let encryptedSecret: Data
}
    

public struct SHGenericShareableEncryptedAsset : SHShareableEncryptedAsset {
    public let globalIdentifier: String
    public let sharedVersions: [SHShareableEncryptedAssetVersion]

}


extension SHLocalUser {
    func decrypt(_ asset: SHEncryptedAsset, quality: SHAssetQuality, receivedFrom: SHServerUser) throws -> SHDecryptedAsset {
        guard let version = asset.encryptedVersions.first(where: { $0.quality == quality }) else {
            throw SHAssetFetchError.fatalError("No such version \(quality.rawValue) for asset=\(asset.globalIdentifier)")
        }
        
        let sharedSecret = SHShareablePayload(ephemeralPublicKeyData: version.publicKeyData,
                                              cyphertext: version.encryptedSecret,
                                              signature: version.publicSignatureData)
        
        let decryptedData = try self.decrypted(data: version.encryptedData,
                                               encryptedSecret: sharedSecret,
                                               receivedFrom: receivedFrom)
        
        return SHGenericDecryptedAsset(globalIdentifier: asset.globalIdentifier,
                                       localIdentifier: asset.localIdentifier,
                                       decryptedData: decryptedData,
                                       creationDate: asset.creationDate)
    }
}

