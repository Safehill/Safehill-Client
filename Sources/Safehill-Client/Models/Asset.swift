import Foundation
import Safehill_Crypto

/// Safehill clients store 2 versions per asset, one low resolution for the thumbnail, one full size
public enum SHAssetQuality: String {
    case lowResolution = "low", hiResolution = "hi"
    
    static var all: [SHAssetQuality] {
        return [.lowResolution, .hiResolution]
    }
}

public protocol SHAssetGroupInfo {
    /// The name of the asset group (optional)
    var name: String? { get }
    /// ISO8601 formatted datetime, representing the time the asset group was created
    var createdAt: Date? { get }
 }

public struct SHGenericAssetGroupInfo : SHAssetGroupInfo, Codable {
    public let name: String?
    public let createdAt: Date?
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        name = try? container.decode(String?.self, forKey: .name)
        let dateString = try? container.decode(String?.self, forKey: .createdAt)
        createdAt = dateString?.iso8601withFractionalSeconds
    }
    
    init(name: String?, createdAt: Date?) {
        self.name = name
        self.createdAt = createdAt
    }
}

public protocol SHDescriptorSharingInfo {
    var sharedByUserIdentifier: String { get }
    /// Maps user public identifiers to asset group identifiers
    var sharedWithUserIdentifiersInGroup: [String: String] { get }
    var groupInfoById: [String: SHAssetGroupInfo] { get }
}

public struct SHGenericDescriptorSharingInfo : SHDescriptorSharingInfo, Codable {
    public let sharedByUserIdentifier: String
    public let sharedWithUserIdentifiersInGroup: [String: String]
    public let groupInfoById: [String: SHAssetGroupInfo]
    
    enum CodingKeys: String, CodingKey {
        case sharedByUserIdentifier
        case sharedWithUserIdentifiersInGroup
        case groupInfoById
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(sharedByUserIdentifier, forKey: .sharedByUserIdentifier)
        try container.encode(sharedWithUserIdentifiersInGroup, forKey: .sharedWithUserIdentifiersInGroup)
        try container.encode(groupInfoById as! [String: SHGenericAssetGroupInfo], forKey: .groupInfoById)
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        sharedByUserIdentifier = try container.decode(String.self, forKey: .sharedByUserIdentifier)
        sharedWithUserIdentifiersInGroup = try container.decode([String: String].self, forKey: .sharedWithUserIdentifiersInGroup)
        groupInfoById = try container.decode([String: SHGenericAssetGroupInfo].self, forKey: .groupInfoById)
    }
    
    public init(sharedByUserIdentifier: String,
                sharedWithUserIdentifiersInGroup: [String: String],
                groupInfoById: [String: SHAssetGroupInfo]) {
        self.sharedByUserIdentifier = sharedByUserIdentifier
        self.sharedWithUserIdentifiersInGroup = sharedWithUserIdentifiersInGroup
        self.groupInfoById = groupInfoById
    }
}

public enum SHAssetDescriptorUploadState: String {
    case notStarted = "not_started", partial = "partial", completed = "completed"
}

/// Safehill Server descriptor: metadata associated with an asset, such as creation date, sender and list of receivers
public protocol SHAssetDescriptor {
    var globalIdentifier: String { get }
    var localIdentifier: String? { get set }
    var creationDate: Date? { get }
    var uploadState: SHAssetDescriptorUploadState { get }
    var sharingInfo: SHDescriptorSharingInfo { get }
}

public struct SHGenericAssetDescriptor : SHAssetDescriptor, Codable {
    
    public let globalIdentifier: String
    public var localIdentifier: String?
    public let creationDate: Date?
    public let uploadState: SHAssetDescriptorUploadState
    public let sharingInfo: SHDescriptorSharingInfo
    
    enum CodingKeys: String, CodingKey {
        case globalIdentifier
        case localIdentifier
        case creationDate
        case uploadState
        case sharingInfo
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(globalIdentifier, forKey: .globalIdentifier)
        try container.encode(localIdentifier, forKey: .localIdentifier)
        try container.encode(creationDate, forKey: .creationDate)
        try container.encode(uploadState.rawValue, forKey: .uploadState)
        try container.encode(sharingInfo as! SHGenericDescriptorSharingInfo, forKey: .sharingInfo)
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        globalIdentifier = try container.decode(String.self, forKey: .globalIdentifier)
        localIdentifier = try container.decode(String.self, forKey: .localIdentifier)
        let dateString = try container.decode(String.self, forKey: .creationDate)
        creationDate = dateString.iso8601withFractionalSeconds
        let uploadStateString = try container.decode(String.self, forKey: .uploadState)
        guard let uploadState = SHAssetDescriptorUploadState(rawValue: uploadStateString) else {
            throw DecodingError.dataCorrupted(.init(codingPath: [CodingKeys.uploadState],
                                                    debugDescription: "Invalid UploadState value \(uploadStateString)")
            )
        }
        self.uploadState = uploadState
        sharingInfo = try container.decode(SHGenericDescriptorSharingInfo.self, forKey: .sharingInfo)
    }
    
    public init(globalIdentifier: String,
                localIdentifier: String?,
                creationDate: Date?,
                uploadState: SHAssetDescriptorUploadState,
                sharingInfo: SHDescriptorSharingInfo) {
        self.globalIdentifier = globalIdentifier
        self.localIdentifier = localIdentifier
        self.creationDate = creationDate
        self.uploadState = uploadState
        self.sharingInfo = sharingInfo
    }
}

/// The interface representing an asset after being decrypted
public protocol SHDecryptedAsset {
    var globalIdentifier: String { get }
    var localIdentifier: String? { get set }
    var decryptedData: Data { get }
    var creationDate: Date? { get }
}

public struct SHGenericDecryptedAsset : SHDecryptedAsset {
    public let globalIdentifier: String
    public var localIdentifier: String?
    public let decryptedData: Data
    public let creationDate: Date?
}

/// Safehill Server description of a version associated with an asset
public struct SHServerAssetVersion : Codable {
    public let versionName: String
    let publicKeyData: Data
    let publicSignatureData: Data
    let encryptedSecret: Data
    let presignedURL: String
    let presignedURLExpiresInMinutes: Int
    
    enum CodingKeys: String, CodingKey {
        case versionName
        case publicKeyData = "ephemeralPublicKey"
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
    
    public init(versionName: String,
                publicKeyData: Data,
                publicSignatureData: Data,
                encryptedSecret: Data,
                presignedURL: String,
                presignedURLExpiresInMinutes: Int)
    {
        self.versionName = versionName
        self.publicKeyData = publicKeyData
        self.publicSignatureData = publicSignatureData
        self.encryptedSecret = encryptedSecret
        self.presignedURL = presignedURL
        self.presignedURLExpiresInMinutes = presignedURLExpiresInMinutes
    }
}

/// Safehill Server description of an asset
public struct SHServerAsset : Codable {
    public let globalIdentifier: String
    public let localIdentifier: String?
    public let creationDate: Date?
    public let groupId: String
    public let versions: [SHServerAssetVersion]
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        
        globalIdentifier = try container.decode(String.self, forKey: .globalIdentifier)
        localIdentifier = try container.decode(String.self, forKey: .localIdentifier)
        let dateString = try container.decode(String.self, forKey: .creationDate)
        creationDate = dateString.iso8601withFractionalSeconds
        groupId = try container.decode(String.self, forKey: .groupId)
        versions = try container.decode([SHServerAssetVersion].self, forKey: .versions)
    }
    
    public init(globalIdentifier: String,
                localIdentifier: String?,
                creationDate: Date?,
                groupId: String,
                versions: [SHServerAssetVersion]) {
        self.globalIdentifier = globalIdentifier
        self.localIdentifier = localIdentifier
        self.creationDate = creationDate
        self.groupId = groupId
        self.versions = versions
    }
}

/// The interface representing a locally encrypted version of an asset
public protocol SHEncryptedAssetVersion {
    var quality: SHAssetQuality { get }
    var encryptedData: Data { get }
    var encryptedSecret: Data { get }
    var publicKeyData: Data { get }
    var publicSignatureData: Data { get }
}

/// The interface representing a locally encrypted asset
public protocol SHEncryptedAsset {
    var globalIdentifier: String { get }
    var localIdentifier: String? { get }
    var creationDate: Date? { get }
    var encryptedVersions: [SHEncryptedAssetVersion] { get }
    var groupId: String { get }
}

/// The interface representing a locally encrypted asset version ready to be shared, hence holding the secret for the user it's being shared with
public protocol SHShareableEncryptedAssetVersion {
    var quality: SHAssetQuality { get }
    var userPublicIdentifier: String { get }
    var encryptedSecret: Data { get }
    var ephemeralPublicKey: Data { get }
    var publicSignature: Data { get }
}

/// The interface representing a locally encrypted asset ready to be shared
public protocol SHShareableEncryptedAsset {
    var globalIdentifier: String { get }
    var sharedVersions: [SHShareableEncryptedAssetVersion] { get }
    var groupId: String { get }
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
    
    public static func fromDict(_ dict: [String: Any]) -> SHEncryptedAssetVersion? {
        if let qualityS = dict["quality"] as? String,
           let quality = SHAssetQuality(rawValue: qualityS),
           let encryptedData = dict["encryptedData"] as? Data,
           let encryptedSecret = dict["senderEncryptedSecret"] as? Data,
           let publicKeyData = dict["publicKey"] as? Data,
           let publicSignatureData = dict["publicSignature"] as? Data {
            return SHGenericEncryptedAssetVersion(
                quality: quality,
                encryptedData: encryptedData,
                encryptedSecret: encryptedSecret,
                publicKeyData: publicKeyData,
                publicSignatureData: publicSignatureData
            )
        }
        return nil
    }
}

public struct SHGenericEncryptedAsset : SHEncryptedAsset {
    public let globalIdentifier: String
    public let localIdentifier: String?
    public let creationDate: Date?
    public let groupId: String
    public let encryptedVersions: [SHEncryptedAssetVersion]
    
    public init(globalIdentifier: String,
                localIdentifier: String?,
                creationDate: Date?,
                groupId: String,
                encryptedVersions: [SHEncryptedAssetVersion]) {
        self.globalIdentifier = globalIdentifier
        self.localIdentifier = localIdentifier
        self.creationDate = creationDate
        self.groupId = groupId
        self.encryptedVersions = encryptedVersions
    }
    
    public static func fromDicts(_ dicts: [[String: Any]]) throws -> [String: SHEncryptedAsset] {
        var encryptedAssetById = [String: SHEncryptedAsset]()
        
        for dict in dicts {
            guard let assetIdentifier = dict["assetIdentifier"] as? String else {
                log.critical("could not deserialize local asset from dictionary=\(dict). Couldn't find assetIdentifier key")
                throw SHBackgroundOperationError.unexpectedData(dict)
            }
            
            guard let version = SHGenericEncryptedAssetVersion.fromDict(dict) else {
                log.critical("could not deserialize asset version information from dictionary=\(dict)")
                throw SHBackgroundOperationError.unexpectedData(dict)
            }
            
            if let existing = encryptedAssetById[assetIdentifier] {
                var versions = existing.encryptedVersions
                versions.append(version)
                let encryptedAsset = SHGenericEncryptedAsset(
                    globalIdentifier: existing.globalIdentifier,
                    localIdentifier: existing.localIdentifier,
                    creationDate: existing.creationDate,
                    groupId: existing.groupId,
                    encryptedVersions: versions
                )
                encryptedAssetById[assetIdentifier] = encryptedAsset
            }
            else if let assetIdentifier = dict["assetIdentifier"] as? String,
                    let phAssetIdentifier = dict["applePhotosAssetIdentifier"] as? String?,
                    let creationDate = dict["creationDate"] as? Date?,
                    let groupId = dict["groupId"] as? String
            {
                let encryptedAsset = SHGenericEncryptedAsset(
                    globalIdentifier: assetIdentifier,
                    localIdentifier: phAssetIdentifier,
                    creationDate: creationDate,
                    groupId: groupId,
                    encryptedVersions: [version]
                )
                encryptedAssetById[assetIdentifier] = encryptedAsset
            } else {
                log.critical("could not deserialize asset information from dictionary=\(dict)")
                throw SHBackgroundOperationError.unexpectedData(dict)
            }
        }
        
        return encryptedAssetById
    }
}

public struct SHGenericShareableEncryptedAssetVersion : SHShareableEncryptedAssetVersion {
    public let quality: SHAssetQuality
    public let userPublicIdentifier: String
    public let encryptedSecret: Data
    public let ephemeralPublicKey: Data
    public let publicSignature: Data
    
    public init(quality: SHAssetQuality,
                userPublicIdentifier: String,
                encryptedSecret: Data,
                ephemeralPublicKey: Data,
                publicSignature: Data) {
        self.quality = quality
        self.userPublicIdentifier = userPublicIdentifier
        self.encryptedSecret = encryptedSecret
        self.ephemeralPublicKey = ephemeralPublicKey
        self.publicSignature = publicSignature
    }
}
    

public struct SHGenericShareableEncryptedAsset : SHShareableEncryptedAsset {
    public let globalIdentifier: String
    public let sharedVersions: [SHShareableEncryptedAssetVersion]
    public let groupId: String

    public init(globalIdentifier: String,
                sharedVersions: [SHShareableEncryptedAssetVersion],
                groupId: String) {
        self.globalIdentifier = globalIdentifier
        self.sharedVersions = sharedVersions
        self.groupId = groupId
    }
}


extension SHLocalUser {
    func decrypt(_ asset: SHEncryptedAsset, quality: SHAssetQuality, receivedFrom user: SHServerUser) throws -> SHDecryptedAsset {
        guard let version = asset.encryptedVersions.first(where: { $0.quality == quality }) else {
            throw SHBackgroundOperationError.fatalError("No such version \(quality.rawValue) for asset=\(asset.globalIdentifier)")
        }
        
        let sharedSecret = SHShareablePayload(
            ephemeralPublicKeyData: version.publicKeyData,
            cyphertext: version.encryptedSecret,
            signature: version.publicSignatureData
        )
        let decryptedData = try self.decrypted(
            data: version.encryptedData,
            encryptedSecret: sharedSecret,
            receivedFrom: user
        )
        return SHGenericDecryptedAsset(
            globalIdentifier: asset.globalIdentifier,
            localIdentifier: asset.localIdentifier,
            decryptedData: decryptedData,
            creationDate: asset.creationDate
        )
    }
}

