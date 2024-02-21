import Foundation
import Safehill_Crypto

/// Safehill clients store 2 versions per asset, one low resolution for the thumbnail, one full size
public enum SHAssetQuality: String {
    case lowResolution = "low",
         midResolution = "mid",
         hiResolution = "hi"
    
    public static var all: [SHAssetQuality] {
        return [
            .lowResolution,
            .midResolution,
            .hiResolution
        ]
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
    var sharedWithUserIdentifiersInGroup: [UserIdentifier: String] { get }
    var groupInfoById: [String: SHAssetGroupInfo] { get }
}

public extension SHDescriptorSharingInfo {
    func userSharingInfo(for userId: String) -> SHAssetGroupInfo? {
        if let groupId = self.sharedWithUserIdentifiersInGroup[userId] {
            return self.groupInfoById[groupId]
        }
        return nil
    }
}

public struct SHGenericDescriptorSharingInfo : SHDescriptorSharingInfo, Codable {
    public let sharedByUserIdentifier: String
    public let sharedWithUserIdentifiersInGroup: [UserIdentifier: String]
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
    case notStarted = "not_started", partial = "partial", completed = "completed", failed = "failed"
}


public protocol SHRemoteAssetIdentifiable: Hashable {
    var globalIdentifier: String { get }
    var localIdentifier: String? { get }
}

public extension SHRemoteAssetIdentifiable {
    static func == (lhs: Self, rhs: Self) -> Bool {
        lhs.localIdentifier == rhs.localIdentifier
        && lhs.globalIdentifier == rhs.globalIdentifier
    }
    
    func hash(into hasher: inout Hasher) {
        hasher.combine(localIdentifier)
        hasher.combine(globalIdentifier)
    }
}

public struct SHRemoteAssetIdentifier: SHRemoteAssetIdentifiable {
    public let globalIdentifier: String
    public let localIdentifier: String?
 
    public init(globalIdentifier: String, localIdentifier: String?) {
        self.globalIdentifier = globalIdentifier
        self.localIdentifier = localIdentifier
    }
}

///
/// Safehill Server descriptor: metadata associated with an asset, such as creation date, sender and list of receivers
///
public protocol SHAssetDescriptor: SHRemoteAssetIdentifiable {
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
public protocol SHDecryptedAsset: SHRemoteAssetIdentifiable {
    var localIdentifier: String? { get set }
    var decryptedVersions: [SHAssetQuality: Data] { get set }
    var creationDate: Date? { get }
}

public struct SHGenericDecryptedAsset : SHDecryptedAsset {
    public let globalIdentifier: String
    public var localIdentifier: String?
    public var decryptedVersions: [SHAssetQuality: Data]
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
public protocol SHEncryptedAsset: SHRemoteAssetIdentifiable {
    var creationDate: Date? { get }
    var encryptedVersions: [SHAssetQuality: SHEncryptedAssetVersion] { get }
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
    var globalIdentifier: GlobalIdentifier { get }
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
    
    public static func fromDict(_ dict: [String: Any], data: Data?) -> SHEncryptedAssetVersion? {
        if let encryptedData = data,
           let qualityS = dict["quality"] as? String,
           let quality = SHAssetQuality(rawValue: qualityS),
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
    public let encryptedVersions: [SHAssetQuality: SHEncryptedAssetVersion]
    
    public init(globalIdentifier: String,
                localIdentifier: String?,
                creationDate: Date?,
                encryptedVersions: [SHAssetQuality: SHEncryptedAssetVersion]) {
        self.globalIdentifier = globalIdentifier
        self.localIdentifier = localIdentifier
        self.creationDate = creationDate
        self.encryptedVersions = encryptedVersions
    }
    
    /// Deserialize key-values coming from DB into `SHEncryptedAsset` objects
    /// - Parameter keyValues: the keys and values retrieved from DB
    /// - Returns: the `SHEncryptedAsset` objects, organized by assetIdentifier
    public static func fromDicts(_ keyValues: [String: [String: Any]]) throws -> [String: any SHEncryptedAsset] {
        var encryptedAssetById = [String: any SHEncryptedAsset]()
        
        ///
        /// Snoog 1.1.3 and earlier store the data along with the metadata.
        /// More precisely, the value under key '<quality>::<assetIdentifier>' stores both the `encryptedData` and metadata associated with it.
        /// Since Snoog 1.1.4, data metadata are stored under two different keys:
        /// 1. '<quality>::<assetIdentifier>' for the metadata
        /// 2. 'data::<quality>::<assetIdentifier>' for the data
        ///
        /// The `dicts` param will then have either `N * quality` values (for 1.1.3), or `N * quality * 2` values (for 1.1.4 and later)
        ///
        /// `assetDataByGlobalIdentifierAndQuality` is version-agnostic, and retrieves the data for each asset identifier and quality.
        ///
        var assetDataByGlobalIdentifierAndQuality = [String: [SHAssetQuality: Data]]()
        for (key, value) in keyValues {
            let keyComponents = key.components(separatedBy: "::")
            var quality: SHAssetQuality? = nil
            
            if keyComponents.count == 3, keyComponents.first == "data" {
                /// Snoog 1.1.4 and later
                quality = SHAssetQuality(rawValue: keyComponents[1])
            } else if keyComponents.count == 2 {
                /// Snoog 1.1.3 and earlier
                quality = SHAssetQuality(rawValue: keyComponents[0])
            }
                
            guard let quality = quality else {
                log.critical("failed to retrieve `quality` from key value object in the local asset store. Skipping")
                continue
            }
                
            if let identifier = value["assetIdentifier"] as? String,
               let data = value["encryptedData"] as? Data {
                if assetDataByGlobalIdentifierAndQuality[identifier] == nil {
                    assetDataByGlobalIdentifierAndQuality[identifier] = [quality: data]
                } else {
                    assetDataByGlobalIdentifierAndQuality[identifier]![quality] = data
                }
            }
        }
        
        for (key, dict) in keyValues {
            let keyComponents = key.components(separatedBy: "::")
            guard keyComponents.count == 2, dict.keys.count > 2 else {
                ///
                /// Because both elements keyed by `data::<quality>::identifier` (data) and  `<quality>::identifier`
                /// (data & metadata or just metadata for Snoog >= 1.4) are present in `dicts`, we can skip elements of `dicts` that
                /// refer to the data (`data::<quality>::<identifier>` keys). Those were already retreived in the
                /// `assetDataByGlobalIdentifierAndQuality` by the logic in the for statement above.
                ///
                continue
            }
            
            guard let quality = SHAssetQuality(rawValue: keyComponents.first ?? "") else {
                log.critical("failed to retrieve `quality` from key value object in the local asset store. Skipping")
                continue
            }
            
            guard let assetIdentifier = dict["assetIdentifier"] as? String else {
                log.critical("could not deserialize local asset from dictionary=\(dict). Couldn't find assetIdentifier key")
                throw SHBackgroundOperationError.unexpectedData(dict)
            }
            
            guard let data = assetDataByGlobalIdentifierAndQuality[assetIdentifier]?[quality],
                  let version = SHGenericEncryptedAssetVersion.fromDict(dict, data: data) else {
                log.critical("could not deserialize asset version information from dictionary=\(dict)")
                throw SHBackgroundOperationError.unexpectedData(dict)
            }
            
            if let existing = encryptedAssetById[assetIdentifier] {
                var versions = existing.encryptedVersions
                versions[version.quality] = version
                let encryptedAsset = SHGenericEncryptedAsset(
                    globalIdentifier: existing.globalIdentifier,
                    localIdentifier: existing.localIdentifier,
                    creationDate: existing.creationDate,
                    encryptedVersions: versions
                )
                encryptedAssetById[assetIdentifier] = encryptedAsset
            }
            else if let assetIdentifier = dict["assetIdentifier"] as? String,
                    let phAssetIdentifier = dict["applePhotosAssetIdentifier"] as? String?,
                    let creationDate = dict["creationDate"] as? Date?
            {
                let encryptedAsset = SHGenericEncryptedAsset(
                    globalIdentifier: assetIdentifier,
                    localIdentifier: phAssetIdentifier,
                    creationDate: creationDate,
                    encryptedVersions: [version.quality: version]
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

