import Foundation

/// Safehill Server description of an asset
public struct SHServerAsset : Codable {
    public let globalIdentifier: GlobalIdentifier
    public let localIdentifier: LocalIdentifier?
    public let perceptualHash: PerceptualHash?
    public let createdBy: UserIdentifier
    public let creationDate: Date?
    public let groupId: String
    public let versions: [SHServerAssetVersion]
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        
        globalIdentifier = try container.decode(String.self, forKey: .globalIdentifier)
        localIdentifier = try? container.decode(String.self, forKey: .localIdentifier)
        perceptualHash = try? container.decode(String.self, forKey: .perceptualHash)
        createdBy = try container.decode(String.self, forKey: .createdBy)
        let dateString = try container.decode(String.self, forKey: .creationDate)
        creationDate = dateString.iso8601withFractionalSeconds
        groupId = try container.decode(String.self, forKey: .groupId)
        versions = try container.decode([SHServerAssetVersion].self, forKey: .versions)
    }
    
    public init(globalIdentifier: GlobalIdentifier,
                localIdentifier: LocalIdentifier?,
                perceptualHash: PerceptualHash?,
                createdBy: UserIdentifier,
                creationDate: Date?,
                groupId: String,
                versions: [SHServerAssetVersion]) {
        self.globalIdentifier = globalIdentifier
        self.localIdentifier = localIdentifier
        self.perceptualHash = perceptualHash
        self.createdBy = createdBy
        self.creationDate = creationDate
        self.groupId = groupId
        self.versions = versions
    }
}
