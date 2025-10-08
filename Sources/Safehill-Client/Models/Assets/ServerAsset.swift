import Foundation

/// Safehill Server description of an asset
public struct SHServerAsset : Codable {
    public let globalIdentifier: GlobalIdentifier
    public let localIdentifier: LocalIdentifier?
    public let createdBy: UserIdentifier
    public let creationDate: Date?
    public let versions: [SHServerAssetVersion]
    /// Indicates if this asset is publicly accessible without encryption
    var isPublic: Bool
    /// Public versions with direct access URLs (only present when isPublic = true)
    var publicVersions: [SHServerPublicAssetVersion]?
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        
        globalIdentifier = try container.decode(String.self, forKey: .globalIdentifier)
        localIdentifier = try? container.decode(String.self, forKey: .localIdentifier)
        createdBy = try container.decode(String.self, forKey: .createdBy)
        let dateString = try container.decode(String.self, forKey: .creationDate)
        creationDate = dateString.iso8601withFractionalSeconds
        versions = try container.decode([SHServerAssetVersion].self, forKey: .versions)
        isPublic = try container.decode(Bool.self, forKey: .isPublic)
        publicVersions = try container.decode([SHServerPublicAssetVersion].self, forKey: .publicVersions)
    }
    
    public init(globalIdentifier: GlobalIdentifier,
                localIdentifier: LocalIdentifier?,
                createdBy: UserIdentifier,
                creationDate: Date?,
                isPublic: Bool,
                versions: [SHServerAssetVersion],
                publicVersions: [SHServerPublicAssetVersion]?) {
        self.globalIdentifier = globalIdentifier
        self.localIdentifier = localIdentifier
        self.createdBy = createdBy
        self.creationDate = creationDate
        self.isPublic = isPublic
        self.versions = versions
        self.publicVersions = publicVersions
    }
}
