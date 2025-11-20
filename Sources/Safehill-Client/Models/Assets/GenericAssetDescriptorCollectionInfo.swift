import Foundation

public struct SHGenericAssetCollectionInfo : SHAssetCollectionInfo, Codable {
    public let collectionId: String
    public let collectionName: String
    public let visibility: String
    public let accessType: String
    public let addedAt: String
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        collectionId = try container.decode(String.self, forKey: .collectionId)
        collectionName = try container.decode(String.self, forKey: .collectionName)
        visibility = try container.decode(String.self, forKey: .visibility)
        accessType = try container.decode(String.self, forKey: .accessType)
        addedAt = try container.decode(String.self, forKey: .addedAt)
    }
    
    init(
        collectionId: String,
        collectionName: String,
        visibility: String,
        accessType: String,
        addedAt: String
    ) {
        self.collectionId = collectionId
        self.collectionName = collectionName
        self.visibility = visibility
        self.accessType = accessType
        self.addedAt = addedAt
    }
}
