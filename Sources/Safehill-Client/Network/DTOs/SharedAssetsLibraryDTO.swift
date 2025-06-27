import Foundation

public struct SharedAssetsLibraryDTO: Codable {
    /**
     * The assets shared explicily in a ConversationThread
     */
    public let photoMessages: [AssetGroupLinkageDTO]
    /**
     * The assets shared with the users currently in a ConversationThread
     */
    public let otherAssets: [AssetRefDTO]
    
    enum CodingKeys: String, CodingKey {
        case photoMessages
        case otherAssets
    }
    
    init(
        photoMessages: [AssetGroupLinkageDTO],
        otherAssets: [AssetRefDTO]
    ) {
        self.photoMessages = photoMessages
        self.otherAssets = otherAssets
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        photoMessages = try container.decode([AssetGroupLinkageDTO].self, forKey: .photoMessages)
        otherAssets = try container.decode([AssetRefDTO].self, forKey: .otherAssets)
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        
        try container.encode(photoMessages, forKey: .photoMessages)
        try container.encode(otherAssets, forKey: .otherAssets)
    }
}
