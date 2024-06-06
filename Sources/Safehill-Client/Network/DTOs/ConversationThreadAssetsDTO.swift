import Foundation

public struct ConversationThreadAssetsDTO: Codable {
    public let photoMessages: [ConversationThreadAssetDTO]
    public let otherAssets: [UsersGroupAssetDTO]
    
    enum CodingKeys: String, CodingKey {
        case photoMessages
        case otherAssets
    }
    
    init(
        photoMessages: [ConversationThreadAssetDTO],
        otherAssets: [UsersGroupAssetDTO]
    ) {
        self.photoMessages = photoMessages
        self.otherAssets = otherAssets
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        photoMessages = try container.decode([ConversationThreadAssetDTO].self, forKey: .photoMessages)
        otherAssets = try container.decode([UsersGroupAssetDTO].self, forKey: .otherAssets)
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        
        try container.encode(photoMessages, forKey: .photoMessages)
        try container.encode(otherAssets, forKey: .otherAssets)
    }
}
