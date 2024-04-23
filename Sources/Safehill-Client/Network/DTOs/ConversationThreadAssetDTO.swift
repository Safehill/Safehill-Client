import Foundation

public struct ConversationThreadAssetDTO: Codable {
    public let globalIdentifier: String
    public let addedByUserIdentifier: String
    public let addedAt: String
    public let groupId: String
}

