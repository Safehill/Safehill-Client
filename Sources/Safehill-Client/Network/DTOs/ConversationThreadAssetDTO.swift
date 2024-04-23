import Foundation

public struct ConversationThreadAssetDTO: Codable {
    public let globalIdentifier: String
    public let addedByUserIdentifier: String
    public let addedAt: String
    public let groupId: String
    
    public init(globalIdentifier: String, addedByUserIdentifier: String, addedAt: String, groupId: String) {
        self.globalIdentifier = globalIdentifier
        self.addedByUserIdentifier = addedByUserIdentifier
        self.addedAt = addedAt
        self.groupId = groupId
    }
}

