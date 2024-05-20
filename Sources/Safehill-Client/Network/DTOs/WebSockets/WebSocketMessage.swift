import Foundation

public struct WebSocketMessage: Codable {
    
    enum MessageType: String, Codable {
        case message = "message"
        case reactionAdd = "reaction-add"
        case reactionRemove = "reaction-remove"
        case threadAdd = "thread-add"
    }
    
    let type: MessageType
    let content: String
    
    struct TextMessage: Codable {
        let interactionId: String?
        let anchorType: String
        let anchorId: String
        let inReplyToAssetGlobalIdentifier: String?
        let inReplyToInteractionId: String?
        let senderPublicIdentifier: String
        let senderPublicSignature: String // base64Encoded signature
        let encryptedMessage: String
        let sentAt: String // ISO8601 formatted datetime
    }

    struct Reaction: Codable {
        let interactionId: String?
        let anchorType: SHInteractionAnchor.RawValue // String
        let anchorId: String
        let inReplyToAssetGlobalIdentifier: String?
        let inReplyToInteractionId: String?
        let senderPublicIdentifier: String
        let reactionType: ReactionType.RawValue // Int
        let updatedAt: String // ISO8601 formatted datetime
    }
}