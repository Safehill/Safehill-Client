import Foundation


public struct ReactionOutputDTO {
    let interactionId: String
    var senderUserIdentifier: String
    let inReplyToAssetGlobalIdentifier: String?
    let inReplyToInteractionId: String?
    let reactionType: ReactionType
    let addedAt: String // ISO8601 formatted datetime
}



// - MARK: SERDE

extension ReactionOutputDTO: Codable {
    enum CodingKeys: String, CodingKey {
        case interactionId
        case senderUserIdentifier
        case inReplyToAssetGlobalIdentifier
        case inReplyToInteractionId
        case reactionType
        case addedAt
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        interactionId = try container.decode(String.self, forKey: .interactionId)
        senderUserIdentifier = try container.decode(String.self, forKey: .senderUserIdentifier)
        inReplyToInteractionId = try container.decode(String?.self, forKey: .inReplyToInteractionId)
        inReplyToAssetGlobalIdentifier = try container.decode(String?.self, forKey: .inReplyToAssetGlobalIdentifier)
        if let reactionType = ReactionType(rawValue: try container.decode(Int.self, forKey: .reactionType)) {
            self.reactionType = reactionType
        } else {
            throw DecodingError.dataCorruptedError(forKey: .reactionType, in: container, debugDescription: "reaction type could not be serialized")
        }
        addedAt = try container.decode(String.self, forKey: .addedAt)
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(interactionId, forKey: .interactionId)
        try container.encode(senderUserIdentifier, forKey: .senderUserIdentifier)
        try container.encode(inReplyToInteractionId, forKey: .inReplyToInteractionId)
        try container.encode(inReplyToAssetGlobalIdentifier, forKey: .inReplyToAssetGlobalIdentifier)
        try container.encode(reactionType.rawValue, forKey: .reactionType)
        try container.encode(addedAt, forKey: .addedAt)
    }
}
