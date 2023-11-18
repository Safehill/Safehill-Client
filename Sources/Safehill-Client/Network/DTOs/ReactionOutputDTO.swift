import Foundation


public struct ReactionOutputDTO {
    private let _interactionId: String
    private let _senderUserIdentifier: String
    public let inReplyToAssetGlobalIdentifier: String?
    public let inReplyToInteractionId: String?
    public let reactionType: ReactionType
    private let _addedAt: String // ISO8601 formatted datetime
    
    init(interactionId: String,
         senderUserIdentifier: String,
         inReplyToAssetGlobalIdentifier: String?,
         inReplyToInteractionId: String?,
         reactionType: ReactionType,
         addedAt: String) {
        self._interactionId = interactionId
        self._senderUserIdentifier = senderUserIdentifier
        self.inReplyToAssetGlobalIdentifier = inReplyToAssetGlobalIdentifier
        self.inReplyToInteractionId = inReplyToInteractionId
        self.reactionType = reactionType
        self._addedAt = addedAt
    }
}

extension ReactionOutputDTO: ReactionInput {
    public var interactionId: String? {
        self._interactionId
    }
    
    public var senderUserIdentifier: String? {
        self._senderUserIdentifier
    }
    
    public var addedAt: String? {
        self._addedAt
    }
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
        _interactionId = try container.decode(String.self, forKey: .interactionId)
        _senderUserIdentifier = try container.decode(String.self, forKey: .senderUserIdentifier)
        inReplyToInteractionId = try? container.decode(String?.self, forKey: .inReplyToInteractionId)
        inReplyToAssetGlobalIdentifier = try? container.decode(String?.self, forKey: .inReplyToAssetGlobalIdentifier)
        if let reactionType = ReactionType(rawValue: try container.decode(Int.self, forKey: .reactionType)) {
            self.reactionType = reactionType
        } else {
            throw DecodingError.dataCorruptedError(forKey: .reactionType, in: container, debugDescription: "reaction type could not be serialized")
        }
        _addedAt = try container.decode(String.self, forKey: .addedAt)
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(_interactionId, forKey: .interactionId)
        try container.encode(_senderUserIdentifier, forKey: .senderUserIdentifier)
        try container.encode(inReplyToInteractionId, forKey: .inReplyToInteractionId)
        try container.encode(inReplyToAssetGlobalIdentifier, forKey: .inReplyToAssetGlobalIdentifier)
        try container.encode(reactionType.rawValue, forKey: .reactionType)
        try container.encode(_addedAt, forKey: .addedAt)
    }
}
