import Foundation


public struct MessageOutputDTO {
    private let _interactionId: String
    private let _senderPublicIdentifier: String
    public let inReplyToAssetGlobalIdentifier: String?
    public let inReplyToInteractionId: String?
    public let encryptedMessage: String // base64EncodedData with the cipher
    private let _createdAt: String // ISO8601 formatted datetime
    
    init(interactionId: String, 
         senderPublicIdentifier: String,
         inReplyToAssetGlobalIdentifier: String?,
         inReplyToInteractionId: String?,
         encryptedMessage: String,
         createdAt: String) {
        self._interactionId = interactionId
        self._senderPublicIdentifier = senderPublicIdentifier
        self.inReplyToAssetGlobalIdentifier = inReplyToAssetGlobalIdentifier
        self.inReplyToInteractionId = inReplyToInteractionId
        self.encryptedMessage = encryptedMessage
        self._createdAt = createdAt
    }
}

extension MessageOutputDTO: MessageInput {
    public var interactionId: String? {
        self._interactionId
    }

    public var senderPublicIdentifier: String? {
        self._senderPublicIdentifier
    }
    
    public var senderPublicSignature: String? {
        return nil
    }
    
    public var createdAt: String? {
        self._createdAt
    }
}

// - MARK: SERDE

extension MessageOutputDTO: Codable {
    enum CodingKeys: String, CodingKey {
        case interactionId
        case senderPublicIdentifier
        case inReplyToAssetGlobalIdentifier
        case inReplyToInteractionId
        case encryptedMessage
        case createdAt
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        _interactionId = try container.decode(String.self, forKey: .interactionId)
        _senderPublicIdentifier = try container.decode(String.self, forKey: .senderPublicIdentifier)
        inReplyToInteractionId = try? container.decode(String?.self, forKey: .inReplyToInteractionId)
        inReplyToAssetGlobalIdentifier = try? container.decode(String?.self, forKey: .inReplyToAssetGlobalIdentifier)
        encryptedMessage = try container.decode(String.self, forKey: .encryptedMessage)
        _createdAt = try container.decode(String.self, forKey: .createdAt)
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(_interactionId, forKey: .interactionId)
        try container.encode(_senderPublicIdentifier, forKey: .senderPublicIdentifier)
        try container.encode(inReplyToInteractionId, forKey: .inReplyToInteractionId)
        try container.encode(inReplyToAssetGlobalIdentifier, forKey: .inReplyToAssetGlobalIdentifier)
        try container.encode(encryptedMessage, forKey: .encryptedMessage)
        try container.encode(_createdAt, forKey: .createdAt)
    }
}
