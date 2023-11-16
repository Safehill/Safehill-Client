import Foundation

public protocol MessageOutput {
    var interactionId: String { get }
    var senderUserIdentifier: String { get }
    var inReplyToAssetGlobalIdentifier: String? { get }
    var inReplyToInteractionId: String? { get }
    var createdAt: String { get } // ISO8601 formatted datetime
}

public struct MessageOutputDTO: MessageOutput {
    public let interactionId: String
    public let senderUserIdentifier: String
    public let inReplyToAssetGlobalIdentifier: String?
    public let inReplyToInteractionId: String?
    public let encryptedMessage: String // base64EncodedData with the cipher
    public let createdAt: String // ISO8601 formatted datetime
}

public struct DecryptedMessageOutputDTO: MessageOutput {
    public let interactionId: String
    public let senderUserIdentifier: String
    public let inReplyToAssetGlobalIdentifier: String?
    public let inReplyToInteractionId: String?
    public let decryptedMessage: String // base64EncodedData with the cipher
    public let createdAt: String // ISO8601 formatted datetime
}


// - MARK: SERDE

extension MessageOutputDTO: Codable {
    enum CodingKeys: String, CodingKey {
        case interactionId
        case senderUserIdentifier
        case inReplyToAssetGlobalIdentifier
        case inReplyToInteractionId
        case encryptedMessage
        case createdAt
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        interactionId = try container.decode(String.self, forKey: .interactionId)
        senderUserIdentifier = try container.decode(String.self, forKey: .senderUserIdentifier)
        inReplyToInteractionId = try container.decode(String?.self, forKey: .inReplyToInteractionId)
        inReplyToAssetGlobalIdentifier = try container.decode(String?.self, forKey: .inReplyToAssetGlobalIdentifier)
        encryptedMessage = try container.decode(String.self, forKey: .encryptedMessage)
        createdAt = try container.decode(String.self, forKey: .createdAt)
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(interactionId, forKey: .interactionId)
        try container.encode(senderUserIdentifier, forKey: .senderUserIdentifier)
        try container.encode(inReplyToInteractionId, forKey: .inReplyToInteractionId)
        try container.encode(inReplyToAssetGlobalIdentifier, forKey: .inReplyToAssetGlobalIdentifier)
        try container.encode(encryptedMessage, forKey: .encryptedMessage)
        try container.encode(createdAt, forKey: .createdAt)
    }
}
