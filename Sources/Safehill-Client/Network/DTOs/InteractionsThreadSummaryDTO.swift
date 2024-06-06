
public struct InteractionsThreadSummaryDTO: Codable {
    public let thread: ConversationThreadOutputDTO
    public let lastEncryptedMessage: MessageOutputDTO?
    public let numMessages: Int
    public let numAssets: Int
    
    enum CodingKeys: String, CodingKey {
        case thread
        case lastEncryptedMessage
        case numMessages
        case numAssets
    }
    
    init(
        thread: ConversationThreadOutputDTO,
        lastEncryptedMessage: MessageOutputDTO?,
        numMessages: Int,
        numAssets: Int
    ) {
        self.thread = thread
        self.lastEncryptedMessage = lastEncryptedMessage
        self.numMessages = numMessages
        self.numAssets = numAssets
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        thread = try container.decode(ConversationThreadOutputDTO.self, forKey: .thread)
        lastEncryptedMessage = try? container.decode(MessageOutputDTO?.self, forKey: .lastEncryptedMessage)
        numMessages = try container.decode(Int.self, forKey: .numMessages)
        numAssets = try container.decode(Int.self, forKey: .numAssets)
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        
        try container.encode(thread, forKey: .thread)
        try container.encode(lastEncryptedMessage, forKey: .lastEncryptedMessage)
        try container.encode(numMessages, forKey: .numMessages)
        try container.encode(numAssets, forKey: .numAssets)
    }
}
