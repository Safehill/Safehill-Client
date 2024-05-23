
public struct InteractionsGroupSummaryDTO: Codable {
    let numComments: Int
    let firstEncryptedMessage: MessageOutputDTO?
    let reactions: [ReactionOutputDTO]
    
    enum CodingKeys: String, CodingKey {
        case numComments
        case firstEncryptedMessage
        case reactions
    }
    
    init(numComments: Int,
         firstEncryptedMessage: MessageOutputDTO?,
         reactions: [ReactionOutputDTO]) {
        self.numComments = numComments
        self.firstEncryptedMessage = firstEncryptedMessage
        self.reactions = reactions
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        numComments = try container.decode(Int.self, forKey: .numComments)
        firstEncryptedMessage = try container.decode(MessageOutputDTO?.self, forKey: .firstEncryptedMessage)
        reactions = try container.decode([ReactionOutputDTO].self, forKey: .reactions)
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        
        try container.encode(reactions, forKey: .reactions)
        try container.encode(firstEncryptedMessage, forKey: .firstEncryptedMessage)
        try container.encode(numComments, forKey: .numComments)
    }
}
