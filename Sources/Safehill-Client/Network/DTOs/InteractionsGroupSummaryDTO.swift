
public struct InteractionsGroupSummaryDTO: Codable {
    public let numComments: Int
    public let firstEncryptedMessage: MessageOutputDTO?
    public let reactions: [ReactionOutputDTO]
    public let invitedUsersPhoneNumbers: [String: String]
    
    enum CodingKeys: String, CodingKey {
        case numComments
        case firstEncryptedMessage
        case reactions
        case invitedUsersPhoneNumbers
    }
    
    init(numComments: Int,
         firstEncryptedMessage: MessageOutputDTO?,
         reactions: [ReactionOutputDTO],
         invitedUsersPhoneNumbers: [String: String]) {
        self.numComments = numComments
        self.firstEncryptedMessage = firstEncryptedMessage
        self.reactions = reactions
        self.invitedUsersPhoneNumbers = invitedUsersPhoneNumbers
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        numComments = try container.decode(Int.self, forKey: .numComments)
        firstEncryptedMessage = try? container.decode(MessageOutputDTO?.self, forKey: .firstEncryptedMessage)
        reactions = try container.decode([ReactionOutputDTO].self, forKey: .reactions)
        invitedUsersPhoneNumbers = try container.decode([String: String].self, forKey: .invitedUsersPhoneNumbers)
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        
        try container.encode(reactions, forKey: .reactions)
        try container.encode(firstEncryptedMessage, forKey: .firstEncryptedMessage)
        try container.encode(numComments, forKey: .numComments)
        try container.encode(invitedUsersPhoneNumbers, forKey: .invitedUsersPhoneNumbers)
    }
}
