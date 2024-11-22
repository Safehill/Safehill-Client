
public struct InteractionsGroupSummaryDTO: Codable {
    public let numComments: Int
    public let encryptedTitle: String?
    public let reactions: [ReactionOutputDTO]
    public let invitedUsersPhoneNumbers: [String: String]
    
    enum CodingKeys: String, CodingKey {
        case numComments
        case encryptedTitle
        case reactions
        case invitedUsersPhoneNumbers
    }
    
    init(numComments: Int,
         encryptedTitle: String?,
         reactions: [ReactionOutputDTO],
         invitedUsersPhoneNumbers: [String: String]) {
        self.numComments = numComments
        self.encryptedTitle = encryptedTitle
        self.reactions = reactions
        self.invitedUsersPhoneNumbers = invitedUsersPhoneNumbers
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        numComments = try container.decode(Int.self, forKey: .numComments)
        encryptedTitle = try? container.decode(String.self, forKey: .encryptedTitle)
        reactions = try container.decode([ReactionOutputDTO].self, forKey: .reactions)
        invitedUsersPhoneNumbers = try container.decode([String: String].self, forKey: .invitedUsersPhoneNumbers)
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        
        try container.encode(reactions, forKey: .reactions)
        try container.encode(encryptedTitle, forKey: .encryptedTitle)
        try container.encode(numComments, forKey: .numComments)
        try container.encode(invitedUsersPhoneNumbers, forKey: .invitedUsersPhoneNumbers)
    }
}
