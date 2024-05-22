
public struct InteractionsGroupSummaryDTO: Codable {
    let numComments: Int
    let reactions: [ReactionOutputDTO]
    
    enum CodingKeys: String, CodingKey {
        case numComments
        case reactions
    }
    
    init(numComments: Int, reactions: [ReactionOutputDTO]) {
        self.numComments = numComments
        self.reactions = reactions
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        numComments = try container.decode(Int.self, forKey: .numComments)
        reactions = try container.decode([ReactionOutputDTO].self, forKey: .reactions)
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        
        try container.encode(reactions, forKey: .reactions)
        try container.encode(numComments, forKey: .numComments)
    }
}
