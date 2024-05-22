
public struct InteractionsSummaryDTO: Codable {
    let summaryByThreadId: [String: InteractionsThreadSummaryDTO]
    let summaryByGroupId: [String: InteractionsGroupSummaryDTO]
    
    enum CodingKeys: String, CodingKey {
        case summaryByThreadId
        case summaryByGroupId
    }
    
    init(
        summaryByThreadId: [String: InteractionsThreadSummaryDTO],
        summaryByGroupId: [String: InteractionsGroupSummaryDTO]
    ) {
        self.summaryByThreadId = summaryByThreadId
        self.summaryByGroupId = summaryByGroupId
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        summaryByThreadId = try container.decode([String: InteractionsThreadSummaryDTO].self, forKey: .summaryByThreadId)
        summaryByGroupId = try container.decode([String: InteractionsGroupSummaryDTO].self, forKey: .summaryByGroupId)
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        
        try container.encode(summaryByThreadId, forKey: .summaryByThreadId)
        try container.encode(summaryByGroupId, forKey: .summaryByGroupId)
    }
}
