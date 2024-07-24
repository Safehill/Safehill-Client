import Foundation

public struct SHGenericAssetGroupInfo : SHAssetGroupInfo, Codable {
    public let name: String?
    public let createdAt: Date?
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        name = try? container.decode(String?.self, forKey: .name)
        let dateString = try? container.decode(String?.self, forKey: .createdAt)
        createdAt = dateString?.iso8601withFractionalSeconds
    }
    
    init(name: String?, createdAt: Date?) {
        self.name = name
        self.createdAt = createdAt
    }
}
