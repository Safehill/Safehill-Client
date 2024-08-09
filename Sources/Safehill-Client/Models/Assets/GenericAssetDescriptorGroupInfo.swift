import Foundation

public struct SHGenericAssetGroupInfo : SHAssetGroupInfo, Codable {
    public let name: String?
    public let createdAt: Date?
    public let isPhotoMessageGroup: Bool
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        name = try? container.decode(String.self, forKey: .name)
        let dateString = try? container.decode(String.self, forKey: .createdAt)
        createdAt = dateString?.iso8601withFractionalSeconds
        let bool = try? container.decode(Bool.self, forKey: .isPhotoMessageGroup)
        isPhotoMessageGroup = bool ?? false
    }
    
    init(name: String?, createdAt: Date?, isPhotoMessageGroup: Bool) {
        self.name = name
        self.createdAt = createdAt
        self.isPhotoMessageGroup = isPhotoMessageGroup
    }
}
