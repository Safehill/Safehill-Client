import Foundation

public struct SHGenericDescriptorSharingInfo : SHDescriptorSharingInfo, Codable {
    public let sharedByUserIdentifier: String
    public let sharedWithUserIdentifiersInGroup: [UserIdentifier: String]
    public let groupInfoById: [String: SHAssetGroupInfo]
    
    enum CodingKeys: String, CodingKey {
        case sharedByUserIdentifier
        case sharedWithUserIdentifiersInGroup
        case groupInfoById
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(sharedByUserIdentifier, forKey: .sharedByUserIdentifier)
        try container.encode(sharedWithUserIdentifiersInGroup, forKey: .sharedWithUserIdentifiersInGroup)
        try container.encode(groupInfoById as! [String: SHGenericAssetGroupInfo], forKey: .groupInfoById)
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        sharedByUserIdentifier = try container.decode(String.self, forKey: .sharedByUserIdentifier)
        sharedWithUserIdentifiersInGroup = try container.decode([String: String].self, forKey: .sharedWithUserIdentifiersInGroup)
        groupInfoById = try container.decode([String: SHGenericAssetGroupInfo].self, forKey: .groupInfoById)
    }
    
    public init(sharedByUserIdentifier: String,
                sharedWithUserIdentifiersInGroup: [String: String],
                groupInfoById: [String: SHAssetGroupInfo]) {
        self.sharedByUserIdentifier = sharedByUserIdentifier
        self.sharedWithUserIdentifiersInGroup = sharedWithUserIdentifiersInGroup
        self.groupInfoById = groupInfoById
    }
}
