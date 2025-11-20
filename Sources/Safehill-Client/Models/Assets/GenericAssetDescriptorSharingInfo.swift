import Foundation

public struct SHGenericDescriptorSharingInfo : SHDescriptorSharingInfo, Codable {
    
    public let sharedByUserIdentifier: UserIdentifier
    public let groupIdsByRecipientUserIdentifier: [UserIdentifier: [String]]
    public let groupInfoById: [String: SHAssetGroupInfo]
    public let collectionInfoById: [String : any SHAssetCollectionInfo]
    
    enum CodingKeys: String, CodingKey {
        case sharedByUserIdentifier
        case groupIdsByRecipientUserIdentifier
        case groupInfoById
        case collectionInfoById
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(sharedByUserIdentifier, forKey: .sharedByUserIdentifier)
        try container.encode(groupIdsByRecipientUserIdentifier, forKey: .groupIdsByRecipientUserIdentifier)
        try container.encode(groupInfoById as! [String: SHGenericAssetGroupInfo], forKey: .groupInfoById)
        try container.encode(collectionInfoById as! [String: SHGenericAssetCollectionInfo], forKey: .collectionInfoById)
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        sharedByUserIdentifier = try container.decode(String.self, forKey: .sharedByUserIdentifier)
        groupIdsByRecipientUserIdentifier = try container.decode([String: [String]].self, forKey: .groupIdsByRecipientUserIdentifier)
        groupInfoById = try container.decode([String: SHGenericAssetGroupInfo].self, forKey: .groupInfoById)
        collectionInfoById = try container.decode([String: SHGenericAssetCollectionInfo].self, forKey: .collectionInfoById)
    }
    
    public init(sharedByUserIdentifier: UserIdentifier,
                groupIdsByRecipientUserIdentifier: [UserIdentifier: [String]],
                groupInfoById: [String: SHAssetGroupInfo],
                collectionInfoById: [String: SHAssetCollectionInfo]
    ) {
        self.sharedByUserIdentifier = sharedByUserIdentifier
        self.groupIdsByRecipientUserIdentifier = groupIdsByRecipientUserIdentifier
        self.groupInfoById = groupInfoById
        self.collectionInfoById = collectionInfoById
    }
}
