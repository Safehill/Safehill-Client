import Foundation

public struct AssetGroupLinkageDTO: Codable {
    public let globalIdentifier: GlobalIdentifier
    public let addedByUserIdentifier: String
    public let addedAt: String
    public let groupId: String
    
    public init(globalIdentifier: GlobalIdentifier, addedByUserIdentifier: String, addedAt: String, groupId: String) {
        self.globalIdentifier = globalIdentifier
        self.addedByUserIdentifier = addedByUserIdentifier
        self.addedAt = addedAt
        self.groupId = groupId
    }
}
