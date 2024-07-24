import Foundation

public protocol SHDescriptorSharingInfo {
    var sharedByUserIdentifier: String { get }
    /// Maps user public identifiers to asset group identifiers
    var sharedWithUserIdentifiersInGroup: [UserIdentifier: String] { get }
    var groupInfoById: [String: SHAssetGroupInfo] { get }
}

public extension SHDescriptorSharingInfo {
    func userSharingInfo(for userId: String) -> SHAssetGroupInfo? {
        if let groupId = self.sharedWithUserIdentifiersInGroup[userId] {
            return self.groupInfoById[groupId]
        }
        return nil
    }
}
