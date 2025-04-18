import Foundation

public protocol SHDescriptorSharingInfo {
    var sharedByUserIdentifier: UserIdentifier { get }
    /// Maps user public identifiers to asset group identifiers
    var groupIdsByRecipientUserIdentifier: [UserIdentifier: [String]] { get }
    var groupInfoById: [String: SHAssetGroupInfo] { get }
}

public extension SHDescriptorSharingInfo {
    func userSharingInfo(for userId: UserIdentifier) -> [SHAssetGroupInfo] {
        if let groupIds = self.groupIdsByRecipientUserIdentifier[userId] {
            return groupIds.compactMap { self.groupInfoById[$0] }
        }
        return []
    }
}
