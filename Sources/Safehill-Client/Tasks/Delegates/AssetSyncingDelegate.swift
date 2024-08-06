import Foundation

public protocol SHAssetSyncingDelegate: SHInboundAssetOperationDelegate {
    func assetIdsAreVisibleToUsers(_: [GlobalIdentifier: [SHServerUser]])
    
    func assetsWereDeleted(_ assets: [SHBackedUpAssetIdentifier])
    
    func usersWereAddedToShare(
        of: GlobalIdentifier,
        groupIdByRecipientId: [UserIdentifier: String],
        groupInfoById: [String: SHAssetGroupInfo]
    )
    
    func usersWereRemovedFromShare(
        of: GlobalIdentifier,
        groupIdByRecipientId: [UserIdentifier: String]
    )
    
    func groupsWereUpdated(withIds groupIds: [String])
    
    func groupsWereRemoved(withIds groupIds: [String])
}
