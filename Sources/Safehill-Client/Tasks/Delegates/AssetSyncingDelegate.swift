import Foundation

public protocol SHAssetSyncingDelegate: SHInboundAssetOperationDelegate {
    func assetIdsAreVisibleToUsers(_: [GlobalIdentifier: [any SHServerUser]])
    
    func assetsWereDeleted(_ assets: [SHBackedUpAssetIdentifier])
    
    func usersWereAddedToShare(
        of: GlobalIdentifier,
        groupIdsByRecipientId: [UserIdentifier: [String]],
        groupInfoById: [String: SHAssetGroupInfo]
    )
    
    func usersWereRemovedFromShare(
        of: GlobalIdentifier,
        groupIdsByRecipientId: [UserIdentifier: [String]]
    )
    
    func groupsInfoWereUpdated(_: [String: SHAssetGroupInfo])
    
    func groupsWereRemoved(withIds groupIds: [String])
}
