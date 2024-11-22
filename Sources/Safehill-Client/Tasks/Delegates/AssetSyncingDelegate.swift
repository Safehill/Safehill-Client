import Foundation

public protocol SHAssetSyncingDelegate: SHInboundAssetOperationDelegate {
    func assetIdsAreVisibleToUsers(_: [GlobalIdentifier: [any SHServerUser]])
    
    func assetsWereDeleted(_ assets: [SHBackedUpAssetIdentifier])
    
    func groupUserSharingInfoChanged(
        forAssetWith globalIdentifier: GlobalIdentifier,
        sharingInfo: any SHDescriptorSharingInfo
    )
    
    func usersWereRemovedFromShare(
        of: GlobalIdentifier,
        _ userIdentifiersRemovedFromGroupId: [UserIdentifier: [String]]
    )
    
    func groupsInfoWereUpdated(_: [String: SHAssetGroupInfo])
    
    func groupsWereRemoved(withIds groupIds: [String])
}
