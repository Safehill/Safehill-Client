import Foundation

public protocol SHAssetSyncingDelegate: SHInboundAssetOperationDelegate {
    
    /// The list of asset descriptors fetched from the server, filtering out what's already available locally (based on the 2 methods above)
    /// - Parameter descriptors: the descriptors fetched from local server
    /// - Parameter users: the `SHServerUser` objects for user ids mentioned in the descriptors
    /// - Parameter completionHandler: called when handling is complete
    func didReceiveLocalAssetDescriptors(_ descriptors: [any SHAssetDescriptor],
                                         referencing users: [UserIdentifier: any SHServerUser])
    
    /// The list of asset descriptors fetched from the server, filtering out what's already available locally (based on the 2 methods above)
    /// - Parameter descriptors: the descriptors fetched from remote server
    /// - Parameter users: the `SHServerUser` objects for user ids mentioned in the descriptors
    /// - Parameter completionHandler: called when handling is complete
    func didReceiveRemoteAssetDescriptors(_ descriptors: [any SHAssetDescriptor],
                                          referencing users: [UserIdentifier: any SHServerUser])
    
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
