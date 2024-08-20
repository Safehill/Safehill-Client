import Foundation

typealias ShareSenderReceivers = (from: UserIdentifier, groupIdByRecipientId: [UserIdentifier: String], groupInfoById: [String: SHAssetGroupInfo])

///
/// Utility class to diff descriptors coming from `LocalServer` and `RemoteServer`
///
struct AssetDescriptorsDiff {
    
    ///
    /// In-memory representation of a state difference between two asset descriptors
    ///
    struct AssetVersionState {
        let globalIdentifier: String
        let localIdentifier: String
        let quality: SHAssetQuality
        let newUploadState: SHAssetDescriptorUploadState
    }
    
    let assetsRemovedOnRemote: [SHBackedUpAssetIdentifier]
    let stateDifferentOnRemote: [AssetVersionState]
    let groupInfoDifferentOnRemote: [String: SHAssetGroupInfo]
    let groupInfoRemovedOnRemote: [String]
    let userIdsAddedToTheShareOfAssetGid: [GlobalIdentifier: ShareSenderReceivers]
    let userIdsRemovedFromTheSharesOfAssetGid: [GlobalIdentifier: ShareSenderReceivers]
    
    
    ///
    /// Diffs the descriptors fetched from the server from the descriptors in the local cache.
    /// Handle the following cases:
    /// 1. The asset has been encrypted but not yet downloaded (so the server doesn't know about that asset yet)
    ///     -> needs to be kept as the user encryption secret is stored there
    /// 2. The descriptor exists on the server but not locally
    ///     -> It will be created locally, created in the ShareHistory or UploadHistory queue item by any DownloadOperation
    /// 3. The descriptor exists locally but not on the server
    ///     -> remove it as long as it's not case 1
    /// 4. The local upload state doesn't match the remote state
    ///     -> inefficient solution is to verify the asset is in S3. Efficient is to trust value on server
    ///
    /// - Parameters:
    ///   - server: the server descriptors to diff against
    ///   - local: the local descriptors
    /// - Returns: the diff
    ///
    static func generateUsing(remote remoteDescriptors: [any SHAssetDescriptor],
                              local localDescriptors: [any SHAssetDescriptor],
                              for user: SHAuthenticatedLocalUser) -> AssetDescriptorsDiff {
        /// Set(remote+local) - Set(local) = stale assets only present in the local server -> to remove
        var onlyLocalAssets = localDescriptors
            .map({
                d in SHBackedUpAssetIdentifier(
                    globalIdentifier: d.globalIdentifier,
                    localIdentifier: d.localIdentifier
                )
            })
            .subtract(
                remoteDescriptors.map({
                    d in SHBackedUpAssetIdentifier(
                        globalIdentifier: d.globalIdentifier,
                        localIdentifier: d.localIdentifier
                    )
                })
            )
        
        if onlyLocalAssets.count > 0 {
            for localDescriptor in localDescriptors {
                let assetRef = SHBackedUpAssetIdentifier(
                    globalIdentifier: localDescriptor.globalIdentifier,
                    localIdentifier: localDescriptor.localIdentifier
                )
                if let index = onlyLocalAssets.firstIndex(of: assetRef) {
                    switch localDescriptor.uploadState {
                    case .notStarted, .started, .partial:
                        ///
                        /// Assets and its details (like the secrets) are stored locally at encryption time.
                        /// As this method can be called while an upload happens, all assets whose sender is this user,
                        /// that are not on the server yet, but are in the local server with state `.started`,
                        /// are assumed to be assets that the user is uploading but didn't start or where the upload is in flight.
                        /// `.partial` will be returned for instance when the low resultion is marked as uploaded but the high res isn't.
                        /// These will make it to the server eventually, if no errors.
                        /// Do not mark them as removed
                        ///
                        if localDescriptor.sharingInfo.sharedByUserIdentifier == user.identifier {
                            onlyLocalAssets.remove(at: index)
                        }
                    case .failed:
                        ///
                        /// Assets can be recorded on device but not on server, when uploading/sharing fails.
                        /// They will actually be intentionally deleted from server when that happens,
                        /// but marked as failed locally
                        ///
                        onlyLocalAssets.remove(at: index)
                    default:
                        break
                    }
                }
            }
        }
        
        var userIdsToAddToSharesByAssetGid = [GlobalIdentifier: ShareSenderReceivers]()
        var userIdsToRemoveFromSharesByAssetGid = [GlobalIdentifier: ShareSenderReceivers]()
        var groupInfoToUpdate = [String: SHAssetGroupInfo]()
        var groupInfoToRemove = Set<String>()
        
        for remoteDescriptor in remoteDescriptors {
            guard remoteDescriptor.uploadState == .completed else {
                continue
            }
            
            ///
            /// If the descriptor exists on local, check if all users are listed in the share info
            /// If any user is missing add to the `userIdsToAddToSharesByAssetGid` portion of the diff
            /// a list of dictionaries `globalIdentifier` -> `(from:, groupIdByRecipientId:, groupInfoById:)`
            /// for all the missing `recipients`
            ///
            guard let correspondingLocalDescriptor = localDescriptors.first(
                where: { $0.globalIdentifier == remoteDescriptor.globalIdentifier }
            ) else {
                continue
            }
            
            for (userId, groupId) in remoteDescriptor.sharingInfo.sharedWithUserIdentifiersInGroup {
                if correspondingLocalDescriptor.sharingInfo.sharedWithUserIdentifiersInGroup[userId] == nil {
                    if userIdsToAddToSharesByAssetGid[correspondingLocalDescriptor.globalIdentifier] == nil 
                    {
                        let remoteDescGroupInfo = remoteDescriptor.sharingInfo.groupInfoById[groupId]
                        if remoteDescGroupInfo == nil {
                            log.warning("group info missing for group \(groupId) in descriptor of \(remoteDescriptor.globalIdentifier). sharedWithUserIdentifiersInGroup=\(remoteDescriptor.sharingInfo.sharedWithUserIdentifiersInGroup)")
                        }
                        userIdsToAddToSharesByAssetGid[correspondingLocalDescriptor.globalIdentifier] = (
                            from: correspondingLocalDescriptor.sharingInfo.sharedByUserIdentifier,
                            groupIdByRecipientId: [userId: groupId],
                            groupInfoById: [groupId: SHGenericAssetGroupInfo(
                                name: remoteDescGroupInfo?.name,
                                createdAt: remoteDescGroupInfo?.createdAt
                            )]
                        )
                    } else {
                        let remoteDescGroupInfo = remoteDescriptor.sharingInfo.groupInfoById[groupId]
                        if remoteDescGroupInfo == nil {
                            log.warning("group info missing for group \(groupId) in descriptor of \(remoteDescriptor.globalIdentifier). sharedWithUserIdentifiersInGroup=\(remoteDescriptor.sharingInfo.sharedWithUserIdentifiersInGroup)")
                        }
                        
                        var newDict = userIdsToAddToSharesByAssetGid[correspondingLocalDescriptor.globalIdentifier]!.groupIdByRecipientId
                        newDict[userId] = groupId
                        
                        var newGroupInfoDict = userIdsToAddToSharesByAssetGid[correspondingLocalDescriptor.globalIdentifier]!.groupInfoById
                        newGroupInfoDict[groupId] = SHGenericAssetGroupInfo(
                            name: newGroupInfoDict[groupId]?.name ?? remoteDescGroupInfo?.name,
                            createdAt: newGroupInfoDict[groupId]?.createdAt ?? remoteDescGroupInfo?.createdAt
                        )
                        
                        userIdsToAddToSharesByAssetGid[correspondingLocalDescriptor.globalIdentifier] = (
                            from: correspondingLocalDescriptor.sharingInfo.sharedByUserIdentifier,
                            groupIdByRecipientId: newDict,
                            groupInfoById: newGroupInfoDict
                        )
                    }
                }
            }
            
            for (groupId, groupInfo) in remoteDescriptor.sharingInfo.groupInfoById {
                if let localGroupInfo = correspondingLocalDescriptor.sharingInfo.groupInfoById[groupId] {
                    if localGroupInfo.createdAt == groupInfo.createdAt,
                       localGroupInfo.name == groupInfo.name {
                        // They are the same
                    } else {
                        groupInfoToUpdate[groupId] = groupInfo
                    }
                } else {
                    groupInfoToUpdate[groupId] = groupInfo
                }
            }
        }
        
        for localDescriptor in localDescriptors {
            guard localDescriptor.uploadState == .completed else {
                continue
            }
            
            guard let remoteDescriptor = remoteDescriptors.first(
                where: { $0.globalIdentifier == localDescriptor.globalIdentifier }
            ) else {
                continue
            }
            
            for (userId, groupId) in localDescriptor.sharingInfo.sharedWithUserIdentifiersInGroup {
                if remoteDescriptor.sharingInfo.sharedWithUserIdentifiersInGroup[userId] == nil {
                    if userIdsToRemoveFromSharesByAssetGid[localDescriptor.globalIdentifier] == nil 
                    {
                        userIdsToRemoveFromSharesByAssetGid[localDescriptor.globalIdentifier] = (
                            from: localDescriptor.sharingInfo.sharedByUserIdentifier,
                            groupIdByRecipientId: [userId: groupId],
                            groupInfoById: [groupId: SHGenericAssetGroupInfo(
                                name: remoteDescriptor.sharingInfo.groupInfoById[groupId]?.name,
                                createdAt: remoteDescriptor.sharingInfo.groupInfoById[groupId]?.createdAt
                            )]
                        )
                    } else {
                        var newDict = userIdsToRemoveFromSharesByAssetGid[localDescriptor.globalIdentifier]!.groupIdByRecipientId
                        newDict[userId] = groupId
                        
                        var newGroupInfoDict = userIdsToRemoveFromSharesByAssetGid[localDescriptor.globalIdentifier]!.groupInfoById
                        newGroupInfoDict[groupId] = SHGenericAssetGroupInfo(
                            name: remoteDescriptor.sharingInfo.groupInfoById[groupId]?.name,
                            createdAt: remoteDescriptor.sharingInfo.groupInfoById[groupId]?.createdAt
                        )
                        
                        userIdsToRemoveFromSharesByAssetGid[localDescriptor.globalIdentifier] = (
                            from: localDescriptor.sharingInfo.sharedByUserIdentifier,
                            groupIdByRecipientId: newDict,
                            groupInfoById: newGroupInfoDict
                        )
                    }
                }
            }
            
            for groupId in localDescriptor.sharingInfo.groupInfoById.keys {
                if remoteDescriptor.sharingInfo.groupInfoById[groupId] == nil {
                    groupInfoToRemove.insert(groupId)
                }
            }
        }
        
        // TODO: Handle missing cases
        /// - Upload state changes?

        return AssetDescriptorsDiff(
            assetsRemovedOnRemote: onlyLocalAssets,
            stateDifferentOnRemote: [],
            groupInfoDifferentOnRemote: groupInfoToUpdate,
            groupInfoRemovedOnRemote: Array(groupInfoToRemove),
            userIdsAddedToTheShareOfAssetGid: userIdsToAddToSharesByAssetGid,
            userIdsRemovedFromTheSharesOfAssetGid: userIdsToRemoveFromSharesByAssetGid
        )
    }
}
