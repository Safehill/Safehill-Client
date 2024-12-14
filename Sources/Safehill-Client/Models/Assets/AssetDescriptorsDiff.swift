import Foundation

typealias GroupInfoDiff = (
    groupInfo: SHAssetGroupInfo,
    descriptorByAssetId: [GlobalIdentifier: any SHAssetDescriptor]
)

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
    let groupInfoDifferentOnRemote: [String: GroupInfoDiff]
    let groupInfoRemovedOnRemote: [String]
    let userGroupChangesByAssetGid: [GlobalIdentifier: any SHDescriptorSharingInfo]
    let userGroupRemovalsByAssetGid: [GlobalIdentifier: [UserIdentifier: [String]]]
    
    
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
        
        ///
        /// STEP 1
        /// Remove descriptors for COMPLETED assets that are only local
        ///
        
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
        
        var userGroupChangesByAssetGid = [GlobalIdentifier: any SHDescriptorSharingInfo]()
        var userGroupRemovalsByAssetGid = [GlobalIdentifier: [UserIdentifier: [String]]]()
        var groupInfoToUpdate = [String: GroupInfoDiff]()
        var groupIdsToRemove = Set<String>()
        
        ///
        /// STEP 2
        /// - Determine all the user-group mapping changes
        /// - Determine all the groupInfo changes
        ///
        
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
            
            for (userId, groupIds) in remoteDescriptor.sharingInfo.groupIdsByRecipientUserIdentifier {
                if Set(correspondingLocalDescriptor.sharingInfo.groupIdsByRecipientUserIdentifier[userId] ?? []) != Set(groupIds) {
                    if userGroupChangesByAssetGid[correspondingLocalDescriptor.globalIdentifier] == nil
                    {
                        userGroupChangesByAssetGid[remoteDescriptor.globalIdentifier] = SHGenericDescriptorSharingInfo(
                            sharedByUserIdentifier: remoteDescriptor.sharingInfo.sharedByUserIdentifier,
                            groupIdsByRecipientUserIdentifier: [userId: groupIds],
                            groupInfoById: remoteDescriptor.sharingInfo.groupInfoById
                        )
                    } else {
                        var newSharingInfo = userGroupChangesByAssetGid[remoteDescriptor.globalIdentifier]!.groupIdsByRecipientUserIdentifier
                        newSharingInfo[userId] = groupIds
                        userGroupChangesByAssetGid[remoteDescriptor.globalIdentifier] = SHGenericDescriptorSharingInfo(
                            sharedByUserIdentifier: remoteDescriptor.sharingInfo.sharedByUserIdentifier,
                            groupIdsByRecipientUserIdentifier: newSharingInfo,
                            groupInfoById: remoteDescriptor.sharingInfo.groupInfoById
                        )
                    }
                }
            }
            
            for (groupId, groupInfo) in remoteDescriptor.sharingInfo.groupInfoById {
                if let localGroupInfo = correspondingLocalDescriptor.sharingInfo.groupInfoById[groupId] {
                    if Int(localGroupInfo.createdAt?.timeIntervalSince1970 ?? 0) == Int(groupInfo.createdAt?.timeIntervalSince1970 ?? 0),
                       localGroupInfo.createdBy == groupInfo.createdBy,
                       localGroupInfo.encryptedTitle == groupInfo.encryptedTitle,
                       localGroupInfo.createdFromThreadId == groupInfo.createdFromThreadId,
                       localGroupInfo.permissions == groupInfo.permissions,
                       Set((localGroupInfo.invitedUsersPhoneNumbers ?? [:]).keys)
                        == Set((groupInfo.invitedUsersPhoneNumbers ?? [:]).keys)
                    {
                        // They are the same
                    } else {
                        if groupInfoToUpdate[groupId] != nil {
                            groupInfoToUpdate[groupId]?.descriptorByAssetId[remoteDescriptor.globalIdentifier] = remoteDescriptor
                        } else {
                            groupInfoToUpdate[groupId] = (
                                groupInfo: groupInfo,
                                descriptorByAssetId: [remoteDescriptor.globalIdentifier: remoteDescriptor]
                            )
                        }
                    }
                } else {
                    if groupInfoToUpdate[groupId] != nil {
                        groupInfoToUpdate[groupId]?.descriptorByAssetId[remoteDescriptor.globalIdentifier] = remoteDescriptor
                    } else {
                        groupInfoToUpdate[groupId] = (
                            groupInfo: groupInfo,
                            descriptorByAssetId: [remoteDescriptor.globalIdentifier: remoteDescriptor]
                        )
                    }
                }
            }
        }
        
        ///
        /// STEP 3
        /// - Determine all the groups that were removed on server
        /// - Determine all users removed from assets on server
        ///
        
        for localDescriptor in localDescriptors {
            guard localDescriptor.uploadState == .completed else {
                continue
            }
            
            guard let remoteDescriptor = remoteDescriptors.first(
                where: { $0.globalIdentifier == localDescriptor.globalIdentifier }
            ) else {
                continue
            }
            
            for (userId, groupIds) in localDescriptor.sharingInfo.groupIdsByRecipientUserIdentifier {
                if remoteDescriptor.sharingInfo.groupIdsByRecipientUserIdentifier[userId] == nil {
                    userGroupRemovalsByAssetGid[localDescriptor.globalIdentifier] = [userId: groupIds]
                }
            }
            
            for groupId in localDescriptor.sharingInfo.groupInfoById.keys {
                if remoteDescriptor.sharingInfo.groupInfoById[groupId] == nil {
                    groupIdsToRemove.insert(groupId)
                }
            }
        }
        
        // TODO: Handle missing cases
        /// - Upload state changes?

        return AssetDescriptorsDiff(
            assetsRemovedOnRemote: onlyLocalAssets,
            stateDifferentOnRemote: [],
            groupInfoDifferentOnRemote: groupInfoToUpdate,
            groupInfoRemovedOnRemote: Array(groupIdsToRemove),
            userGroupChangesByAssetGid: userGroupChangesByAssetGid,
            userGroupRemovalsByAssetGid: userGroupRemovalsByAssetGid
        )
    }
}
