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
    
    let assetsRemovedOnServer: [SHRemoteAssetIdentifier]
    let stateDifferentOnServer: [AssetVersionState]
    let userIdsToRemoveFromGroup: [String: Set<UserIdentifier>]
    let userIdsToAddToSharesOfAssetGid: [GlobalIdentifier: ShareSenderReceivers]
    let userIdsToRemoveToSharesOfAssetGid: [GlobalIdentifier: ShareSenderReceivers]
    
    
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
    static func generateUsing(server serverDescriptors: [any SHAssetDescriptor],
                              local localDescriptors: [any SHAssetDescriptor],
                              serverUserIds: [String],
                              localUserIds: [String],
                              for user: SHAuthenticatedLocalUser) -> AssetDescriptorsDiff {
        var onlyLocalAssets = localDescriptors
            .map({
                d in SHRemoteAssetIdentifier(globalIdentifier: d.globalIdentifier,
                                             localIdentifier: d.localIdentifier)
            })
            .subtract(
                serverDescriptors.map({
                    d in SHRemoteAssetIdentifier(globalIdentifier: d.globalIdentifier,
                                                 localIdentifier: d.localIdentifier)
                })
            )
        
        if onlyLocalAssets.count > 0 {
            for localDescriptor in localDescriptors {
                let assetRef = SHRemoteAssetIdentifier(globalIdentifier: localDescriptor.globalIdentifier,
                                                       localIdentifier: localDescriptor.localIdentifier)
                if let index = onlyLocalAssets.firstIndex(of: assetRef) {
                    switch localDescriptor.uploadState {
                    case .notStarted, .partial:
                        ///
                        /// Assets and its details (like the secrets) are stored locally at encryption time.
                        /// As this method can be called while an upload happens, all assets whose sender is this user,
                        /// that are not on the server yet, but are in the local server with state `.notStarted`,
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
        
        let userIdsToRemove = localUserIds.subtract(serverUserIds)
        
        var userIdsToRemoveFromGroup = [String: Set<UserIdentifier>]()
        for localDescriptor in localDescriptors {
            let userIdByGroup = localDescriptor.sharingInfo.sharedWithUserIdentifiersInGroup
            for (userId, groupId) in userIdByGroup.filter({ userIdsToRemove.contains($0.key) }) {
                if userIdsToRemoveFromGroup[groupId] == nil {
                    userIdsToRemoveFromGroup[groupId] = [userId]
                } else {
                    userIdsToRemoveFromGroup[groupId]!.insert(userId)
                }
            }
        }
        
        var userIdsToAddToSharesByAssetGid = [GlobalIdentifier: ShareSenderReceivers]()
        var userIdsToRemoveFromSharesByAssetGid = [GlobalIdentifier: ShareSenderReceivers]()
        for serverDescriptor in serverDescriptors {
            /// 
            /// If the descriptor exists on local, check if all users are listed in the share info
            /// If any user is missing add to the `userIdsToAddToSharesByAssetGid` portion of the diff
            /// a list of dictionaries `globalIdentifier` -> `(from: sender, to: recipients)`
            /// for all the missing `recipients`
            ///
            if let localDescriptor = localDescriptors.filter({ $0.globalIdentifier == serverDescriptor.globalIdentifier }).first {
                for (userId, groupId) in serverDescriptor.sharingInfo.sharedWithUserIdentifiersInGroup {
                    if localDescriptor.sharingInfo.sharedWithUserIdentifiersInGroup[userId] == nil {
                        if userIdsToAddToSharesByAssetGid[localDescriptor.globalIdentifier] == nil {
                            userIdsToAddToSharesByAssetGid[localDescriptor.globalIdentifier] = (
                                from: localDescriptor.sharingInfo.sharedByUserIdentifier,
                                groupIdByRecipientId: [userId: groupId],
                                groupInfoById: [groupId: SHGenericAssetGroupInfo(
                                    name: localDescriptor.sharingInfo.groupInfoById[groupId]!.name,
                                    createdAt: localDescriptor.sharingInfo.groupInfoById[groupId]!.createdAt
                                )]
                            )
                        } else {
                            var newDict = userIdsToAddToSharesByAssetGid[localDescriptor.globalIdentifier]!.groupIdByRecipientId
                            newDict[userId] = groupId
                            var newGroupInfoDict = userIdsToAddToSharesByAssetGid[localDescriptor.globalIdentifier]!.groupInfoById
                            newGroupInfoDict[groupId] = SHGenericAssetGroupInfo(
                                name: localDescriptor.sharingInfo.groupInfoById[groupId]!.name,
                                createdAt: localDescriptor.sharingInfo.groupInfoById[groupId]!.createdAt
                            )
                            userIdsToAddToSharesByAssetGid[localDescriptor.globalIdentifier] = (
                                from: localDescriptor.sharingInfo.sharedByUserIdentifier,
                                groupIdByRecipientId: newDict,
                                groupInfoById: newGroupInfoDict
                            )
                        }
                    }
                }
                
                /*
                for (userId, groupId) in localDescriptor.sharingInfo.sharedWithUserIdentifiersInGroup {
                    if serverDescriptor.sharingInfo.sharedWithUserIdentifiersInGroup[userId] == nil {
                        if userIdsToRemoveFromSharesByAssetGid[localDescriptor.globalIdentifier] == nil {
                            userIdsToRemoveFromSharesByAssetGid[localDescriptor.globalIdentifier] = (
                                from: localDescriptor.sharingInfo.sharedByUserIdentifier,
                                groupIdByRecipientId: [userId: groupId]
                            )
                        } else {
                            var newDict = userIdsToRemoveFromSharesByAssetGid[localDescriptor.globalIdentifier]!.groupIdByRecipientId
                            newDict[userId] = groupId
                            userIdsToRemoveFromSharesByAssetGid[localDescriptor.globalIdentifier] = (
                                from: localDescriptor.sharingInfo.sharedByUserIdentifier,
                                groupIdByRecipientId: newDict
                            )
                        }
                    }
                }
                */
            }
        }
        
        // TODO: Handle missing cases
        /// - Upload state changes?

        return AssetDescriptorsDiff(
            assetsRemovedOnServer: onlyLocalAssets,
            stateDifferentOnServer: [],
            userIdsToRemoveFromGroup: userIdsToRemoveFromGroup,
            userIdsToAddToSharesOfAssetGid: userIdsToAddToSharesByAssetGid,
            userIdsToRemoveToSharesOfAssetGid: userIdsToRemoveFromSharesByAssetGid
        )
    }
}
