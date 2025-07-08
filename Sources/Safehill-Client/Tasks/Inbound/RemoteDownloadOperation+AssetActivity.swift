import Foundation

extension SHRemoteDownloadOperation {
    
    /// Returns nil if the encrypted title is nil
    /// Throws if the decryption fails or some required details are missing
    /// - Parameters:
    ///   - groupInfo: the GroupInfo in the descriptor
    ///   - groupId: the group id
    /// - Returns: 
    private func decryptTitle(
        groupInfo: any SHAssetGroupInfo,
        groupId: String
    ) async throws -> String? {
        if let encryptedTitle = groupInfo.encryptedTitle,
           let groupCreatorPublicId = groupInfo.createdBy
        {
            let interactionsController = SHUserInteractionController(user: self.user)
            return try await interactionsController.decryptTitle(
                encryptedTitle: encryptedTitle,
                createdBy: groupCreatorPublicId,
                groupId: groupId
            )
        }
        
        return nil
    }
    
    private func buildGlobalGroupInfoMap(from descriptors: [any SHAssetDescriptor]) -> [String: any SHAssetGroupInfo] {
        var globalGroupInfoById: [String: any SHAssetGroupInfo] = [:]

        for asset in descriptors {
            let groupInfoMap = asset.sharingInfo.groupInfoById

            for (groupId, groupInfo) in groupInfoMap {
                if let existingInfo = globalGroupInfoById[groupId] {
#if DEBUG
                    guard
                        existingInfo.encryptedTitle == groupInfo.encryptedTitle,
                        existingInfo.createdBy == groupInfo.createdBy,
                        existingInfo.createdFromThreadId == groupInfo.createdFromThreadId,
                        existingInfo.permissions == groupInfo.permissions,
                        existingInfo.invitedUsersPhoneNumbers == groupInfo.invitedUsersPhoneNumbers
                    else {
                        assertionFailure("Inconsistent SHAssetGroupInfo for groupId: \(groupId)")
                        fatalError()
                    }
#endif
                } else {
                    globalGroupInfoById[groupId] = groupInfo
                }
            }
        }

        return globalGroupInfoById
    }
    
    private func computeSharingMaps(
        from descriptors: [any SHAssetDescriptor],
        usersDict: [UserIdentifier: any SHServerUser]
    ) -> (
        assetGIdsByGroupId: [String: [GlobalIdentifier]],
        sharedWithByGroupId: [String: [any SHServerUser]] // Use Set to avoid duplicates
    ) {
        var assetGIdsByGroupId: [String: Set<GlobalIdentifier>] = [:]
        var sharedWithByGroupId: [String: Set<UserIdentifier>] = [:]

        for asset in descriptors {
            let globalId = asset.globalIdentifier
            let groupMapping = asset.sharingInfo.groupIdsByRecipientUserIdentifier

            for (userId, groupIds) in groupMapping {
                for groupId in groupIds {
                    assetGIdsByGroupId[groupId, default: []].insert(globalId)
                    sharedWithByGroupId[groupId, default: []].insert(userId)
                }
            }
        }
        
        var usersByGroupId: [String: [any SHServerUser]] = [:]
        
        for (groupId, userIds) in sharedWithByGroupId {
            let users = userIds.compactMap { userId -> (any SHServerUser)? in
#if DEBUG
                guard let user = usersDict[userId] else {
                    assertionFailure("Missing user for userId: \(userId)")
                    return nil
                }
                return user
#else
                return usersDict[userId]
#endif
            }
            
            usersByGroupId[groupId] = users
        }

        return (assetGIdsByGroupId.mapValues({ Array($0) }), usersByGroupId)
    }
    
    private func createAssetActivities(
        assetGIdsByGroupId: [String: [GlobalIdentifier]],
        groupInfoById: [String: SHAssetGroupInfo],
        sharedWithByGroupId: [String: [any SHServerUser]],
        usersDict: [UserIdentifier: any SHServerUser]
    ) async -> [any AssetActivity] {
        
        var activities = [any AssetActivity]()
        
        for (groupId, groupInfo) in groupInfoById {
            guard let assetIds = assetGIdsByGroupId[groupId], !assetIds.isEmpty else {
                self.log.critical("[\(type(of: self))] no assets for groupId=\(groupId) even though a group with such id was referenced")
                continue
            }
            
            guard let createdBy = groupInfo.createdBy,
                    let groupSenderUser = usersDict[createdBy] else {
                self.log.critical("[\(type(of: self))] inconsistency between user ids referenced in descriptors and user objects returned from server. No user for id \(groupInfo.createdBy ?? "nil")")
                continue
            }
            
            let clearTitle: String?
            do {
                clearTitle = try await self.decryptTitle(groupInfo: groupInfo, groupId: groupId)
            } catch {
                self.log.critical("[\(type(of: self))] unable to decrypt title for groupId=\(groupId): \(error.localizedDescription)")
                continue
            }
            
            let sharedWithInThisGroup = sharedWithByGroupId[groupId] ?? []
            let permissions = GroupPermission(rawValue: groupInfo.permissions ?? GroupPermission.confidential.rawValue) ?? .confidential
            let shareInfo = sharedWithInThisGroup.map({ (with: $0, at: groupInfo.createdAt ?? Date()) })
            let invitationsInfo = (groupInfo.invitedUsersPhoneNumbers ?? [:]).compactMap({
                (key: String, value: String) -> (with: FormattedPhoneNumber, at: Date)? in
                guard let date = value.iso8601withFractionalSeconds else {
                    return nil
                }
                return (with: key, at: date)
            })
            
            let activity = GenericAssetActivity(
                assetIds: assetIds,
                groupId: groupId,
                groupTitle: clearTitle,
                groupPermissions: permissions,
                eventOriginator: groupSenderUser,
                shareInfo: shareInfo,
                invitationsInfo: invitationsInfo,
                asPhotoMessageInThreadId: groupInfo.createdFromThreadId
            )
            activities.append(activity)
        }
        
        return activities
    }
    
    ///
    /// Convert descriptors into `any AssetActivity` objects and call the `activitySyncingDelegates`.
    ///
    /// - Parameters:
    ///   - descriptorsByGlobalIdentifier: all the descriptors keyed by asset global identifier
    ///   - usersDict: the mapping user id <-> user
    ///
    internal func createAssetActivities(
        from descriptors: [any SHAssetDescriptor],
        usersDict: [UserIdentifier: any SHServerUser]
    ) async -> [any AssetActivity] {
        
        let maps = self.computeSharingMaps(from: descriptors, usersDict: usersDict)
        let globalGroupInfo = self.buildGlobalGroupInfoMap(from: descriptors)
        
        return await self.createAssetActivities(
            assetGIdsByGroupId: maps.assetGIdsByGroupId,
            groupInfoById: globalGroupInfo,
            sharedWithByGroupId: maps.sharedWithByGroupId,
            usersDict: usersDict
        )
    }
}
