import Foundation
import KnowledgeBase
import os


extension SHServerProxy {
    
    public func getUsers(inAssetDescriptors descriptors: [any SHAssetDescriptor]) throws -> [any SHServerUser] {
        let group = DispatchGroup()
        var response = [any SHServerUser]()
        var error: Error? = nil
        
        var userIdsSet = Set<String>()
        for descriptor in descriptors {
            userIdsSet.insert(descriptor.sharingInfo.sharedByUserIdentifier)
            descriptor.sharingInfo.sharedWithUserIdentifiersInGroup.keys.forEach({ userIdsSet.insert($0) })
        }
        userIdsSet.remove(self.remoteServer.requestor.identifier)
        let userIds = Array(userIdsSet)

        group.enter()
        self.remoteServer.getUsers(withIdentifiers: userIds) { result in
            switch result {
            case .success(let serverUsers):
                response = serverUsers
            case .failure(let err):
                log.error("failed to fetch users from server when calculating diff: \(err.localizedDescription)")
                error = err
            }
            group.leave()
        }
        
        let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultNetworkTimeoutInMilliseconds))
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
        guard error == nil else {
            throw error!
        }
        
        return response
    }
    
}

// MARK: - Sync Operation

public class SHSyncOperation: SHAbstractBackgroundOperation, SHBackgroundOperationProtocol {
    
    public let log = Logger(subsystem: "com.safehill", category: "BG-SYNC")
    
    let user: SHLocalUser
    
    let delegates: [SHAssetSyncingDelegate]
    
    private var serverProxy: SHServerProxy {
        SHServerProxy(user: self.user)
    }
    
    public init(user: SHLocalUser, delegates: [SHAssetSyncingDelegate]) {
        self.user = user
        self.delegates = delegates
    }
    
    public func clone() -> SHBackgroundOperationProtocol {
        SHSyncOperation(user: self.user, delegates: self.delegates)
    }
    
    /// Removes any evidence of the users from the local storage:
    /// - Replaces the items in the `ShareHistoryQueue` with the same items by omitting the users removed.
    /// - Removes the sharing information from the `assetsStore`
    ///
    /// Returns the queueItemIdentifiers replaced and the ones removed
    /// - Parameter userIdsToRemoveFromGroup: maps groupId -> list of user ids to remove
    /// - Returns: the list of keys changed and removed in the `SHShareHistoryQueue`
    ///
    private func removeUsersFromStores(_ userIdsToRemoveFromGroup: [String: Set<UserIdentifier>]) -> (changed: [String], removed: [String]) {
        
        guard let successfulShareQueue = try? BackgroundOperationQueue.of(type: .successfulShare) else {
            log.error("failed to connect to the successful share queue. users could not be removed from stores")
            return (changed: [], removed: [])
        }
        
        var oldShareHistoryItems = [String: (item: SHShareHistoryItem, timestamp: Date)]()
        
        do {
            var condition = KBGenericCondition(value: false)
            for groupId in userIdsToRemoveFromGroup.keys {
                condition = condition.or(KBGenericCondition(.contains, value: groupId))
            }
            let matchingShareHistoryItem = try successfulShareQueue.keyValuesAndTimestamps(forKeysMatching: condition)
            
            for kvpairWithTimestamp in matchingShareHistoryItem {
                guard let data = kvpairWithTimestamp.value as? Data else {
                    log.critical("Found unexpected data in the ShareHistoryQueue")
                    continue
                }
                
                let unarchiver: NSKeyedUnarchiver
                if #available(macOS 10.13, *) {
                    unarchiver = try NSKeyedUnarchiver(forReadingFrom: data)
                } else {
                    unarchiver = NSKeyedUnarchiver(forReadingWith: data)
                }
                
                guard let succesfulShareQueueItem = unarchiver.decodeObject(
                    of: SHShareHistoryItem.self,
                    forKey: NSKeyedArchiveRootObjectKey) else {
                    log.critical("Found undeserializable SHShareHistoryItem item in the ShareHistoryQueue")
                    continue
                }
                
                oldShareHistoryItems[kvpairWithTimestamp.key] = (
                    item: succesfulShareQueueItem,
                    timestamp: kvpairWithTimestamp.timestamp
                )
            }
        } catch {
            log.critical("Failed to fetch SHShareHistoryItem items in the ShareHistoryQueue with groupIds \(userIdsToRemoveFromGroup.keys)")
        }
        
        var (queueItemsChanged, queueItemsRemoved) = (Set<String>(), Set<String>())
        for (groupId, userIds) in userIdsToRemoveFromGroup {
            let matchShares = oldShareHistoryItems.filter({
                $0.value.item.groupId == groupId
                && $0.value.item.sharedWith.contains { userIds.contains($0.identifier) }
            })
            
            guard matchShares.count > 0 else {
                log.warning("Unable to retrieve item in ShareHistoryQueue with groupId=\(groupId) having users \(userIds)")
                continue
            }
            
            
            for share in matchShares {
                ///
                /// Remove asset sharing information
                ///
                for userId in userIds {
                    self.serverProxy.localServer.unshare(assetId: share.value.item.globalAssetId, with: userId) { result in
                        if case .failure(let error) = result {
                            self.log.error("some assets were unshared on server but not in the local cache. This operation will be attempted again, but for now the cache is out of sync. error=\(error.localizedDescription)")
                        }
                    }
                }
                
                ///
                /// Remove user from existing `SHShareHistoryItem`
                ///
                let newSharedWith = share.value.item.sharedWith.filter { !userIds.contains($0.identifier) }
                if newSharedWith.count > 0 {
                    let newShareHistoryItem = SHShareHistoryItem(
                        localAssetId: share.value.item.localIdentifier,
                        globalAssetId: share.value.item.globalAssetId,
                        versions: share.value.item.versions,
                        groupId: share.value.item.groupId,
                        eventOriginator: share.value.item.eventOriginator,
                        sharedWith: newSharedWith,
                        isBackground: share.value.item.isBackground
                    )
                    do {
                        try newShareHistoryItem.insert(in: successfulShareQueue, with: share.key, at: share.value.timestamp)
                        queueItemsChanged.insert(share.key)
                    } catch {
                        log.warning("failed to delete users \(userIds) from ShareHistoryItem for groupId \(groupId). This operation will be attempted again")
                    }
                } else {
                    do {
                        try successfulShareQueue.removeValue(for: share.key)
                        queueItemsRemoved.insert(share.key)
                    } catch {
                        log.warning("failed to delete item from ShareHistoryItem for groupId \(groupId). This operation will be attempted again")
                    }
                }
            }
        }
        
        return (changed: Array(queueItemsChanged), removed: Array(queueItemsRemoved))
    }
    
    private func syncDescriptors(
        _ remoteDescriptors: [any SHAssetDescriptor],
        completionHandler: @escaping (Result<AssetDescriptorsDiff, Error>) -> ()
    ) {
        var localDescriptors = [any SHAssetDescriptor]()
        var localError: Error? = nil
        var remoteUsers = [SHServerUser]()
        var remoteUsersError: Error? = nil
        var dispatchResult: DispatchTimeoutResult? = nil
        
        ///
        /// Get all the local descriptors
        ///
        let group = DispatchGroup()
        group.enter()
        self.serverProxy.getLocalAssetDescriptors { localResult in
            switch localResult {
            case .success(let descriptors):
                localDescriptors = descriptors
            case .failure(let err):
                self.log.error("failed to fetch descriptors from LOCAL server when calculating diff: \(err.localizedDescription)")
                localError = err
            }
            group.leave()
        }
        
        dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds))
        guard dispatchResult == .success else {
            completionHandler(.failure(SHBackgroundOperationError.timedOut))
            return
        }
        guard localError == nil else {
            completionHandler(.failure(localError!))
            return
        }
        
        ///
        /// Get all users referenced in either local or remote descriptors (excluding THIS user)
        ///
        var userIdsInLocalDescriptorsSet = Set<String>()
        for localDescriptor in localDescriptors {
            userIdsInLocalDescriptorsSet.insert(localDescriptor.sharingInfo.sharedByUserIdentifier)
            localDescriptor.sharingInfo.sharedWithUserIdentifiersInGroup.keys.forEach({ userIdsInLocalDescriptorsSet.insert($0) })
        }
        userIdsInLocalDescriptorsSet.remove(self.user.identifier)
        let userIdsInLocalDescriptors = Array(userIdsInLocalDescriptorsSet)
        
        var userIdsInRemoteDescriptorsSet = Set<String>()
        for remoteDescriptor in remoteDescriptors {
            userIdsInRemoteDescriptorsSet.insert(remoteDescriptor.sharingInfo.sharedByUserIdentifier)
            remoteDescriptor.sharingInfo.sharedWithUserIdentifiersInGroup.keys.forEach({ userIdsInRemoteDescriptorsSet.insert($0) })
        }
        userIdsInRemoteDescriptorsSet.remove(self.user.identifier)
        let userIdsInRemoteDescriptors = Array(userIdsInRemoteDescriptorsSet)
        
        ///
        /// Get the `SHServerUser` for each of the users mentioned in the remote descriptors
        ///
        group.enter()
        self.serverProxy.remoteServer.getUsers(withIdentifiers: userIdsInRemoteDescriptors) { result in
            switch result {
            case .success(let serverUsers):
                remoteUsers = serverUsers
            case .failure(let err):
                self.log.error("failed to fetch users from server when calculating diff: \(err.localizedDescription)")
                remoteUsersError = err
            }
            group.leave()
        }
        
        dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultNetworkTimeoutInMilliseconds))
        guard dispatchResult == .success else {
            completionHandler(.failure(SHBackgroundOperationError.timedOut))
            return
        }
        guard remoteUsersError == nil else {
            completionHandler(.failure(remoteUsersError!))
            return
        }
        
        ///
        /// Remove all users that don't exist on the server from the local server and the graph
        ///
        let uIdsToRemoveFromLocal = Array(userIdsInLocalDescriptorsSet.subtracting(userIdsInRemoteDescriptorsSet))
        if uIdsToRemoveFromLocal.count > 0 {
            log.info("removing user ids from graph \(uIdsToRemoveFromLocal)")
            do {
                try SHUsersController(localUser: self.user).deleteUsers(withIdentifiers: uIdsToRemoveFromLocal)
            } catch {
                log.warning("error removing local users, but this operation will be retried")
            }
            
            do {
                try SHKGQuery.removeUsers(with: uIdsToRemoveFromLocal)
            } catch {
                let _ = try? SHDBManager.sharedInstance.graph().removeAll()
                log.warning("error updating the graph. Trying to remove all graph entries and force quitting. On restart the graph will be re-created, but this operation will be retried")
            }
        }
        
        ///
        /// Let the delegate know about the new list of verified users
        ///
        self.delegates.forEach({
            $0.usersAreConnectedAndVerified(remoteUsers)
        })
        
        ///
        /// Get all the asset identifiers mentioned in the remote descriptors
        ///
        let allSharedAssetGIds = remoteDescriptors
            .filter({ $0.sharingInfo.sharedByUserIdentifier != self.user.identifier })
            .map({ $0.globalIdentifier })
        self.delegates.forEach({
            $0.assetIdsAreSharedWithUser(Array(Set(allSharedAssetGIds)))
        })
        
        ///
        /// Remove all users that don't exist on the server from any blacklist
        ///
        /// If a user that was in the blacklist no longer exists on the server
        /// that user can be safely removed from the blacklist,
        /// as well as all downloads from that user currently awaiting authorization
        ///
        DownloadBlacklist.shared.removeFromBlacklistIfNotIn(userIdentifiers: userIdsInRemoteDescriptors)
        
        let downloadsManager = SHAssetsDownloadManager(user: self.user)
        do {
            try downloadsManager.cleanEntriesNotIn(allSharedAssetIds: allSharedAssetGIds,
                                                   allUserIds: userIdsInRemoteDescriptors)
        } catch {
            log.error("failed to clean up download queues and index on deleted assets: \(error.localizedDescription)")
        }
        let userBlacklist = downloadsManager.blacklistedUsers
        let uIdsToRemoveFromBlacklist = userBlacklist.subtract(userIdsInRemoteDescriptors)
        downloadsManager.removeUsersFromBlacklist(with: uIdsToRemoveFromBlacklist)
        
        ///
        /// Generate a diff of assets and users (latter organized by group)
        ///
        /// Handle the following cases:
        /// 1. The asset has been encrypted but not yet downloaded (so the server doesn't know about that asset yet)
        ///     -> needs to be kept as the user encryption secret - necessary for uploading and sharing - is stored there
        /// 2. The descriptor exists on the server but not locally
        ///     -> It will be created locally, created in the ShareHistory or UploadHistory queue item by any DownloadOperation. Nothing to do here.
        /// 3. The descriptor exists locally but not on the server
        ///     -> remove it as long as it's not case 1
        /// 4. The local upload state doesn't match the remote state
        ///     -> inefficient solution is to verify the asset is in S3. Efficient is to trust the value from the server
        ///
        let diff = AssetDescriptorsDiff.generateUsing(server: remoteDescriptors,
                                                      local: localDescriptors,
                                                      serverUserIds: userIdsInRemoteDescriptors,
                                                      localUserIds: userIdsInLocalDescriptors,
                                                      for: self.user)
        
        ///
        /// Remove users that should be removed from local server
        ///
        if diff.assetsRemovedOnServer.count > 0 {
            let globalIdentifiers = diff.assetsRemovedOnServer.compactMap { $0.globalIdentifier }
            self.serverProxy.localServer.deleteAssets(withGlobalIdentifiers: globalIdentifiers) { result in
                if case .failure(let error) = result {
                    self.log.error("some assets were deleted on server but couldn't be deleted from local cache. This operation will be attempted again, but for now the cache is out of sync. error=\(error.localizedDescription)")
                }
            }
        }
        
        ///
        /// Change the upload state of the assets that are not in sync with server states
        ///
        for stateChangeDiff in diff.stateDifferentOnServer {
            self.serverProxy.localServer.markAsset(with: stateChangeDiff.globalIdentifier,
                                                   quality: stateChangeDiff.quality,
                                                   as: stateChangeDiff.newUploadState) { result in
                if case .failure(let error) = result {
                    self.log.error("some assets were marked as \(stateChangeDiff.newUploadState.rawValue) on server but not in the local cache. This operation will be attempted again, but for now the cache is out of sync. error=\(error.localizedDescription)")
                }
            }
        }
        
        let queueDiff = self.removeUsersFromStores(diff.userIdsToRemoveFromGroup)
        if queueDiff.changed.count > 0 {
            self.log.debug("[sync] notifying queue items changed \(queueDiff.changed)")
            self.delegates.forEach({
                $0.shareHistoryQueueItemsChanged(withIdentifiers: queueDiff.changed)
            })
        }
        if queueDiff.removed.count > 0 {
            self.log.debug("[sync] notifying queue items removed \(queueDiff.removed)")
            self.delegates.forEach({
                $0.shareHistoryQueueItemsRemoved(withIdentifiers: queueDiff.removed)
            })
        }
        
        if diff.userIdsToAddToSharesOfAssetGid.count > 0 {
            let dispatchGroup = DispatchGroup()
            var addRecipientErrorById = [GlobalIdentifier: Error]()
            
            ///
            /// Add users to the shares in the graph and notify the delegates
            ///
            for (globalIdentifier, shareDiff) in diff.userIdsToAddToSharesOfAssetGid {
                do {
                    try SHKGQuery.ingestShare(
                        of: globalIdentifier,
                        from: shareDiff.from,
                        to: Array(shareDiff.groupIdByRecipientId.keys)
                    )
                    
                    dispatchGroup.enter()
                    self.serverProxy.localServer.addAssetRecipients(
                        to: globalIdentifier,
                        basedOn: shareDiff.groupIdByRecipientId
                    ) { result in
                        switch result {
                        case .success():
                            self.delegates.forEach {
                                $0.usersWereAddedToShare(of: globalIdentifier, groupIdByRecipientId: shareDiff.groupIdByRecipientId)
                            }
                        case .failure(let err):
                            addRecipientErrorById[globalIdentifier] = err
                        }
                        dispatchGroup.leave()
                    }
                } catch {
                    log.error("[sync] failed to add recipients to the share of \(globalIdentifier): \(error.localizedDescription)")
                    continue
                }
            }
            
            let dispatchResult = dispatchGroup.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds * diff.userIdsToRemoveToSharesOfAssetGid.count))
            if dispatchResult != .success {
                log.error("[sync] failed to add recipients to some shares: \(SHBackgroundOperationError.timedOut.localizedDescription)")
            }
            if addRecipientErrorById.count != 0 {
                log.error("[sync] failed to add recipients to some shares: \(addRecipientErrorById)")
            }
        }
        
        if diff.userIdsToRemoveToSharesOfAssetGid.count > 0 {
            var condition = KBTripleCondition(value: false)
            let dispatchGroup = DispatchGroup()
            var removeRecipientErrorById = [GlobalIdentifier: Error]()
            
            ///
            /// Remove users from the shares
            ///
            for (globalIdentifier, shareDiff) in diff.userIdsToRemoveToSharesOfAssetGid {
                for recipientId in shareDiff.groupIdByRecipientId.keys {
                    condition = condition.or(KBTripleCondition(
                        subject: globalIdentifier,
                        predicate: SHKGPredicates.sharedWith.rawValue,
                        object: recipientId
                    ))
                }
            }
            do {
                try SHKGQuery.removeTriples(matching: condition)
                
                ///
                /// Only after the Graph is updated, remove the recipients from the DB
                /// This ensures that if the graph update fails is attempted again (as the descriptors from local haven't been updated yet)
                ///
                for (globalIdentifier, shareDiff) in diff.userIdsToRemoveToSharesOfAssetGid {
                    dispatchGroup.enter()
                    let userIds = Array(shareDiff.groupIdByRecipientId.keys)
                    self.serverProxy.localServer.removeAssetRecipients(
                        recipientUserIds: userIds,
                        from: globalIdentifier
                    ) { result in
                        switch result {
                        case .success():
                            self.delegates.forEach {
                                $0.usersWereRemovedFromShare(of: globalIdentifier,
                                                             groupIdByRecipientId: shareDiff.groupIdByRecipientId)
                            }
                        case .failure(let err):
                            removeRecipientErrorById[globalIdentifier] = err
                        }
                        dispatchGroup.leave()
                    }
                }
                
                let dispatchResult = dispatchGroup.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds * diff.userIdsToRemoveToSharesOfAssetGid.count))
                guard dispatchResult == .success else {
                    throw SHBackgroundOperationError.timedOut
                }
                if removeRecipientErrorById.count != 0 {
                    log.error("[sync] failed to remove recipients from some shares: \(removeRecipientErrorById)")
                }
            } catch {
                log.error("[sync] failed to remove recipients from some shares: \(error.localizedDescription)")
            }
        }
        
        completionHandler(.success(diff))
    }
    
    private func syncReactions(
        in groupId: String,
        localReactions: [ReactionOutputDTO],
        remoteReactions: [ReactionOutputDTO]
    ) throws {
        var reactionsToUpdate = [ReactionOutputDTO]()
        var reactionsToRemove = [ReactionOutputDTO]()
        for remoteReaction in remoteReactions {
            let existing = localReactions.first(where: {
                $0.senderUserIdentifier == remoteReaction.senderUserIdentifier
                && $0.inReplyToInteractionId == remoteReaction.inReplyToInteractionId
                && $0.inReplyToAssetGlobalIdentifier == remoteReaction.inReplyToAssetGlobalIdentifier
                && $0.reactionType == remoteReaction.reactionType
            })
            if existing == nil {
                reactionsToUpdate.append(remoteReaction)
            }
        }
        
        for localReaction in localReactions {
            let existingOnRemote = remoteReactions.first(where: {
                $0.senderUserIdentifier == localReaction.senderUserIdentifier
                && $0.inReplyToInteractionId == localReaction.inReplyToInteractionId
                && $0.inReplyToAssetGlobalIdentifier == localReaction.inReplyToAssetGlobalIdentifier
                && $0.reactionType == localReaction.reactionType
            })
            if existingOnRemote == nil {
                reactionsToRemove.append(localReaction)
            }
        }
        
        let dispatchGroup = DispatchGroup()
        var anyChanged = false
        
        if reactionsToUpdate.count > 0 {
            dispatchGroup.enter()
            serverProxy.localServer.addReactions(reactionsToUpdate,
                                                 toGroupId: groupId) { addReactionsResult in
                if case .failure(let failure) = addReactionsResult {
                    self.log.warning("failed to add reactions retrieved from server on local. \(failure.localizedDescription)")
                } else {
                    anyChanged = true
                }
                dispatchGroup.leave()
            }
        }
        if reactionsToRemove.count > 0 {
            dispatchGroup.enter()
            serverProxy.localServer.removeReactions(reactionsToRemove,
                                                    fromGroupId: groupId) { removeReactionsResult in
                if case .failure(let failure) = removeReactionsResult {
                    self.log.warning("failed to remove reactions from local. \(failure.localizedDescription)")
                } else {
                    anyChanged = true
                }
                dispatchGroup.leave()
            }
        }
        
        if anyChanged {
            self.delegates.forEach({ $0.reactionsDidChange(in: groupId) })
        }
        
        let dispatchResult = dispatchGroup.wait(timeout: .now() + .milliseconds(SHDefaultNetworkTimeoutInMilliseconds))
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
    }
    
    private func syncMessages(
        in groupId: String,
        localMessages: [MessageOutputDTO],
        remoteMessages: [MessageOutputDTO]
    ) throws {
        var messagesToUpdate = [MessageOutputDTO]()
        for remoteMessage in remoteMessages {
            let existing = localMessages.first(where: {
                $0.interactionId == remoteMessage.interactionId
            })
            if existing == nil {
                messagesToUpdate.append(remoteMessage)
            }
        }
        
        let dispatchGroup = DispatchGroup()
        
        if messagesToUpdate.count > 0 {
            dispatchGroup.enter()
            serverProxy.localServer.addMessages(messagesToUpdate,
                                                toGroupId: groupId) { addMessagesResult in
                if case .failure(let failure) = addMessagesResult {
                    self.log.warning("failed to add messages retrieved from server on local. \(failure.localizedDescription)")
                } else {
                    self.delegates.forEach({ $0.didReceiveMessage(in: groupId) })
                }
                dispatchGroup.leave()
            }
        }
        
        let dispatchResult = dispatchGroup.wait(timeout: .now() + .milliseconds(SHDefaultNetworkTimeoutInMilliseconds))
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
    }
    
    ///
    /// Best attempt to sync the interactions from the server to the local server proxy by calling SHUserInteractionController::retrieveInteractions
    ///
    /// - Parameter descriptorsByGlobalIdentifier: the descriptors retrieved from server, from which to collect all unique groups
    ///
    private func syncInteractions(
        remoteDescriptors: [any SHAssetDescriptor],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        ///
        /// Extract unique group ids from the descriptors
        ///
        
        let allSharedGroupIds = Array(remoteDescriptors.reduce([String: Int](), { partialResult, descriptor in
            var result = partialResult
            
            var isShared = false
            let userIdsSharedWith: Set<String> = Set(descriptor.sharingInfo.sharedWithUserIdentifiersInGroup.keys)
            let myUserId = self.user.identifier
            if descriptor.sharingInfo.sharedByUserIdentifier == myUserId {
                if userIdsSharedWith.subtracting([myUserId]).count > 0 {
                    isShared = true
                }
            } else if userIdsSharedWith.contains(myUserId) {
                isShared = true
            }
            
            guard isShared else {
                /// If not shared, do nothing (do not update the partial result)
                return result
            }
            
            for (userId, groupId) in descriptor.sharingInfo.sharedWithUserIdentifiersInGroup {
                if userId != self.user.identifier {
                    result[groupId] = 1
                }
            }
            return result
        }).keys)
        
        let dispatchGroup = DispatchGroup()
        var error: Error? = nil
        
        ///
        /// For each group …
        ///
        
        for groupId in allSharedGroupIds {
            
            var remoteInteractions: InteractionsGroupDTO? = nil
            
            ///
            /// Retrieve the REMOTE interactions
            ///
            
            dispatchGroup.enter()
            self.serverProxy.retrieveRemoteInteractions(
                inGroup: groupId,
                per: 1000, page: 1
            ) { result in
                switch result {
                case .failure(let err):
                    error = err
                case .success(let interactions):
                    remoteInteractions = interactions
                }
                dispatchGroup.leave()
            }
            
            ///
            /// Retrieve the LOCAL interactions
            ///
            
            var shouldCreateE2EEDetailsLocally = false
            var localMessages = [MessageOutputDTO]()
            var localReactions = [ReactionOutputDTO]()
            
            dispatchGroup.enter()
            serverProxy.retrieveInteractions(
                inGroup: groupId,
                per: 10000, page: 1
            ) { localResult in
                switch localResult {
                case .failure(let err):
                    if case SHBackgroundOperationError.missingE2EEDetailsForGroup(_) = err {
                        shouldCreateE2EEDetailsLocally = true
                    }
                    self.log.error("failed to retrieve local interactions for groupId \(groupId)")
                case .success(let localInteractions):
                    localMessages = localInteractions.messages
                    localReactions = localInteractions.reactions
                }
                dispatchGroup.leave()
            }
            
            var dispatchResult = dispatchGroup.wait(timeout: .now() + .milliseconds(SHDefaultNetworkTimeoutInMilliseconds))
            guard dispatchResult == .success else {
                completionHandler(.failure(SHBackgroundOperationError.timedOut))
                return
            }
            guard error == nil else {
                log.error("error syncing interactions for group \(groupId). \(error!.localizedDescription)")
                continue
            }
            guard let remoteInteractions = remoteInteractions else {
                log.error("error retrieving remote interactions for group \(groupId)")
                continue
            }
            
            ///
            /// Add the E2EE encryption details for the group locally if missing
            ///
            
            if shouldCreateE2EEDetailsLocally {
                let recipientEncryptionDetails = RecipientEncryptionDetailsDTO(
                    userIdentifier: self.user.identifier,
                    ephemeralPublicKey: remoteInteractions.ephemeralPublicKey,
                    encryptedSecret: remoteInteractions.encryptedSecret,
                    secretPublicSignature: remoteInteractions.secretPublicSignature
                )
                
                dispatchGroup.enter()
                serverProxy.localServer.setGroupEncryptionDetails(
                    groupId: groupId,
                    recipientsEncryptionDetails: [recipientEncryptionDetails]
                ) { setE2EEDetailsResult in
                    switch setE2EEDetailsResult {
                    case .success(_):
                        break
                    case .failure(let err):
                        self.log.error("Cache interactions for group \(groupId) won't be readable because setting the E2EE details for such group failed: \(err.localizedDescription)")
                    }
                    dispatchGroup.leave()
                }
            }
            
            dispatchResult = dispatchGroup.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds))
            if dispatchResult != .success {
                log.warning("timeout while setting E2EE details for groupId \(groupId)")
            }
            
            do {
                ///
                /// Sync (create, update and delete) reactions
                ///
                
                let remoteReactions = remoteInteractions.reactions
                try self.syncReactions(
                    in: groupId,
                    localReactions: localReactions,
                    remoteReactions: remoteReactions
                )
                
                ///
                /// Sync (create, update and delete) messages
                ///
                
                let remoteMessages = remoteInteractions.messages
                try self.syncMessages(
                    in: groupId,
                    localMessages: localMessages,
                    remoteMessages: remoteMessages
                )
            } catch {
                log.warning("error while syncing messages and reactions retrieved from server on local")
            }
        }
        
        completionHandler(.success(()))
    }
    
    public func sync(
        remoteDescriptors: [any SHAssetDescriptor],
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        let group = DispatchGroup()
        var descriptorsSyncError: Error? = nil
        var interactionsSyncError: Error? = nil
        
        ///
        /// Sync them with the local descriptors
        ///
        group.enter()
        self.syncDescriptors(remoteDescriptors) { result in
            switch result {
            case .success(let diff):
                if diff.assetsRemovedOnServer.count > 0 {
                    ///
                    /// Remove items in download queues and indices that no longer exist
                    ///
                    do {
                        let downloadsManager = SHAssetsDownloadManager(user: self.user)
                        try downloadsManager.cleanEntries(for: diff.assetsRemovedOnServer.map({ $0.globalIdentifier }))
                    } catch {
                        self.log.error("[sync] failed to clean up download queues and index on deleted assets: \(error.localizedDescription)")
                    }
                    
                    //
                    // TODO: THIS IS A BIG ONE!!!
                    // TODO: The deletion of these assets from all the other queues is currently taken care of by the delegate.
                    // TODO: The framework should be responsible for it instead.
                    // TODO: Deletion of entities in the graph should be taken care of here, too. Currently it can't because the client is currently querying the graph before deleting to understand which conversation threads need to be removed
                    //
                    
                    self.log.debug("[sync] notifying about deleted assets \(diff.assetsRemovedOnServer)")
                    self.delegates.forEach({
                        $0.assetsWereDeleted(diff.assetsRemovedOnServer)
                    })
                }
                if diff.stateDifferentOnServer.count > 0 {
                    // TODO: Do we need to mark things as failed/pending depending on state?
                }
            case .failure(let err):
                self.log.error("failed to update local descriptors from server descriptors: \(err.localizedDescription)")
                descriptorsSyncError = err
            }
            group.leave()
        }
        
        ///
        /// Sync interactions
        ///
        group.enter()
        self.syncInteractions(remoteDescriptors: remoteDescriptors) { result in
            if case .failure(let err) = result {
                self.log.error("failed to sync interactions: \(err.localizedDescription)")
                interactionsSyncError = err
            }
            group.leave()
        }
        
        group.notify(queue: .global(qos: .background)) {
            if let err = descriptorsSyncError {
                completionHandler(.failure(err))
            }
            else if let err = interactionsSyncError {
                completionHandler(.failure(err))
            }
            else {
                completionHandler(.success(()))
            }
        }
    }
    
    private func runOnce(completionHandler: @escaping (Result<Void, Error>) -> Void) {
        ///
        /// Get the descriptors from the server
        ///
        self.serverProxy.getRemoteAssetDescriptors { remoteResult in
            switch remoteResult {
            case .success(let descriptors):
                ///
                /// Start the sync process
                ///
                self.sync(remoteDescriptors: descriptors, completionHandler: completionHandler)
            case .failure(let err):
                self.log.error("failed to fetch descriptors from server when calculating diff: \(err.localizedDescription)")
                completionHandler(.failure(err))
            }
        }
    }
    
    public override func main() {
        guard !self.isCancelled else {
            state = .finished
            return
        }
        
        state = .executing
        
        self.runOnce { result in
            self.state = .finished
        }
    }
}


private class SHSyncProcessor : SHBackgroundOperationProcessor<SHSyncOperation> {
    
    public static var shared = SHSyncProcessor(
        delayedStartInSeconds: 6,
        dispatchIntervalInSeconds: 15
    )
    private override init(delayedStartInSeconds: Int,
                          dispatchIntervalInSeconds: Int? = nil) {
        super.init(delayedStartInSeconds: delayedStartInSeconds,
                   dispatchIntervalInSeconds: dispatchIntervalInSeconds)
    }
}
