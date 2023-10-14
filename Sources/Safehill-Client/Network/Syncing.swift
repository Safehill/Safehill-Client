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
    
    public override func main() {
        guard !self.isCancelled else {
            state = .finished
            return
        }
        
        state = .executing
        
        self.sync()
        
        self.state = .finished
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
    
    private func syncDescriptors(completionHandler: @escaping (Swift.Result<AssetDescriptorsDiff, Error>) -> ()) {
        var localDescriptors = [any SHAssetDescriptor](), remoteDescriptors = [any SHAssetDescriptor]()
        var localError: Error? = nil, remoteError: Error? = nil
        var remoteUsers = [SHServerUser]()
        var remoteUsersError: Error? = nil
        
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
        
        ///
        /// Get all the remote descriptors
        ///
        group.enter()
        self.serverProxy.getRemoteAssetDescriptors { remoteResult in
            switch remoteResult {
            case .success(let descriptors):
                remoteDescriptors = descriptors
            case .failure(let err):
                self.log.error("failed to fetch descriptors from server when calculating diff: \(err.localizedDescription)")
                remoteError = err
            }
            group.leave()
        }
        
        let dispatcResult = group.wait(timeout: .now() + .milliseconds(SHDefaultNetworkTimeoutInMilliseconds))
        guard dispatcResult == .success else {
            completionHandler(.failure(SHBackgroundOperationError.timedOut))
            return
        }
        guard localError == nil, remoteError == nil else {
            completionHandler(.failure(localError ?? remoteError!))
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
        
        let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultNetworkTimeoutInMilliseconds))
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
            do {
                try SHUsersController(localUser: self.user).deleteUsers(withIdentifiers: uIdsToRemoveFromLocal)
            } catch {
                log.warning("error removing local users, but this operation will be retried")
            }
            
            do {
                let graph = try SHDBManager.sharedInstance.graph()
                for userId in uIdsToRemoveFromLocal {
                    try graph.removeEntity(userId)
                }
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
            log.error("failed to clean up download queues and index on deleted assets")
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
            self.delegates.forEach({
                $0.shareHistoryQueueItemsChanged(withIdentifiers: queueDiff.changed)
            })
        }
        if queueDiff.removed.count > 0 {
            self.delegates.forEach({
                $0.shareHistoryQueueItemsRemoved(withIdentifiers: queueDiff.removed)
            })
        }
        
        completionHandler(.success(diff))
    }
    
    public func sync() {
        let semaphore = DispatchSemaphore(value: 0)
        self.syncDescriptors { result in
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
                        self.log.error("failed to clean up download queues and index on deleted assets")
                    }
                    
                    //
                    // TODO: THIS IS A BIG ONE!!!
                    // TODO: The deletion of these assets from all the other queues is currently taken care of by the delegate.
                    // TODO: The framework should be responsible for it instead.
                    // TODO: Deletion of entities in the graph should be taken care of here, too. Currently it can't because the client is currently querying the graph before deleting to understand which conversation threads need to be removed
                    //
                    self.delegates.forEach({
                        $0.assetsWereDeleted(diff.assetsRemovedOnServer)
                    })
                }
                if diff.stateDifferentOnServer.count > 0 {
                    // TODO: Do we need to mark things as failed/pending depending on state?
                }
            case .failure(let err):
                self.log.error("failed to update local descriptors from server descriptors: \(err.localizedDescription)")
            }
            semaphore.signal()
        }
        
        semaphore.wait()
    }
}


public class SHSyncProcessor : SHBackgroundOperationProcessor<SHSyncOperation> {
    
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
