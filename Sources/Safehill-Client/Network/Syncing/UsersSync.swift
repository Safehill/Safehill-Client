import Foundation
import KnowledgeBase

extension SHSyncOperation {
    
    /// ** !!!!!!!!!! **
    /// ** !!!!!!!!!! **
    /// ** !!!!!!!!!! **
    // TODO: Re-enable this
    /// ** !!!!!!!!!! **
    /// ** !!!!!!!!!! **
    /// ** !!!!!!!!!! **
    
    /*
    
    /// Removes any evidence of the users from the local storage:
    /// - Replaces the items in the `ShareHistoryQueue` with the same items by omitting the users removed.
    /// - Removes the sharing information from the `assetsStore`
    ///
    /// Returns the queueItemIdentifiers replaced and the ones removed
    /// - Parameter userIdsToRemoveFromGroup: maps groupId -> list of user ids to remove
    /// - Returns: the list of keys changed and removed in the `SHShareHistoryQueue`
    ///
    func removeUsersFromShareHistoryQueueItems(_ userIdsToRemoveFromGroup: [String: Set<UserIdentifier>]) -> (changed: [String], removed: [String]) {
        
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
     */
}
