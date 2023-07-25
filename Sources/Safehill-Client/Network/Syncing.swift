import Foundation
import KnowledgeBase
import os


extension SHServerProxy {
    
    /// Removes any evidence of the users removed from the local storage:
    /// - Replaces the items in the `ShareHistoryQueue` with the same items by omitting the users removed.
    /// - Removes the sharing information from the `assetsStore`
    ///
    /// Returns the queueItemIdentifiers replaced and the ones removed
    /// - Parameter userIdsToRemoveFromGroup: maps groupId -> list of user ids to remove
    /// - Returns: the list of keys changed and removed in the `SHShareHistoryQueue`
    /// 
    private func removeUsersFromStores(_ userIdsToRemoveFromGroup: [String: [String]]) -> (changed: [String], removed: [String]) {
        
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
                    self.localServer.unshare(assetId: share.value.item.globalAssetId, with: userId) { result in
                        if case .failure(let error) = result {
                            log.error("some assets were unshared on server but not in the local cache. This operation will be attempted again, but for now the cache is out of sync. error=\(error.localizedDescription)")
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
    
    private func syncDescriptors(delegate: SHAssetSyncingDelegate?,
                                 completionHandler: @escaping (Swift.Result<AssetDescriptorsDiff, Error>) -> ()) {
        var localDescriptors = [any SHAssetDescriptor](), remoteDescriptors = [any SHAssetDescriptor]()
        var remoteUsers = [SHServerUser]()
        var localError: Error? = nil, remoteError: Error? = nil
        var remoteUsersError: Error? = nil
        
        let group = DispatchGroup()
        group.enter()
        self.getLocalAssetDescriptors { localResult in
            switch localResult {
            case .success(let descriptors):
                localDescriptors = descriptors
            case .failure(let err):
                log.error("failed to fetch descriptors from LOCAL server when calculating diff: \(err.localizedDescription)")
                localError = err
            }
            group.leave()
        }
        
        group.enter()
        self.getRemoteAssetDescriptors { remoteResult in
            switch remoteResult {
            case .success(let descriptors):
                remoteDescriptors = descriptors
            case .failure(let err):
                log.error("failed to fetch descriptors from server when calculating diff: \(err.localizedDescription)")
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
        
        var userIdsInDescriptorsSet = Set<String>()
        for localDescriptor in localDescriptors {
            userIdsInDescriptorsSet.insert(localDescriptor.sharingInfo.sharedByUserIdentifier)
            localDescriptor.sharingInfo.sharedWithUserIdentifiersInGroup.keys.forEach({ userIdsInDescriptorsSet.insert($0) })
        }
        userIdsInDescriptorsSet.remove(self.remoteServer.requestor.identifier)
        let userIdsInDescriptors = Array(userIdsInDescriptorsSet)
        
        group.enter()
        self.remoteServer.getUsers(withIdentifiers: nil) { result in
            switch result {
            case .success(let serverUsers):
                remoteUsers = serverUsers
            case .failure(let err):
                log.error("failed to fetch users from server when calculating diff: \(err.localizedDescription)")
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
        
        let allSharedAssetGIds = remoteDescriptors
            .filter({ $0.sharingInfo.sharedByUserIdentifier != self.localServer.requestor.identifier })
            .map({ $0.globalIdentifier })
        delegate?.assetIdsAreSharedWithUser(Array(Set(allSharedAssetGIds)))
        delegate?.usersAreConnectedAndVerified(remoteUsers)
        let remoteUserIds = remoteUsers.map({ $0.identifier })
        
        DownloadBlacklist.shared.removeFromBlacklistIfNotIn(userIdentifiers: remoteUserIds)
        
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
                                                      serverUserIds: remoteUserIds,
                                                      localUserIds: userIdsInDescriptors,
                                                      for: self.localServer.requestor)
        
        if diff.assetsRemovedOnServer.count > 0 {
            let globalIdentifiers = diff.assetsRemovedOnServer.compactMap { $0.globalIdentifier }
            self.localServer.deleteAssets(withGlobalIdentifiers: globalIdentifiers) { result in
                if case .failure(let error) = result {
                    log.error("some assets were deleted on server but couldn't be deleted from local cache. This operation will be attempted again, but for now the cache is out of sync. error=\(error.localizedDescription)")
                }
            }
        }
        
        for stateChangeDiff in diff.stateDifferentOnServer {
            self.localServer.markAsset(with: stateChangeDiff.globalIdentifier,
                                       quality: stateChangeDiff.quality,
                                       as: stateChangeDiff.newUploadState) { result in
                if case .failure(let error) = result {
                    log.error("some assets were marked as uploaded on server but not in the local cache. This operation will be attempted again, but for now the cache is out of sync. error=\(error.localizedDescription)")
                }
            }
        }
        
        let userIdsToRemove = userIdsInDescriptors.subtract(remoteUserIds)
        ServerUserCache.shared.evict(usersWithIdentifiers: userIdsToRemove)
        
        let queueDiff = self.removeUsersFromStores(diff.userIdsToRemoveFromGroup)
        if queueDiff.changed.count > 0 {
            delegate?.shareHistoryQueueItemsChanged(withIdentifiers: queueDiff.changed)
        }
        if queueDiff.removed.count > 0 {
            delegate?.shareHistoryQueueItemsRemoved(withIdentifiers: queueDiff.removed)
        }
        
        completionHandler(.success(diff))
    }
    
    public func sync(delegate: SHAssetSyncingDelegate?) {
        let semaphore = DispatchSemaphore(value: 0)
        self.syncDescriptors(delegate: delegate) { result in
            switch result {
            case .success(let diff):
                if diff.assetsRemovedOnServer.count > 0 {
                    //
                    // TODO: THIS IS A BIG ONE!!!
                    // TODO: The deletion from the queues defined in the framework is taken care of by the `AssetUploadController` which is a client of the framework. Consider moving `AssetUploadController` and the sister controllers to the framework
                    //
                    delegate?.assetsWereDeleted(diff.assetsRemovedOnServer)
                }
                if diff.stateDifferentOnServer.count > 0 {
                    // TODO: Do we need to mark things as failed/pending depending on state?
                }
            case .failure(let err):
                log.error("failed to update local descriptors from server descriptors: \(err.localizedDescription)")
            }
            semaphore.signal()
        }
        
        semaphore.wait()
    }
}

// MARK: - Sync Operation

public class SHSyncOperation: SHAbstractBackgroundOperation, SHBackgroundOperationProtocol {
    
    public let log = Logger(subsystem: "com.safehill", category: "BG-SYNC")
    
    let user: SHLocalUser
    
    let delegate: SHAssetSyncingDelegate?
    
    private var serverProxy: SHServerProxy {
        SHServerProxy(user: self.user)
    }
    
    public init(user: SHLocalUser, delegate: SHAssetSyncingDelegate?) {
        self.user = user
        self.delegate = delegate
    }
    
    public func clone() -> SHBackgroundOperationProtocol {
        SHSyncOperation(user: self.user, delegate: self.delegate)
    }
    
    public override func main() {
        guard !self.isCancelled else {
            state = .finished
            return
        }
        
        state = .executing
        
        self.serverProxy.sync(delegate: delegate)
        
        self.state = .finished
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
