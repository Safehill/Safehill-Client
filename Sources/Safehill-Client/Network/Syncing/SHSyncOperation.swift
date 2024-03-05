import Foundation
import KnowledgeBase
import os


// MARK: - Sync Operation

public class SHSyncOperation: SHAbstractBackgroundOperation, SHBackgroundOperationProtocol {
    
    public let log = Logger(subsystem: "com.safehill", category: "BG-SYNC")
    
    let delegatesQueue = DispatchQueue(label: "com.safehill.sync.delegates")
    
    let user: SHAuthenticatedLocalUser
    
    let assetsDelegates: [SHAssetSyncingDelegate]
    let threadsDelegates: [SHThreadSyncingDelegate]
    
    var serverProxy: SHServerProxy { user.serverProxy }
    
    public init(
        user: SHAuthenticatedLocalUser,
        assetsDelegates: [SHAssetSyncingDelegate],
        threadsDelegates: [SHThreadSyncingDelegate]
    ) {
        self.user = user
        self.assetsDelegates = assetsDelegates
        self.threadsDelegates = threadsDelegates
    }
    
    public func clone() -> SHBackgroundOperationProtocol {
        SHSyncOperation(
            user: self.user,
            assetsDelegates: self.assetsDelegates,
            threadsDelegates: self.threadsDelegates
        )
    }
    
    private func uniqueUserIds(in descriptors: [any SHAssetDescriptor]) -> Set<UserIdentifier> {
        var userIdsDescriptorsSet = Set<UserIdentifier>()
        for descriptor in descriptors {
            userIdsDescriptorsSet.insert(descriptor.sharingInfo.sharedByUserIdentifier)
            descriptor.sharingInfo.sharedWithUserIdentifiersInGroup.keys.forEach({ userIdsDescriptorsSet.insert($0) })
        }
        return userIdsDescriptorsSet
    }
    
    private func syncDescriptors(
        remoteDescriptors: [any SHAssetDescriptor],
        localDescriptors: [any SHAssetDescriptor],
        completionHandler: @escaping (Result<AssetDescriptorsDiff, Error>) -> ()
    ) {
        let remoteUsersById: [UserIdentifier: any SHServerUser]
        
        ///
        /// Get all users referenced in either local or remote descriptors (excluding THIS user)
        ///
        var userIdsInLocalDescriptorsSet = self.uniqueUserIds(in: localDescriptors)
        userIdsInLocalDescriptorsSet.remove(self.user.identifier)
        let userIdsInLocalDescriptors = Array(userIdsInLocalDescriptorsSet)
        
        var userIdsInRemoteDescriptorsSet = self.uniqueUserIds(in: remoteDescriptors)
        userIdsInRemoteDescriptorsSet.remove(self.user.identifier)
        let userIdsInRemoteDescriptors = Array(userIdsInRemoteDescriptorsSet)
        
        ///
        /// Get the `SHServerUser` for each of the users mentioned in the remote descriptors
        ///
        do {
            remoteUsersById = try SHUsersController(localUser: self.user).getUsers(
                withIdentifiers: userIdsInRemoteDescriptors
            )
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        ///
        /// Don't consider users that can't be retrieved by the `SHUserController`.
        /// This is just an extra measure on the client in case the server returns users that are deleted or deactivated.
        ///
        userIdsInRemoteDescriptorsSet = userIdsInRemoteDescriptorsSet.intersection(remoteUsersById.keys)
        
        ///
        /// Remove all users that don't exist on the server from the local server and the graph
        ///
        let uIdsToRemoveFromLocal = Array(userIdsInLocalDescriptorsSet.subtracting(userIdsInRemoteDescriptorsSet))
        if uIdsToRemoveFromLocal.count > 0 {
            log.info("removing user ids from local store and the graph \(uIdsToRemoveFromLocal)")
            do {
                try SHUsersController(localUser: self.user).deleteUsers(withIdentifiers: uIdsToRemoveFromLocal)
            } catch {
                log.warning("error removing local users, but this operation will be retried")
            }
        }
        
        ///
        /// Get all the asset identifiers and user identifiers mentioned in the remote descriptors
        ///
        let assetIdToUserIds = remoteDescriptors
            .reduce([GlobalIdentifier: [any SHServerUser]]()) { partialResult, descriptor in
                var result = partialResult
                var userIdList = Array(descriptor.sharingInfo.sharedWithUserIdentifiersInGroup.keys)
                userIdList.append(descriptor.sharingInfo.sharedByUserIdentifier)
                result[descriptor.globalIdentifier] = userIdList.compactMap({ remoteUsersById[$0] })
                return result
            }
        
        self.delegatesQueue.async { [weak self] in
            self?.assetsDelegates.forEach({
                $0.assetIdsAreVisibleToUsers(assetIdToUserIds)
            })
        }
        
        ///
        /// Remove all users that don't exist on the server from any blacklist
        ///
        /// If a user that was in the blacklist no longer exists on the server
        /// that user can be safely removed from the blacklist,
        /// as well as all downloads from that user currently awaiting authorization
        ///
        Task(priority: .low) {
            await SHDownloadBlacklist.shared.removeFromBlacklistIfNotIn(
                userIdentifiers: userIdsInRemoteDescriptors
            )
        }
        
        do {
            try SHAssetsDownloadManager.cleanEntriesNotIn(
                allSharedAssetIds: Array(assetIdToUserIds.keys),
                allUserIds: userIdsInRemoteDescriptors
            )
        } catch {
            log.error("failed to clean up download queues and index on deleted assets: \(error.localizedDescription)")
        }
        
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
            self.serverProxy.localServer.deleteAssets(
                withGlobalIdentifiers: globalIdentifiers
            ) { result in
                switch result {
                case .failure(let error):
                    self.log.error("some assets were deleted on server but couldn't be deleted from local cache. This operation will be attempted again, but for now the cache is out of sync. error=\(error.localizedDescription)")
                case .success:
                    ///
                    /// Remove items in DOWNLOAD queues and indices that no longer exist
                    ///
                    do {
                        let downloadsManager = SHAssetsDownloadManager(user: self.user)
                        try downloadsManager.cleanEntries(
                            for: diff.assetsRemovedOnServer.map({ $0.globalIdentifier })
                        )
                    } catch {
                        self.log.error("[sync] failed to clean up download queues and index on deleted assets: \(error.localizedDescription)")
                    }
                    
                    ///
                    /// Remove items in the UPLOAD and SHARE queues that no longer exist
                    ///
                    do {
                        try SHQueueOperation.removeItems(correspondingTo: diff.assetsRemovedOnServer.compactMap({ $0.localIdentifier }))
                    } catch {
                        self.log.error("[sync] failed to clean up UPLOAD and SHARE queues on deleted assets: \(error.localizedDescription)")
                    }
                    
                    self.log.debug("[sync] notifying delegates about deleted assets \(diff.assetsRemovedOnServer)")
                    
                    self.delegatesQueue.async { [weak self] in
                        self?.assetsDelegates.forEach({
                            $0.assetsWereDeleted(diff.assetsRemovedOnServer)
                        })
                    }
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
        
        self.delegatesQueue.async { [weak self] in
            if queueDiff.changed.count > 0 {
                self?.log.debug("[sync] notifying queue items changed \(queueDiff.changed)")
                self?.assetsDelegates.forEach({
                    $0.shareHistoryQueueItemsChanged(withIdentifiers: queueDiff.changed)
                })
            }
            if queueDiff.removed.count > 0 {
                self?.log.debug("[sync] notifying queue items removed \(queueDiff.removed)")
                self?.assetsDelegates.forEach({
                    $0.shareHistoryQueueItemsRemoved(withIdentifiers: queueDiff.removed)
                })
            }
        }
        
        let dispatchGroup = DispatchGroup()
        
        if diff.userIdsToAddToSharesOfAssetGid.count > 0 {
            var error: Error? = nil
            
            ///
            /// Add users to the shares in the graph and notify the delegates
            ///
            dispatchGroup.enter()
            self.serverProxy.localServer.addAssetRecipients(
                basedOn: diff.userIdsToAddToSharesOfAssetGid
            ) { result in
                switch result {
                case .success():
                    self.delegatesQueue.async { [weak self] in
                        for (globalIdentifier, shareDiff) in diff.userIdsToAddToSharesOfAssetGid {
                            self?.assetsDelegates.forEach {
                                $0.usersWereAddedToShare(of: globalIdentifier, groupIdByRecipientId: shareDiff.groupIdByRecipientId)
                            }
                        }
                    }
                case .failure(let err):
                    error = err
                }
                dispatchGroup.leave()
            }
            
            let dispatchResult = dispatchGroup.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds))
            guard dispatchResult == .success else {
                log.error("[sync] TIMED OUT when adding recipients to some shares")
                completionHandler(.failure(SHBackgroundOperationError.timedOut))
                return
            }
            if let error {
                log.error("[sync] failed to add recipients to some shares: \(error)")
            }
        }
        
        if diff.userIdsToRemoveToSharesOfAssetGid.count > 0 {
            var error: Error? = nil
            
            dispatchGroup.enter()
            self.serverProxy.localServer.removeAssetRecipients(
                basedOn: diff.userIdsToRemoveToSharesOfAssetGid
            ) { result in
                switch result {
                case .success:
                    self.delegatesQueue.async { [weak self] in
                        for (globalIdentifier, shareDiff) in diff.userIdsToRemoveToSharesOfAssetGid {
                            self?.assetsDelegates.forEach {
                                $0.usersWereRemovedFromShare(of: globalIdentifier,
                                                             groupIdByRecipientId: shareDiff.groupIdByRecipientId)
                            }
                        }
                    }
                case .failure(let err):
                    error = err
                }
                dispatchGroup.leave()
            }
            
            let dispatchResult = dispatchGroup.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds))
            guard dispatchResult == .success else {
                log.error("[sync] TIMED OUT when removing recipients to some shares")
                completionHandler(.failure(SHBackgroundOperationError.timedOut))
                return
            }
            if let error {
                log.error("[sync] failed to remove recipients from some shares: \(error)")
            }
        }
        
        completionHandler(.success(diff))
    }
    
    public func sync(
        remoteDescriptors: [any SHAssetDescriptor],
        localDescriptors: [any SHAssetDescriptor],
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        let group = DispatchGroup()
        var descriptorsSyncError: Error? = nil
        
        ///
        /// Sync them with the local descriptors
        ///
        group.enter()
        self.syncDescriptors(
            remoteDescriptors: remoteDescriptors,
            localDescriptors: localDescriptors
        ) { syncWithLocalDescResult in
            switch syncWithLocalDescResult {
            case .success(let diff):
                if diff.stateDifferentOnServer.count > 0 {
                    // TODO: Do we need to mark things as failed/pending depending on state?
                }
            case .failure(let err):
                self.log.error("failed to update local descriptors from server descriptors: \(err.localizedDescription)")
                descriptorsSyncError = err
            }
            group.leave()
        }
        
        group.notify(queue: .global(qos: .background)) {
            if let err = descriptorsSyncError {
                completionHandler(.failure(err))
            }
            else {
                completionHandler(.success(()))
            }
        }
    }
    
    public func syncInteractions(
        remoteDescriptors: [any SHAssetDescriptor],
        completionHandler: @escaping (Result<Void, Error>) -> Void
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
        
        let group = DispatchGroup()
        var interactionsSyncError: Error? = nil
        
        group.enter()
        self.syncGroupInteractions(groupIds: allSharedGroupIds) { result in
            if case .failure(let err) = result {
                self.log.error("failed to sync interactions: \(err.localizedDescription)")
                interactionsSyncError = err
            }
            group.leave()
        }
        
        group.enter()
        self.syncThreadInteractions { result in
            if case .failure(let err) = result {
                self.log.error("failed to sync interactions: \(err.localizedDescription)")
                interactionsSyncError = err
            }
            group.leave()
        }
        
        group.notify(queue: .global(qos: .background)) {
            if let err = interactionsSyncError {
                completionHandler(.failure(err))
            }
            else {
                completionHandler(.success(()))
            }
        }
    }
    
    private func runOnce(completionHandler: @escaping (Result<Void, Error>) -> Void) {
        
        ///
        /// Get the descriptors from the local server
        ///
        self.serverProxy.getLocalAssetDescriptors { localResult in
            switch localResult {
            case .success(let descriptors):
                let localDescriptors = descriptors
                
                ///
                /// Get the descriptors from the server
                ///
                self.serverProxy.getRemoteAssetDescriptors { remoteResult in
                    switch remoteResult {
                    case .success(let descriptors):
                        let remoteDescriptors = descriptors.filter { remoteDesc in
                            localDescriptors.contains(where: {
                                $0.globalIdentifier == remoteDesc.globalIdentifier
                            })
                        }
                        
                        ///
                        /// Start the sync process
                        ///
                        self.sync(remoteDescriptors: remoteDescriptors,
                                  localDescriptors: localDescriptors,
                                  completionHandler: completionHandler)
                    case .failure(let err):
                        self.log.error("failed to fetch descriptors from server when calculating diff: \(err.localizedDescription)")
                        completionHandler(.failure(err))
                    }
                }
            case .failure(let err):
                self.log.error("failed to fetch descriptors from LOCAL server when calculating diff: \(err.localizedDescription)")
                completionHandler(.failure(err))
            }
        }
    }
    
    public func runOnce(
        for anchor: SHInteractionAnchor,
        anchorId: String,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        switch anchor {
        case .group:
            self.syncGroupInteractions(groupId: anchorId) { result in
                switch result {
                case .failure(let err):
                    self.log.error("failed to sync interactions in \(anchor.rawValue) \(anchorId): \(err.localizedDescription)")
                    completionHandler(.failure(err))
                case .success:
                    completionHandler(.success(()))
                }
            }
        case .thread:
            self.serverProxy.getThread(withId: anchorId) { getThreadResult in
                switch getThreadResult {
                case .failure(let error):
                    self.log.error("failed to get thread with id \(anchorId) from server")
                    completionHandler(.failure(error))
                case .success(let serverThread):
                    guard let serverThread else {
                        self.log.warning("no such thread with id \(anchorId) from server")
                        completionHandler(.success(()))
                        return
                    }
                    self.syncThreadInteractions(serverThread: serverThread) { syncResult in
                        switch syncResult {
                        case .failure(let err):
                            self.log.error("failed to sync interactions in \(anchor.rawValue) \(anchorId): \(err.localizedDescription)")
                            completionHandler(.failure(err))
                        case .success:
                            completionHandler(.success(()))
                        }
                    }
                }
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
