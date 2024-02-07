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
    
    let assetsDelegates: [SHAssetSyncingDelegate]
    let threadsDelegates: [SHThreadSyncingDelegate]
    
    var serverProxy: SHServerProxy {
        SHServerProxy(user: self.user)
    }
    
    public init(
        user: SHLocalUser,
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
    
    private func syncDescriptors(
        _ remoteDescriptors: [any SHAssetDescriptor],
        completionHandler: @escaping (Result<AssetDescriptorsDiff, Error>) -> ()
    ) {
        var localDescriptors = [any SHAssetDescriptor]()
        var localError: Error? = nil
        var remoteUsersById = [UserIdentifier: SHServerUser]()
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
        var userIdsInLocalDescriptorsSet = Set<UserIdentifier>()
        for localDescriptor in localDescriptors {
            userIdsInLocalDescriptorsSet.insert(localDescriptor.sharingInfo.sharedByUserIdentifier)
            localDescriptor.sharingInfo.sharedWithUserIdentifiersInGroup.keys.forEach({ userIdsInLocalDescriptorsSet.insert($0) })
        }
        userIdsInLocalDescriptorsSet.remove(self.user.identifier)
        let userIdsInLocalDescriptors = Array(userIdsInLocalDescriptorsSet)
        
        var userIdsInRemoteDescriptorsSet = Set<UserIdentifier>()
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
                remoteUsersById = serverUsers.reduce([:], { partialResult, serverUser in
                    var result = partialResult
                    result[serverUser.identifier] = serverUser
                    return result
                })
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
            log.info("removing user ids from local store and the graph \(uIdsToRemoveFromLocal)")
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
        /// Get all the asset identifiers and user identifiers mentioned in the remote descriptors
        ///
        let assetIdToUserIds = remoteDescriptors
            .reduce([GlobalIdentifier: [SHServerUser]]()) { partialResult, descriptor in
                var result = partialResult
                var userIdList = Array(descriptor.sharingInfo.sharedWithUserIdentifiersInGroup.keys)
                userIdList.append(descriptor.sharingInfo.sharedByUserIdentifier)
                result[descriptor.globalIdentifier] = userIdList.compactMap({ remoteUsersById[$0] })
                return result
            }
        self.assetsDelegates.forEach({
            $0.assetIdsAreVisibleToUsers(assetIdToUserIds)
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
            try downloadsManager.cleanEntriesNotIn(allSharedAssetIds: Array(assetIdToUserIds.keys),
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
            self.assetsDelegates.forEach({
                $0.shareHistoryQueueItemsChanged(withIdentifiers: queueDiff.changed)
            })
        }
        if queueDiff.removed.count > 0 {
            self.log.debug("[sync] notifying queue items removed \(queueDiff.removed)")
            self.assetsDelegates.forEach({
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
                            self.assetsDelegates.forEach {
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
                            self.assetsDelegates.forEach {
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
                    /// Remove items in DOWNLOAD queues and indices that no longer exist
                    ///
                    do {
                        let downloadsManager = SHAssetsDownloadManager(user: self.user)
                        try downloadsManager.cleanEntries(for: diff.assetsRemovedOnServer.map({ $0.globalIdentifier }))
                    } catch {
                        let _ = try? SHDBManager.sharedInstance.graph().removeAll()
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
                    
                    do {
                        try SHKGQuery.removeAssets(with: diff.assetsRemovedOnServer.compactMap({ $0.globalIdentifier }))
                    } catch {
                        let _ = try? SHDBManager.sharedInstance.graph().removeAll()
                        self.log.error("[sync] error removing deleted assets from the graph. Removing all triples in the graph and re-building it")
                    }
                    
                    self.log.debug("[sync] notifying delegates about deleted assets \(diff.assetsRemovedOnServer)")
                    self.assetsDelegates.forEach({
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
        self.syncGroupInteractions(remoteDescriptors: remoteDescriptors) { result in
            if case .failure(let err) = result {
                self.log.error("failed to sync interactions: \(err.localizedDescription)")
                interactionsSyncError = err
            }
            group.leave()
        }
        
        group.enter()
        self.syncThreadInteractions(remoteDescriptors: remoteDescriptors) { result in
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
