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
    
    public func sync(
        remoteAndLocalDescriptors: [any SHAssetDescriptor],
        localDescriptors: [any SHAssetDescriptor],
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
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
        
        let diff = AssetDescriptorsDiff.generateUsing(
            remote: remoteAndLocalDescriptors,
            local: localDescriptors,
            for: self.user
        )
        
        ///
        /// Remove users that should be removed from local server
        ///
        if diff.assetsRemovedOnRemote.count > 0 {
            let globalIdentifiers = diff.assetsRemovedOnRemote.compactMap { $0.globalIdentifier }
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
                            for: diff.assetsRemovedOnRemote.map({ $0.globalIdentifier })
                        )
                    } catch {
                        self.log.error("[sync] failed to clean up download queues and index on deleted assets: \(error.localizedDescription)")
                    }
                    
                    ///
                    /// Remove items in the UPLOAD and SHARE queues that no longer exist
                    ///
                    do {
                        try SHQueueOperation.removeItems(correspondingTo: diff.assetsRemovedOnRemote.compactMap({ $0.localIdentifier }))
                    } catch {
                        self.log.error("[sync] failed to clean up UPLOAD and SHARE queues on deleted assets: \(error.localizedDescription)")
                    }
                    
                    self.log.debug("[sync] notifying delegates about deleted assets \(diff.assetsRemovedOnRemote)")
                    
                    let assetsDelegates = self.assetsDelegates
                    self.delegatesQueue.async {
                        assetsDelegates.forEach({
                            $0.assetsWereDeleted(diff.assetsRemovedOnRemote)
                        })
                    }
                }
            }
        }
        
        ///
        /// Change the upload state of the assets that are not in sync with server states
        ///
        for stateChangeDiff in diff.stateDifferentOnRemote {
            self.serverProxy.localServer.markAsset(with: stateChangeDiff.globalIdentifier,
                                                   quality: stateChangeDiff.quality,
                                                   as: stateChangeDiff.newUploadState) { result in
                if case .failure(let error) = result {
                    self.log.error("some assets were marked as \(stateChangeDiff.newUploadState.rawValue) on server but not in the local cache. This operation will be attempted again, but for now the cache is out of sync. error=\(error.localizedDescription)")
                }
            }
        }
        
        guard diff.userIdsAddedToTheShareOfAssetGid.count > 0 else {
            completionHandler(.success(()))
            return
        }
        
        let dispatchGroup = DispatchGroup()
        var error: Error? = nil
        
        ///
        /// Add users to the shares in the graph and notify the delegates
        ///
        dispatchGroup.enter()
        self.serverProxy.localServer.addAssetRecipients(
            basedOn: diff.userIdsAddedToTheShareOfAssetGid
        ) { result in
            switch result {
            case .success():
                
                /// ** !!!!!!!!!! **
                /// ** !!!!!!!!!! **
                /// ** !!!!!!!!!! **
                // TODO: Add recipients to the queue items (add a method in UserSync)
                /// ** !!!!!!!!!! **
                /// ** !!!!!!!!!! **
                /// ** !!!!!!!!!! **
                
                let assetsDelegates = self.assetsDelegates
                self.delegatesQueue.async {
                    for (globalIdentifier, shareDiff) in diff.userIdsAddedToTheShareOfAssetGid {
                        assetsDelegates.forEach {
                            $0.usersWereAddedToShare(
                                of: globalIdentifier,
                                groupIdByRecipientId: shareDiff.groupIdByRecipientId,
                                groupInfoById: shareDiff.groupInfoById
                            )
                        }
                    }
                }
            case .failure(let err):
                error = err
            }
            dispatchGroup.leave()
        }
        
        dispatchGroup.notify(queue: .global(qos: qos)) {
            guard error == nil else {
                self.log.error("[sync] failed to add recipients to some shares: \(error!.localizedDescription)")
                completionHandler(.failure(error!))
                return
            }
            
            guard diff.userIdsRemovedFromTheSharesOfAssetGid.count > 0 else {
                completionHandler(.success(()))
                return
            }
            
            var error: Error? = nil
            
            dispatchGroup.enter()
            self.serverProxy.localServer.removeAssetRecipients(
                basedOn: diff.userIdsRemovedFromTheSharesOfAssetGid
            ) { result in
                switch result {
                case .success:
                    
                    /// ** !!!!!!!!!! **
                    /// ** !!!!!!!!!! **
                    /// ** !!!!!!!!!! **
                    // TODO: Remove recipients to the queue items (adapt `UserSync::removeUsersFromShareHistoryQueueItems`)
                    /// ** !!!!!!!!!! **
                    /// ** !!!!!!!!!! **
                    /// ** !!!!!!!!!! **
                    
                    let assetsDelegates = self.assetsDelegates
                    self.delegatesQueue.async {
                        for (globalIdentifier, shareDiff) in diff.userIdsRemovedFromTheSharesOfAssetGid {
                            assetsDelegates.forEach {
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
            
            dispatchGroup.notify(queue: .global(qos: qos)) {
                if let error {
                    self.log.error("[sync] failed to remove recipients from some shares: \(error)")
                }
                
                completionHandler(.success(()))
            }
        }
    }
    
    private func runOnce(qos: DispatchQoS.QoSClass, completionHandler: @escaping (Result<Void, Error>) -> Void) {
        
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
                        self.sync(remoteAndLocalDescriptors: remoteDescriptors,
                                  localDescriptors: localDescriptors,
                                  qos: qos,
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
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        switch anchor {
        case .group:
            self.syncGroupInteractions(groupId: anchorId, qos: qos) { result in
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
                    self.syncThreadInteractions(serverThread: serverThread, qos: qos) { syncResult in
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
        
        self.runOnce(qos: .background) { result in
            self.state = .finished
        }
    }
}
