import Foundation
import KnowledgeBase
import os


///
/// ** ASSETS SYNC OPERATION **
///
/// Generates a diff between local and corresponding remote descriptors. Based on the diff:
/// - Removes assets only on local
/// - Updates assets on both that are different on remote
///
///
public class SHAssetsSyncOperation: Operation, SHBackgroundOperationProtocol, @unchecked Sendable {
    
    public let log = Logger(subsystem: "com.safehill", category: "BG-ASSETS-SYNC")
    
    let delegatesQueue = DispatchQueue(label: "com.safehill.sync.delegates")
    
    let user: SHAuthenticatedLocalUser
    
    let assetsDelegates: [SHAssetSyncingDelegate]
    
    var serverProxy: SHServerProxy { user.serverProxy }
    
    public init(
        user: SHAuthenticatedLocalUser,
        assetsDelegates: [SHAssetSyncingDelegate]
    ) {
        self.user = user
        self.assetsDelegates = assetsDelegates
    }
    
    public func sync(
        remoteAndLocalDescriptors: [any SHAssetDescriptor],
        localDescriptors: [any SHAssetDescriptor],
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
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
        /// 4. The group information is missing or different on remote
        ///     -> Remove or update the local DB
        /// 5. The local upload state doesn't match the remote state
        ///     -> inefficient solution is to verify the asset is in S3. Efficient is to trust the value from the server
        ///
        
        let diff = AssetDescriptorsDiff.generateUsing(
            remote: remoteAndLocalDescriptors,
            local: localDescriptors,
            for: self.user
        )
        
        let dispatchGroup = DispatchGroup()
        
        ///
        /// Remove assets that are no longer returned by the remote server
        ///
        if diff.assetsRemovedOnRemote.isEmpty == false {
            let globalIdentifiers = diff.assetsRemovedOnRemote.compactMap { $0.globalIdentifier }
            dispatchGroup.enter()
            self.serverProxy.localServer.deleteAssets(
                withGlobalIdentifiers: globalIdentifiers
            ) { [weak self] result in
                
                guard let self = self else {
                    dispatchGroup.leave()
                    return
                }
                
                switch result {
                case .failure(let error):
                    self.log.error("[sync] some assets were deleted on server but couldn't be deleted from local cache. This operation will be attempted again, but for now the cache is out of sync. error=\(error.localizedDescription)")
                case .success:
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
                
                dispatchGroup.leave()
            }
        }
        
        ///
        /// Change the upload state of the assets that are not in sync with server states
        ///
        for stateChangeDiff in diff.stateDifferentOnRemote {
            dispatchGroup.enter()
            self.serverProxy.localServer.markAsset(
                with: stateChangeDiff.globalIdentifier,
                quality: stateChangeDiff.quality,
                as: stateChangeDiff.newUploadState
            ) { [weak self] result in
                
                guard let self = self else {
                    dispatchGroup.leave()
                    return
                }
                
                if case .failure(let error) = result {
                    self.log.error("[sync] some assets were marked as \(stateChangeDiff.newUploadState.rawValue) on server but not in the local cache. This operation will be attempted again, but for now the cache is out of sync. error=\(error.localizedDescription)")
                }
                
                dispatchGroup.leave()
            }
        }
        
        ///
        /// Remove groupIds from the asset store that no longer exist
        ///
        if diff.groupInfoRemovedOnRemote.isEmpty == false {
            dispatchGroup.enter()
            self.serverProxy.localServer.removeGroupIds(
                diff.groupInfoRemovedOnRemote
            ) { [weak self] result in
                
                guard let self = self else {
                    dispatchGroup.leave()
                    return
                }
                
                switch result {
                case .success:
                    let assetsDelegates = self.assetsDelegates
                    self.delegatesQueue.async {
                        assetsDelegates.forEach {
                            $0.groupsWereRemoved(withIds: diff.groupInfoRemovedOnRemote)
                        }
                    }
                    
                case .failure(let error):
                    self.log.error("[sync] failed to remove group ids removed on remote. \(error.localizedDescription)")
                }
                
                dispatchGroup.leave()
            }
        }
        
        ///
        /// Update groupIds in the asset store that are different on server
        ///
        if diff.groupInfoDifferentOnRemote.isEmpty == false {
            dispatchGroup.enter()
            self.serverProxy.localServer.updateGroupIds(
                diff.groupInfoDifferentOnRemote
            ) { [weak self] result in
                
                guard let self = self else {
                    dispatchGroup.leave()
                    return
                }
                
                switch result {
                case .success:
                    let assetsDelegates = self.assetsDelegates
                    self.delegatesQueue.async {
                        assetsDelegates.forEach {
                            $0.groupsInfoWereUpdated(diff.groupInfoDifferentOnRemote.mapValues({ $0.groupInfo }))
                        }
                    }
                    
                case .failure(let error):
                    self.log.error("[sync] failed to update group ids from remote. \(error.localizedDescription)")
                }
                
                dispatchGroup.leave()
            }
        }
        
        ///
        /// Add users to the shares in the local server, in the graph
        /// and notify the delegates so that the UI gets updated
        ///
        if diff.userGroupChangesByAssetGid.isEmpty == false {
            dispatchGroup.enter()
            self.serverProxy.localServer.updateUserGroupInfo(
                basedOn: diff.userGroupChangesByAssetGid,
                versions: nil
            ) { [weak self] result in
                
                guard let self = self else {
                    dispatchGroup.leave()
                    return
                }
                
                switch result {
                case .success:
                    let assetsDelegates = self.assetsDelegates
                    self.delegatesQueue.async {
                        for (globalIdentifier, sharingInfo) in diff.userGroupChangesByAssetGid {
                            assetsDelegates.forEach {
                                $0.groupUserSharingInfoChanged(
                                    forAssetWith: globalIdentifier,
                                    sharingInfo: sharingInfo
                                )
                            }
                        }
                    }
                case .failure(let error):
                    self.log.error("[sync] some users were added to a share on server but the local DB couldn't be updated. This operation will be attempted again, but for now the cache is out of sync. error=\(error.localizedDescription)")
                }
                dispatchGroup.leave()
            }
        }
        
        if diff.userGroupRemovalsByAssetGid.isEmpty == false {
            dispatchGroup.enter()
            self.serverProxy.localServer.removeAssetRecipients(
                basedOn: diff.userGroupRemovalsByAssetGid.mapValues({ Array($0.keys) }),
                versions: nil
            ) { [weak self] result in
                guard let self = self else {
                    return
                }
                
                switch result {
                case .success:
                    let assetsDelegates = self.assetsDelegates
                    self.delegatesQueue.async {
                        for (globalIdentifier, userGroupRemovals) in diff.userGroupRemovalsByAssetGid {
                            assetsDelegates.forEach {
                                $0.usersWereRemovedFromShare(
                                    of: globalIdentifier,
                                    userGroupRemovals
                                )
                            }
                        }
                    }
                case .failure(let error):
                    self.log.error("[sync] some users were removed from a share on server but the local DB couldn't be updated. This operation will be attempted again, but for now the cache is out of sync. error=\(error.localizedDescription)")
                }
                dispatchGroup.leave()
            }
        }
        
        dispatchGroup.notify(queue: .global(qos: qos)) {
            completionHandler(.success(()))
        }
    }
    
    public func run(qos: DispatchQoS.QoSClass, completionHandler: @escaping (Result<Void, Error>) -> Void) {
        ///
        /// Get the descriptors from the local server
        ///
        self.serverProxy.getLocalAssetDescriptors(after: nil) { localResult in
            switch localResult {
            case .success(let localDescriptors):
                let localDescriptorsGids = localDescriptors.map({ $0.globalIdentifier })
                ///
                /// Get the corresponding descriptors from the server
                ///
                self.serverProxy.getRemoteAssetDescriptors(
                    for: localDescriptorsGids,
                    after: nil
                ) { remoteResult in
                    switch remoteResult {
                    case .success(let remoteAndLocalDescriptors):
                        ///
                        /// Start the sync process
                        ///
                        self.sync(remoteAndLocalDescriptors: remoteAndLocalDescriptors,
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
}

public let AssetsSyncProcessor = SHBackgroundOperationProcessor<SHAssetsSyncOperation>()
