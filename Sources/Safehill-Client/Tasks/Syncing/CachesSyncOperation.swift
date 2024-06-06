import Foundation
import KnowledgeBase
import os


public class SHCachesSyncOperation: Operation, SHBackgroundOperationProtocol {
    
    public let log = Logger(subsystem: "com.safehill", category: "BG-LP-SYNC")
    
    let delegatesQueue = DispatchQueue(label: "com.safehill.low-prio.sync.delegates")
    
    let user: SHAuthenticatedLocalUser
    
    let assetsSyncDelegates: [SHAssetSyncingDelegate]
    
    public init(
        user: SHAuthenticatedLocalUser,
        assetsSyncDelegates: [SHAssetSyncingDelegate]
    ) {
        self.user = user
        self.assetsSyncDelegates = assetsSyncDelegates
    }
    
    var serverProxy: SHServerProxy { self.user.serverProxy }
    
    private func uniqueUserIds(in descriptors: [any SHAssetDescriptor]) -> Set<UserIdentifier> {
        var userIdsDescriptorsSet = Set<UserIdentifier>()
        for descriptor in descriptors {
            userIdsDescriptorsSet.insert(descriptor.sharingInfo.sharedByUserIdentifier)
            descriptor.sharingInfo.sharedWithUserIdentifiersInGroup.keys.forEach({ userIdsDescriptorsSet.insert($0) })
        }
        return userIdsDescriptorsSet
    }
    
    private func fetchDescriptors(
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<(
            remote: [any SHAssetDescriptor],
            local: [any SHAssetDescriptor]
        ), Error>) -> Void
    ) {
        let dispatchGroup = DispatchGroup()
        
        var localDescriptors = [any SHAssetDescriptor]()
        var remoteDescriptors = [any SHAssetDescriptor]()
        var localError: Error? = nil
        var remoteError: Error? = nil
        
        ///
        /// Get all asset descriptors associated with this user from the server.
        ///
        dispatchGroup.enter()
        self.serverProxy.getRemoteAssetDescriptors(after: nil) { remoteResult in
            switch remoteResult {
            case .success(let descriptors):
                remoteDescriptors = descriptors
            case .failure(let err):
                self.log.error("[lowPrioritySyncOperation] failed to fetch descriptors from REMOTE server when syncing: \(err.localizedDescription)")
                remoteError = err
            }
            dispatchGroup.leave()
        }
        
        ///
        /// Get all the local descriptors.
        ///
        dispatchGroup.enter()
        self.serverProxy.getLocalAssetDescriptors(after: nil) { localResult in
            switch localResult {
            case .success(let descriptors):
                localDescriptors = descriptors
            case .failure(let err):
                self.log.error("[lowPrioritySyncOperation] failed to fetch descriptors from LOCAL server when syncing: \(err.localizedDescription)")
                localError = err
            }
            dispatchGroup.leave()
        }
        
        dispatchGroup.notify(queue: .global(qos: qos)) {
            guard remoteError == nil else {
                completionHandler(.failure(remoteError!))
                return
            }
            guard localError == nil else {
                completionHandler(.failure(localError!))
                return
            }
            
            completionHandler(.success((
                remote: remoteDescriptors,
                local: localDescriptors
            )))
        }
    }
    
    
    /// Given the full set of descriptors on the remote server and the full set on the local server:
    /// - Removes users that are only on the local server from
    ///     - The local server
    ///     - The user cache
    ///     - The graph
    ///     - The blacklist
    ///     - The user authorization queue
    /// - Notifies the delegate about which assets are linked to which users
    ///
    /// - Parameters:
    ///   - allRemoteDescriptors: the full set of unfiltered descriptors from the remote server
    ///   - allLocalDescriptors: the full set of unfiltered descriptors from the local server
    ///   - completionHandler: the callback
    private func syncCaches(
        allRemoteDescriptors: [any SHAssetDescriptor],
        allLocalDescriptors: [any SHAssetDescriptor],
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) { 
        ///
        /// Get all users referenced in either local or remote descriptors (excluding THIS user)
        ///
        var userIdsInLocalDescriptorsSet = self.uniqueUserIds(in: allLocalDescriptors)
        userIdsInLocalDescriptorsSet.remove(self.user.identifier)
        
        var userIdsInRemoteDescriptorsSet = self.uniqueUserIds(in: allRemoteDescriptors)
        userIdsInRemoteDescriptorsSet.remove(self.user.identifier)
        let userIdsInRemoteDescriptors = Array(userIdsInRemoteDescriptorsSet)
        
        ///
        /// Get the `SHServerUser` for each of the users mentioned in the remote descriptors
        ///
        SHUsersController(localUser: self.user).getUsers(withIdentifiers: userIdsInRemoteDescriptors) {
            result in
            switch result {
            case .failure(let error):
                completionHandler(.failure(error))
            case .success(let remoteUsersById):
                ///
                /// Don't consider users that can't be retrieved by the `SHUserController`.
                /// This is just an extra measure on the client in case the server returns users that are deleted or deactivated.
                ///
                userIdsInRemoteDescriptorsSet = userIdsInRemoteDescriptorsSet.intersection(remoteUsersById.keys)
                
                ///
                /// Remove all users that no longer exist on the server from the local server and the graph
                ///
                let uIdsToRemoveFromLocal = Array(userIdsInLocalDescriptorsSet.subtracting(userIdsInRemoteDescriptorsSet))
                if uIdsToRemoveFromLocal.count > 0 {
                    self.log.info("removing user ids from local store and the graph \(uIdsToRemoveFromLocal)")
                    do {
                        /// ** !!!!!!!!!! **
                        /// ** !!!!!!!!!! **
                        /// ** !!!!!!!!!! **
                        // TODO: Re-enable this
                        /// ** !!!!!!!!!! **
                        /// ** !!!!!!!!!! **
                        /// ** !!!!!!!!!! **
        //                try SHUsersController(localUser: self.user).deleteUsers(withIdentifiers: uIdsToRemoveFromLocal)
                    } catch {
                        self.log.warning("error removing local users, but this operation will be retried")
                    }
                }
                
                ///
                /// Get all the asset identifiers and user identifiers mentioned in the remote descriptors
                ///
                let assetIdToUserIds = allRemoteDescriptors
                    .reduce([GlobalIdentifier: [any SHServerUser]]()) { partialResult, descriptor in
                        var result = partialResult
                        var userIdList = Array(descriptor.sharingInfo.sharedWithUserIdentifiersInGroup.keys)
                        userIdList.append(descriptor.sharingInfo.sharedByUserIdentifier)
                        result[descriptor.globalIdentifier] = userIdList.compactMap({ remoteUsersById[$0] })
                        return result
                    }
                
                let assetsDelegates = self.assetsSyncDelegates
                self.delegatesQueue.async {
                    assetsDelegates.forEach({
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
                Task(priority: qos.toTaskPriority()) {
                    await SHDownloadBlacklist.shared.removeFromBlacklistIfNotIn(
                        userIdentifiers: userIdsInRemoteDescriptors
                    )
                }
                
                do {
                    try SHAssetsDownloadManager.cleanEntriesNotIn(
                        allSharedAssetIds: Array(assetIdToUserIds.keys),
                        allUserIds: userIdsInRemoteDescriptors
                    )
                    completionHandler(.success(()))
                } catch {
                    self.log.error("failed to clean up download queues and index on deleted assets: \(error.localizedDescription)")
                    completionHandler(.failure(error))
                }
            }
        }
    }
    
    public func run(qos: DispatchQoS.QoSClass, completionHandler: @escaping (Result<Void, Error>) -> Void) {
        DispatchQueue.global(qos: qos).async {
            self.fetchDescriptors(qos: qos) {
                (result: Result<(
                    remote: [any SHAssetDescriptor],
                    local: [any SHAssetDescriptor]
                ), Error>) in
                switch result {
                case .success(let descriptors):
                    self.syncCaches(
                        allRemoteDescriptors: descriptors.remote,
                        allLocalDescriptors: descriptors.local,
                        qos: qos,
                        completionHandler: completionHandler
                    )
                case .failure(let error):
                    completionHandler(.failure(error))
                }
            }
        }
    }
}

public let CachesSyncProcessor = SHBackgroundOperationProcessor<SHCachesSyncOperation>()
