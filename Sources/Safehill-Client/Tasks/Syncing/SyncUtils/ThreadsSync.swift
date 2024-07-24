import Foundation

let ThreadLastInteractionSyncLimit = 20

extension SHInteractionsSyncOperation {
    
    public func syncInteractionSummaries() async throws {
        ///
        /// Get the summary to update the latest messages and interactions
        /// in threads and groups
        let remoteSummary = try await self.serverProxy.topLevelInteractionsSummaryFromRemote()
        
        self.delegatesQueue.async { [weak self] in
            self?.interactionsSyncDelegates.forEach {
                $0.didFetchRemoteThreadSummary(remoteSummary.summaryByThreadId)
                $0.didFetchRemoteGroupSummary(remoteSummary.summaryByGroupId)
            }
        }
        
        let localSummary = try await self.serverProxy.topLevelInteractionsSummaryFromLocal()
        
        ///
        /// Sync the threads (creates, removals)
        /// based on the list from server
        ///
        try await self.syncRemoteAndLocalThreads(
            remoteSummary: remoteSummary,
            localSummary: localSummary,
            qos: .userInteractive
        )
        
        self.updateThreadsInteractions(using: remoteSummary.summaryByThreadId)
        self.updateGroupsInteractions(using: remoteSummary.summaryByGroupId)
    }
    
    /// Sync the threads betwen remote and local server
    /// and returns the list of threads that have been synced, filtering out the ones where users are unauthorized.
    ///
    /// - Parameter qos: the quality of service
    /// - Returns: the list of threads from known users
    func syncRemoteAndLocalThreads(
        remoteSummary: InteractionsSummaryDTO,
        localSummary: InteractionsSummaryDTO,
        qos: DispatchQoS.QoSClass
    ) async throws {
        
        let remoteThreadIds = remoteSummary.summaryByThreadId.keys
        let localThreadIds = localSummary.summaryByThreadId.keys
        let missingThreadIdsLocally = Set(remoteThreadIds).subtracting(localThreadIds)
        let extraThreadIdsLocally = Set(localThreadIds).subtracting(remoteThreadIds)
        let intersection = Set(remoteThreadIds).intersection(localThreadIds)
        
        let remoteThreads = remoteSummary.summaryByThreadId.values.map({ $0.thread })
        let localThreads = localSummary.summaryByThreadId.values.map({ $0.thread })
        
        ///
        /// Update the `lastUpdatedAt` on the ones existing locally
        ///
        if intersection.isEmpty == false {
            do {
                try await self.updateLastUpdatedAt(
                    for: Array(intersection),
                    allRemoteThreads: remoteThreads
                )
            } catch {
                log.error("[thread-sync] failed to update lastUpdated date on local threads: \(error.localizedDescription)")
            }
        }
        
        ///
        /// Remove locally the ones removed on remote
        ///
        if extraThreadIdsLocally.isEmpty == false {
            var removedCount = 0
            let removalDispatchGroup = DispatchGroup()
            for threadIdToRemoveLocally in extraThreadIdsLocally {
                removalDispatchGroup.enter()
                self.serverProxy.localServer.deleteThread(withId: threadIdToRemoveLocally) { result in
                    if case .success = result {
                        removedCount += 1
                    }
                    removalDispatchGroup.leave()
                }
            }
            removalDispatchGroup.notify(queue: .global(qos: qos)) {
                self.log.info("threads to remove: \(extraThreadIdsLocally.count), removed: \(removedCount)")
                
                let remainingThreads = localThreads.filter({ extraThreadIdsLocally.contains($0.threadId) == false })
                
                let interactionsSyncDelegates = self.interactionsSyncDelegates
                self.delegatesQueue.async {
                    interactionsSyncDelegates.forEach({ $0.didUpdateThreadsList(remainingThreads) })
                }
            }
        }
        
        ///
        /// Add locally the ones added on remote
        ///
        if missingThreadIdsLocally.isEmpty == false {
            ///
            /// Create threads locally as needed, and notify the delegates
            ///
            await self.createThreads(
                withIds: Array(missingThreadIdsLocally),
                remoteThreads: remoteThreads
            )
        }
    }
    
    ///
    /// Determine if the creators are authorized users,
    /// If they aren't these threads should be ignored.
    /// If they are, then create them locally if they don't exist, and notify the delegates
    ///
    /// - Parameters:
    ///   - threads: the threads from server
    ///   - localThreads: the local threads if already available. If `nil`, the corresponding local threads will be fetched from DB
    internal func createThreads(
        withIds threadIds: [String],
        remoteThreads: [ConversationThreadOutputDTO]
    ) async {
        let notYetOnLocal = remoteThreads.filter({ threadIds.contains($0.threadId) })
        
        let createdThreads = await self.serverProxy.createThreadsLocally(
            notYetOnLocal
        )
        
        let interactionsSyncDelegates = self.interactionsSyncDelegates
        self.delegatesQueue.async {
            interactionsSyncDelegates.forEach({ delegate in
                createdThreads.forEach({ createdThread in
                    delegate.didAddThread(createdThread)
                })
            })
        }
    }
    
    ///
    /// Determine if the creators are authorized users,
    /// If they aren't these threads should be ignored.
    /// If they are, then create them locally if they don't exist, and notify the delegates
    ///
    /// - Parameters:
    ///   - threads: the threads from server
    ///   - remoteThreads: the local threads if already available. If `nil`, the corresponding local threads will be fetched from DB
    internal func updateLastUpdatedAt(
        for threadIds: [String],
        allRemoteThreads: [ConversationThreadOutputDTO]
    ) async throws {
        let remoteThreads = allRemoteThreads.filter({
            threadIds.contains($0.threadId)
        })
        try await self.serverProxy.updateLastUpdatedAt(from: remoteThreads)
    }
    
    ///
    /// Add the last message pulled from the summary to the thread
    ///
    internal func updateThreadsInteractions(
        using summaryByThreadId: [String: InteractionsThreadSummaryDTO]
    ) {
        for (threadId, threadSummary) in summaryByThreadId {
            if let lastMessage = threadSummary.lastEncryptedMessage {
                self.serverProxy.addLocalMessages(
                    [lastMessage],
                    toThread: threadId
                ) { result in
                    guard case .success(let messages) = result else {
                        return
                    }
                    
                    let interactionsSyncDelegates = self.interactionsSyncDelegates
                    self.delegatesQueue.async {
                        interactionsSyncDelegates.forEach({ delegate in
                            delegate.didReceiveTextMessages(messages, inThread: threadId)
                        })
                    }
                }
            }
        }
    }
    
    ///
    /// Add all the interactions referenced in the summary
    ///
    internal func updateGroupsInteractions(using summaryByGroupId: [String: InteractionsGroupSummaryDTO]) {
        for (groupId, groupSummary) in summaryByGroupId {
            
            self.serverProxy.addLocalReactions(
                groupSummary.reactions,
                toGroup: groupId
            ) { result in
                guard case .success = result else {
                    return
                }
                
                let interactionsSyncDelegates = self.interactionsSyncDelegates
                self.delegatesQueue.async {
                    interactionsSyncDelegates.forEach({ delegate in
                        delegate.reactionsDidChange(inGroup: groupId)
                    })
                }
            }
            
            if let firstMessage = groupSummary.firstEncryptedMessage {
                self.serverProxy.addLocalMessages(
                    [firstMessage],
                    toGroup: groupId
                ) { result in
                    guard case .success(let messages) = result else {
                        return
                    }
                    
                    let interactionsSyncDelegates = self.interactionsSyncDelegates
                    self.delegatesQueue.async {
                        interactionsSyncDelegates.forEach({ delegate in
                            delegate.didReceiveTextMessages(messages, inGroup: groupId)
                        })
                    }
                }
            }
        }
    }
}
