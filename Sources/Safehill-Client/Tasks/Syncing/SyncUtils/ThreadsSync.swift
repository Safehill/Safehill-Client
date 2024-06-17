import Foundation

let ThreadLastInteractionSyncLimit = 20

extension SHInteractionsSyncOperation {
    
    /// Sync the threads betwen remote and local server
    /// and returns the list of threads that have been synced, filtering out the ones where users are unauthorized.
    ///
    /// - Parameter qos: the quality of service
    /// - Returns: the list of threads from known users
    public func syncThreads(
        qos: DispatchQoS.QoSClass
    ) async throws {
        
        let allThreads = try await self.serverProxy.listThreads()
        let localThreads = try await self.serverProxy.listLocalThreads()
        
        ///
        /// Remove extra threads locally, and notify the delegates
        ///
        var threadIdsToRemoveLocally = [String]()
        for localThread in localThreads {
            if allThreads.contains(where: { $0.threadId == localThread.threadId }) == false {
                threadIdsToRemoveLocally.append(localThread.threadId)
            }
        }
        
        if threadIdsToRemoveLocally.isEmpty == false {
            var removedCount = 0
            let removalDispatchGroup = DispatchGroup()
            for threadIdToRemoveLocally in threadIdsToRemoveLocally {
                removalDispatchGroup.enter()
                self.serverProxy.localServer.deleteThread(withId: threadIdToRemoveLocally) { result in
                    if case .success = result {
                        removedCount += 1
                    }
                    removalDispatchGroup.leave()
                }
            }
            removalDispatchGroup.notify(queue: .global(qos: qos)) {
                self.log.info("threads to remove: \(threadIdsToRemoveLocally.count), removed: \(removedCount)")
                
                let remainingThreads = localThreads.filter({ threadIdsToRemoveLocally.contains($0.threadId) == false })
                
                let interactionsSyncDelegates = self.interactionsSyncDelegates
                self.delegatesQueue.async {
                    interactionsSyncDelegates.forEach({ $0.didUpdateThreadsList(remainingThreads) })
                }
            }
        }
        
        ///
        /// Create threads locally as needed, and notify the delegates
        ///
        await self.syncThreads(
            remoteThreads: allThreads,
            localThreads: localThreads
        )
    }
    
    ///
    /// Determine if the creators are authorized users,
    /// If they aren't these threads should be ignored.
    /// If they are, then create them locally if they don't exist, and notify the delegates
    ///
    /// - Parameters:
    ///   - threads: the threads from server
    ///   - localThreads: the local threads if already available. If `nil`, the corresponding local threads will be fetched from DB
    ///   - completionHandler: the callback method
    internal func syncThreads(
        remoteThreads: [ConversationThreadOutputDTO],
        localThreads: [ConversationThreadOutputDTO]? = nil
    ) async {
        let createdThreads = await self.serverProxy.createThreadsLocallyIfMissing(
            remoteThreads,
            localThreads: localThreads
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
    /// Add the last message pulled from the summary to the thread
    ///
    internal func updateThreadsInteractions(
        using summaryByThreadId: [String: InteractionsThreadSummaryDTO]
    ) {
        
        for (threadId, threadSummary) in summaryByThreadId {
            if let lastMessage = threadSummary.lastEncryptedMessage {
                self.serverProxy.addLocalMessages(
                    [lastMessage],
                    inThread: threadId
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
                inGroup: groupId
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
                    inGroup: groupId
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
