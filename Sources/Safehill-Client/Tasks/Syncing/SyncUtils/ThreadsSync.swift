import Foundation

let ThreadLastInteractionSyncLimit = 20

extension SHInteractionsSyncOperation {
    
    /// Sync the threads betwen remote and local server
    /// and returns the list of threads that have been synced, filtering out the ones where users are unauthorized.
    ///
    /// - Parameter qos: the quality of service
    /// - Returns: the list of threads from known users
    internal func syncThreads(
        qos: DispatchQoS.QoSClass
    ) async throws -> [ConversationThreadOutputDTO] {
        
        return try await withUnsafeThrowingContinuation { continuation in
            
            let dispatchGroup = DispatchGroup()
            var remoteError: Error? = nil, localError: Error? = nil
            var allThreads = [ConversationThreadOutputDTO]()
            var localThreads = [ConversationThreadOutputDTO]()
            
            ///
            /// Pull all threads from REMOTE
            ///
            dispatchGroup.enter()
            self.serverProxy.listThreads(filteringUnknownUsers: false) { result in
                switch result {
                case .success(let threadList):
                    allThreads = threadList
                case .failure(let err):
                    remoteError = err
                }
                dispatchGroup.leave()
            }
            
            ///
            /// Pull all threads from LOCAL
            ///
            dispatchGroup.enter()
            self.serverProxy.listLocalThreads { result in
                switch result {
                case .success(let threadList):
                    localThreads = threadList
                case .failure(let err):
                    localError = err
                }
                dispatchGroup.leave()
            }
            
            dispatchGroup.notify(queue: .global(qos: qos)) {
                guard remoteError == nil else {
                    self.log.error("[sync] error getting all threads from server. \(remoteError!.localizedDescription)")
                    
                    continuation.resume(throwing: remoteError!)
                    return
                }
                
                guard localError == nil else {
                    self.log.error("[sync] error getting local threads. \(localError!.localizedDescription)")
                    
                    continuation.resume(throwing: localError!)
                    return
                }
                
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
                self.syncThreads(
                    remoteThreads: allThreads,
                    localThreads: localThreads,
                    qos: qos
                ) { result in
                    switch result {
                        
                    case .failure(let error):
                        continuation.resume(throwing: error)
                        
                    case .success(let threadsFromKnownUsers):
                        continuation.resume(returning: threadsFromKnownUsers)
                    }
                }
            }
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
    ///   - completionHandler: the callback method
    internal func syncThreads(
        remoteThreads: [ConversationThreadOutputDTO],
        localThreads: [ConversationThreadOutputDTO]? = nil,
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<[ConversationThreadOutputDTO], Error>) -> Void
    ) {
        ///
        /// Request authorization for unknown users that messaged this user.
        /// Don't filter out threads where messages were sent by this user,
        /// in case these threads weren't created yet on this device
        ///
        self.serverProxy.filterThreadsCreatedByUnknownUsers(
            remoteThreads,
            filterIfThisUserHasSentMessages: false
        ) { result in
            
            switch result {
                
            case .failure(let error):
                self.log.critical("some threads were received from server, but the client could not determine if the creators are known: \(error.localizedDescription). ASSUMING KNOWN")
                let interactionsSyncDelegates = self.interactionsSyncDelegates
                self.delegatesQueue.async {
                    interactionsSyncDelegates.forEach({ delegate in
                        remoteThreads.forEach({ thread in
                            delegate.didAddThread(thread)
                        })
                    })
                }
                completionHandler(.failure(error))
                
            case .success(let threadsFromKnownUsers):
                let threadIdsFromknownUsers = threadsFromKnownUsers.map({ $0.threadId })
                let threadsFromUnknownUsers = remoteThreads.filter({
                    threadIdsFromknownUsers.contains($0.threadId) == false
                })
                var unauthorizedUsers = Set(threadsFromUnknownUsers.compactMap({ $0.creatorPublicIdentifier }))
                unauthorizedUsers.remove(self.user.identifier)
                
                let unauthorizedUsersImmutable = unauthorizedUsers
                
                ///
                /// Create the local thread if it doesn't exist
                ///
                
                Task {
                    let createdThreads = await self.serverProxy.createThreadsLocallyIfMissing(
                        threadsFromKnownUsers,
                        localThreads: localThreads
                    )
                    
                    completionHandler(.success(createdThreads))
                    
                    ///
                    /// Handle the ones from AUTHORIZED creators
                    ///
                    
                    let interactionsSyncDelegates = self.interactionsSyncDelegates
                    self.delegatesQueue.async {
                        interactionsSyncDelegates.forEach({ delegate in
                            createdThreads.forEach({ createdThread in
                                delegate.didAddThread(createdThread)
                            })
                        })
                    }
                    
                    if unauthorizedUsersImmutable.isEmpty == false {
                        
                        ///
                        /// Handle the ones from UNAUTHORIZED creators
                        ///
                        
                        do {
                            let usersDict = try await SHUsersController(localUser: self.user).getUsersOrCached(
                                with: Array(unauthorizedUsersImmutable)
                            )
                            self.delegatesQueue.async {
                                interactionsSyncDelegates.forEach({
                                    $0.didReceiveTextMessagesFromUnauthorized(users: unauthorizedUsersImmutable.compactMap({ uid in usersDict[uid] }))
                                })
                            }
                        } catch {
                            self.log.error("failed to retrieve unauthorized users \(unauthorizedUsersImmutable). \(error.localizedDescription)")
                        }
                    }
                }
            }
        }
    }
    
    
    ///
    /// Add the last message pulled from the summary to the thread
    ///
    internal func updateThreadsInteractions(using summaryByThreadId: [String: InteractionsThreadSummaryDTO]) {
        
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
