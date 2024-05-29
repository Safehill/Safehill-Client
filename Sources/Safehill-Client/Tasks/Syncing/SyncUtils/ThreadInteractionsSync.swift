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
                self.syncThreadsFromAuthorizedUsers(
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
    internal func syncThreadsFromAuthorizedUsers(
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
    
    /// Sync the last `ThreadLastInteractionSyncLimit` interactions in a specific thread
    /// - Parameters:
    ///   - thread: the thread
    ///   - qos: the quality of service
    ///   - completionHandler: the callback
    func syncThreadInteractions(
        in thread: ConversationThreadOutputDTO,
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        log.debug("[sync] syncing interactions in thread \(thread.threadId)")
        
        let threadId = thread.threadId
        
        let dispatchGroup = DispatchGroup()
        var error: Error? = nil
        
        ///
        /// Retrieve the REMOTE interactions
        ///
        var remoteInteractions: InteractionsGroupDTO? = nil
        dispatchGroup.enter()
        self.serverProxy.retrieveRemoteInteractions(
            inThread: threadId,
            ofType: nil,
            underMessage: nil,
            before: nil,
            limit: ThreadLastInteractionSyncLimit
        ) { result in
            switch result {
            case .failure(let err):
                error = err
            case .success(let interactions):
                remoteInteractions = interactions
            }
            dispatchGroup.leave()
        }
        
        ///
        /// Retrieve the LOCAL interactions
        ///
        var shouldCreateE2EEDetailsLocally = false
        var localMessages = [MessageOutputDTO]()
        var localReactions = [ReactionOutputDTO]()
        
        dispatchGroup.enter()
        serverProxy.retrieveLocalInteractions(
            inThread: threadId,
            ofType: nil,
            underMessage: nil,
            before: nil,
            limit: ThreadLastInteractionSyncLimit
        ) { localResult in
            switch localResult {
            case .failure(let err):
                if case SHBackgroundOperationError.missingE2EEDetailsForThread(_) = err {
                    shouldCreateE2EEDetailsLocally = true
                } else {
                    error = err
                    self.log.error("failed to retrieve local interactions for thread \(threadId)")
                }
            case .success(let localInteractions):
                localMessages = localInteractions.messages
                localReactions = localInteractions.reactions
            }
            dispatchGroup.leave()
        }
        
        dispatchGroup.notify(queue: .global(qos: qos)) {
            guard error == nil else {
                self.log.error("error syncing interactions for thread \(threadId). \(error!.localizedDescription)")
                completionHandler(.failure(error!))
                return
            }
            guard let remoteInteractions else {
                self.log.error("error retrieving remote interactions for thread \(threadId)")
                completionHandler(.failure(SHBackgroundOperationError.fatalError("error retrieving interactions for thread \(threadId)")))
                return
            }
            
            ///
            /// Add the E2EE encryption details for the group locally if missing
            ///
            
            if shouldCreateE2EEDetailsLocally {
                dispatchGroup.enter()
                self.serverProxy.localServer.createOrUpdateThread(
                    serverThread: thread
                ) { threadCreateResult in
                    switch threadCreateResult {
                    case .success(_):
                        break
                    case .failure(let err):
                        self.log.error("Cache interactions for thread \(threadId) won't be readable because setting the E2EE details for such thread failed: \(err.localizedDescription)")
                    }
                    dispatchGroup.leave()
                }
            }
            
            dispatchGroup.notify(queue: .global(qos: qos)) {
                
                var errors = [Error]()
                
                ///
                /// Sync (create, update and delete) reactions
                ///
                
                let remoteReactions = remoteInteractions.reactions
                dispatchGroup.enter()
                self.syncReactions(
                    anchor: .thread,
                    anchorId: threadId,
                    localReactions: localReactions,
                    remoteReactions: remoteReactions,
                    qos: qos
                ) { result in
                    if case .failure(let err) = result {
                        errors.append(err)
                    }
                    dispatchGroup.leave()
                }
                
                ///
                /// Sync (create, update and delete) messages
                ///
                
                let encryptionDetails = EncryptionDetailsClass(
                    ephemeralPublicKey: remoteInteractions.ephemeralPublicKey,
                    encryptedSecret: remoteInteractions.encryptedSecret,
                    secretPublicSignature: remoteInteractions.secretPublicSignature,
                    senderPublicSignature: remoteInteractions.senderPublicSignature
                )
                let remoteMessages = remoteInteractions.messages
                dispatchGroup.enter()
                self.syncMessages(
                    anchor: .thread,
                    anchorId: threadId,
                    localMessages: localMessages,
                    remoteMessages: remoteMessages,
                    encryptionDetails: encryptionDetails,
                    qos: qos
                ) { result in
                    if case .failure(let err) = result {
                        errors.append(err)
                    }
                    dispatchGroup.leave()
                }
                
                dispatchGroup.notify(queue: .global(qos: qos)) {
                    guard errors.isEmpty else {
                        self.log.warning("error while syncing messages and reactions retrieved from server on local for thread \(threadId): \(errors.map({ $0.localizedDescription }))")
                        completionHandler(.failure(errors.first!))
                        return
                    }
                    
                    completionHandler(.success(()))
                }
            }
        }
    }
}
