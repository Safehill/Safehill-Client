import Foundation

let ThreadLastInteractionSyncLimit = 20

extension SHInteractionsSyncOperation {
    
    public func syncThreads(qos: DispatchQoS.QoSClass, completionHandler: @escaping (Result<Void, Error>) -> Void) {
        self.syncLastThreadInteractions(qos: qos) { result in
            if case .failure(let err) = result {
                self.log.error("failed to sync interactions: \(err.localizedDescription)")
                completionHandler(.failure(err))
            }
            completionHandler(.success(()))
        }
    }
    
    ///
    /// Update the list of threads locally by fetching the latest from remote
    ///
    func syncLastThreadInteractions(
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
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
                
                let interactionsSyncDelegates = self.interactionsSyncDelegates
                self.delegatesQueue.async {
                    interactionsSyncDelegates.forEach({ $0.didUpdateThreadsList(localThreads) })
                }
                
                completionHandler(.failure(remoteError!))
                return
            }
            
            guard localError == nil else {
                self.log.error("[sync] error getting local threads. \(localError!.localizedDescription)")
                completionHandler(.failure(localError!))
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
                    completionHandler(.failure(error))
                
                case .success(let threadsFromKnownUsers):
                    Task {
                        let dispatchGroup = DispatchGroup()
                        
                        ///
                        /// Sync last interactions for the threads from authorized users
                        ///
                        threadsFromKnownUsers.forEach({ thread in
                            dispatchGroup.enter()
                            self.syncThreadInteractions(serverThread: thread, qos: qos) { result in
                                if case .failure(let err) = result {
                                    self.log.error("error syncing interactions in thread \(thread.threadId). \(err.localizedDescription)")
                                }
                                
                                self.syncThreadAssets(serverThread: thread, qos: qos) { result in
                                    if case .failure(let err) = result {
                                        self.log.error("error syncing assets in thread \(thread.threadId). \(err.localizedDescription)")
                                    }
                                    
                                    dispatchGroup.leave()
                                }
                            }
                        })
                        
                        dispatchGroup.notify(queue: .global(qos: qos)) {
                            completionHandler(.success(()))
                        }
                    }
                }
            }
        }
    }
    
    func syncThreadInteractions(
        serverThread: ConversationThreadOutputDTO,
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        log.debug("[sync] syncing interactions in thread \(serverThread.threadId)")
        
        let threadId = serverThread.threadId
        
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
                    serverThread: serverThread
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
                
                ///
                /// Create the local thread from the provided thread if it doesn't exist
                ///
                
                self.createThreadsLocally(
                    threadsFromKnownUsers,
                    localThreads: localThreads,
                    qos: qos
                ) {
                    completionHandler(.success(threadsFromKnownUsers))
                }
                
                ///
                /// Handle the ones from AUTHORIZED creators
                ///
                let interactionsSyncDelegates = self.interactionsSyncDelegates
                self.delegatesQueue.async {
                    interactionsSyncDelegates.forEach({ delegate in
                        threadsFromKnownUsers.forEach({ threadFromKnownUser in
                            delegate.didAddThread(threadFromKnownUser)
                        })
                    })
                }
                
                ///
                /// Handle the ones from UNAUTHORIZED creators
                ///
                if unauthorizedUsers.isEmpty == false {
                    let unauthorizedUsersImmutable = unauthorizedUsers
                    SHUsersController(localUser: self.user).getUsersOrCached(
                        with: Array(unauthorizedUsers)
                    ) { result in
                        switch result {
                        
                        case .success(let usersDict):
                            let interactionsSyncDelegates = self.interactionsSyncDelegates
                            self.delegatesQueue.async {
                                interactionsSyncDelegates.forEach({
                                    $0.didReceiveMessagesFromUnauthorized(users: unauthorizedUsersImmutable.compactMap({ uid in usersDict[uid] }))
                                })
                            }

                        case .failure(let error):
                            self.log.error("failed to retrieve unauthorized users \(unauthorizedUsers). \(error.localizedDescription)")
                        }
                    }
                }
            }
        }
    }
    
    private func createThreadsLocally(
        _ threadsToCreate: [ConversationThreadOutputDTO],
        localThreads: [ConversationThreadOutputDTO]?,
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping () -> Void
    ) {
        var notYetOnLocal: [ConversationThreadOutputDTO] = threadsToCreate
        
        if let localThreads {
            let localThreadIds = localThreads.map({ $0.threadId })
            notYetOnLocal = threadsToCreate.filter({ localThreadIds.contains($0.threadId) == false })
        } else {
            self.serverProxy.listLocalThreads(
                withIdentifiers: threadsToCreate.map({ $0.threadId })
            ) { getThreadsResult in
                
                switch getThreadsResult {
                    
                case .failure(let failure):
                    self.log.error("failed to get local threads when syncing. Assuming these threads don't exist")
                    
                case .success(let localThreads):
                    let localThreadIds = localThreads.map({ $0.threadId })
                    notYetOnLocal = threadsToCreate.filter({ localThreadIds.contains($0.threadId) == false })
                }
            }
        }
        
        let dispatchGroup = DispatchGroup()
        for threadToCreateLocally in notYetOnLocal {
            dispatchGroup.enter()
            self.serverProxy.localServer.createOrUpdateThread(
                serverThread: threadToCreateLocally
            ) { createResult in
                if case .failure(let error) = createResult {
                    self.log.error("failed to create thread locally. \(error.localizedDescription)")
                }
                dispatchGroup.leave()
            }
        }
        
        dispatchGroup.notify(queue: .global(qos: qos)) {
            completionHandler()
        }
    }
}
