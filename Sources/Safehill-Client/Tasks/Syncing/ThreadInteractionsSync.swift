import Foundation

extension SHSyncOperation {
    
    public func runOnceForThreads(qos: DispatchQoS.QoSClass, completionHandler: @escaping (Result<Void, Error>) -> Void) {
        self.syncThreadInteractions(qos: qos) { result in
            if case .failure(let err) = result {
                self.log.error("failed to sync interactions: \(err.localizedDescription)")
                completionHandler(.failure(err))
            }
            completionHandler(.success(()))
        }
    }
    
    ///
    /// Best attempt to sync the interactions from the server to the local server proxy by calling SHUserInteractionController::retrieveInteractions
    ///
    /// - Parameter descriptorsByGlobalIdentifier: the descriptors retrieved from server, from which to collect all unique groups
    ///
    func syncThreadInteractions(
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        let dispatchGroup = DispatchGroup()
        var error: Error? = nil
        var allThreads = [ConversationThreadOutputDTO]()
        var localThreads = [ConversationThreadOutputDTO]()
        
        ///
        /// Pull all threads
        ///
        dispatchGroup.enter()
        self.serverProxy.listThreads { result in
            switch result {
            case .success(let threadList):
                allThreads = threadList
            case .failure(let err):
                error = err
            }
            dispatchGroup.leave()
        }
        
        dispatchGroup.enter()
        self.serverProxy.localServer.listThreads { result in
            switch result {
            case .success(let threadList):
                localThreads = threadList
            case .failure(let err):
                error = err
            }
            dispatchGroup.leave()
        }
        
        dispatchGroup.notify(queue: .global(qos: qos)) {
            guard error == nil else {
                self.log.error("[sync] error getting all threads. \(error!.localizedDescription)")
                completionHandler(.failure(error!))
                return
            }
            
            ///
            /// Remove extra threads locally
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
                removalDispatchGroup.notify(queue: .global(qos: .background)) {
                    self.log.info("threads to remove: \(threadIdsToRemoveLocally.count), removed: \(removedCount)")
                }
            }
            
            ///
            /// Add remote threads locally and sync their interactions.
            /// Max date of local threads is the **last known date**
            ///
            var lastKnownThreadUpdateAt: Date? = localThreads
                .sorted(by: {
                    let a = ($0.lastUpdatedAt?.iso8601withFractionalSeconds ?? .distantPast)
                    let b = ($1.lastUpdatedAt?.iso8601withFractionalSeconds ?? .distantPast)
                    return a.compare(b) == .orderedAscending
                })
                .last?
                .lastUpdatedAt?
                .iso8601withFractionalSeconds
            if lastKnownThreadUpdateAt == .distantPast {
                lastKnownThreadUpdateAt = nil
            }
            
            let syncInteractionsInThread = { (thread: ConversationThreadOutputDTO) in
                self.syncThreadInteractions(serverThread: thread, qos: qos) { result in
                    if case .failure(let err) = result {
                        self.log.error("error syncing interactions in thread \(thread.threadId). \(err.localizedDescription)")
                    }
                    dispatchGroup.leave()
                }
            }
            
            for thread in allThreads {
                if let lastKnown = lastKnownThreadUpdateAt,
                   let lastUpdated = thread.lastUpdatedAt?.iso8601withFractionalSeconds,
                   lastUpdated.compare(lastKnown) == .orderedAscending {
                    continue
                }
                
                dispatchGroup.enter()
                
                if localThreads.contains(where: { $0.threadId == thread.threadId }) == false {
                    self.serverProxy.localServer.createOrUpdateThread(serverThread: thread) { createResult in
                        switch createResult {
                        case .success:
                            syncInteractionsInThread(thread)
                        case .failure(let err):
                            self.log.error("error locally creating thread \(thread.threadId). \(err.localizedDescription)")
                        }
                    }
                } else {
                    syncInteractionsInThread(thread)
                }
            }
            
            let threadsDelegates = self.threadsDelegates
            dispatchGroup.notify(queue: .global()) {
                self.delegatesQueue.async {
                    threadsDelegates.forEach({ $0.didUpdateThreadsList(allThreads) })
                }
                completionHandler(.success(()))
            }
        }
    }
    
    func syncThreadInteractions(
        serverThread: ConversationThreadOutputDTO,
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
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
            underMessage: nil,
            before: nil,
            limit: 20
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
        serverProxy.retrieveInteractions(
            inThread: threadId,
            underMessage: nil,
            before: nil,
            limit: 20
        ) { localResult in
            switch localResult {
            case .failure(let err):
                if case SHBackgroundOperationError.missingE2EEDetailsForThread(_) = err {
                    shouldCreateE2EEDetailsLocally = true
                } else {
                    error = err
                }
                self.log.error("failed to retrieve local interactions for thread \(threadId)")
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
            guard let remoteInteractions = remoteInteractions else {
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
}
