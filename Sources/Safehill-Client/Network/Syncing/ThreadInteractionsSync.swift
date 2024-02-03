import Foundation

extension SHSyncOperation {
    
    ///
    /// Best attempt to sync the interactions from the server to the local server proxy by calling SHUserInteractionController::retrieveInteractions
    ///
    /// - Parameter descriptorsByGlobalIdentifier: the descriptors retrieved from server, from which to collect all unique groups
    ///
    func syncThreadInteractions(
        remoteDescriptors: [any SHAssetDescriptor],
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
        
        let dispatchResult1 = dispatchGroup.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds))
        guard dispatchResult1 == .success else {
            log.error("[sync] timeout while getting all threads")
            completionHandler(.failure(SHBackgroundOperationError.timedOut))
            return
        }
        guard error == nil else {
            log.error("[sync] error getting all threads. \(error!.localizedDescription)")
            completionHandler(.failure(error!))
            return
        }
        
        self.threadsDelegates.forEach({ $0.didUpdateThreadsList(allThreads) })
        
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
        /// For each thread …
        ///
        
        for thread in allThreads {
            
            var remoteInteractions: InteractionsGroupDTO? = nil
            
            ///
            /// Retrieve the REMOTE interactions
            ///
            
            dispatchGroup.enter()
            self.serverProxy.retrieveRemoteInteractions(
                inThread: thread.threadId,
                underMessage: nil,
                per: 1000, page: 1
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
                inThread: thread.threadId,
                underMessage: nil,
                per: 10000, page: 1
            ) { localResult in
                switch localResult {
                case .failure(let err):
                    if case SHBackgroundOperationError.missingE2EEDetailsForThread(_) = err {
                        shouldCreateE2EEDetailsLocally = true
                    }
                    self.log.error("failed to retrieve local interactions for thread \(thread.threadId)")
                case .success(let localInteractions):
                    localMessages = localInteractions.messages
                    localReactions = localInteractions.reactions
                }
                dispatchGroup.leave()
            }
            
            let dispatchResult2 = dispatchGroup.wait(timeout: .now() + .milliseconds(SHDefaultNetworkTimeoutInMilliseconds))
            guard dispatchResult2 == .success else {
                completionHandler(.failure(SHBackgroundOperationError.timedOut))
                return
            }
            guard error == nil else {
                log.error("error syncing interactions for thread \(thread.threadId). \(error!.localizedDescription)")
                continue
            }
            guard let remoteInteractions = remoteInteractions else {
                log.error("error retrieving remote interactions for thread \(thread.threadId)")
                continue
            }
            
            ///
            /// Add the E2EE encryption details for the group locally if missing
            ///
            
            if shouldCreateE2EEDetailsLocally {
                dispatchGroup.enter()
                serverProxy.localServer.createOrUpdateThread(
                    serverThread: thread
                ) { threadCreateResult in
                    switch threadCreateResult {
                    case .success(_):
                        break
                    case .failure(let err):
                        self.log.error("Cache interactions for thread \(thread.threadId) won't be readable because setting the E2EE details for such thread failed: \(err.localizedDescription)")
                    }
                    dispatchGroup.leave()
                }
            }
            
            let dispatchResult3 = dispatchGroup.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds))
            if dispatchResult3 != .success {
                log.warning("timeout while setting E2EE details for thread \(thread.threadId)")
            }
            
            do {
                ///
                /// Sync (create, update and delete) reactions
                ///
                
                let remoteReactions = remoteInteractions.reactions
                try self.syncReactions(
                    anchor: .thread,
                    anchorId: thread.threadId,
                    localReactions: localReactions,
                    remoteReactions: remoteReactions
                )
                
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
                try self.syncMessages(
                    anchor: .thread,
                    anchorId: thread.threadId,
                    localMessages: localMessages,
                    remoteMessages: remoteMessages,
                    encryptionDetails: encryptionDetails
                )
            } catch {
                log.warning("error while syncing messages and reactions retrieved from server on local")
            }
        }
        
        completionHandler(.success(()))
    }
}
