import Foundation

extension SHSyncOperation {
    
    ///
    /// Best attempt to sync the interactions from the server to the local server proxy by calling SHUserInteractionController::retrieveInteractions
    ///
    /// - Parameter descriptorsByGlobalIdentifier: the descriptors retrieved from server, from which to collect all unique groups
    ///
    func syncGroupInteractions(
        groupIds: [String],
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        var error: Error? = nil
        ///
        /// For each group …
        ///
        let dispatchGroup = DispatchGroup()
        for groupId in groupIds {
            dispatchGroup.enter()
            self.syncGroupInteractions(groupId: groupId, qos: qos) { result in
                if case .failure(let err) = result {
                    self.log.error("error syncing interactions in group \(groupId). \(err.localizedDescription)")
                    error = err
                }
                dispatchGroup.leave()
            }
        }
        
        dispatchGroup.notify(queue: .global(qos: qos)) {
            if let error {
                completionHandler(.failure(error))
            } else {
                completionHandler(.success(()))
            }
        }
    }
    
    func syncGroupInteractions(
        groupId: String,
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        let dispatchGroup = DispatchGroup()
        var errors = [Error]()
        
        ///
        /// Retrieve the REMOTE interactions
        ///
        
        var remoteInteractions: InteractionsGroupDTO? = nil
        dispatchGroup.enter()
        self.serverProxy.retrieveRemoteInteractions(
            inGroup: groupId,
            underMessage: nil,
            per: 10000,
            page: 1
        ) { result in
            switch result {
            case .failure(let err):
                errors.append(err)
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
            inGroup: groupId,
            underMessage: nil,
            per: 10000, 
            page: 1
        ) { localResult in
            switch localResult {
            case .failure(let err):
                if case SHBackgroundOperationError.missingE2EEDetailsForGroup(_) = err {
                    shouldCreateE2EEDetailsLocally = true
                } else {
                    errors.append(err)
                }
                self.log.error("failed to retrieve local interactions for groupId \(groupId): \(err.localizedDescription)")
            case .success(let localInteractions):
                localMessages = localInteractions.messages
                localReactions = localInteractions.reactions
            }
            dispatchGroup.leave()
        }
        
        dispatchGroup.notify(queue: .global(qos: qos)) {
            guard errors.isEmpty else {
                self.log.warning("error syncing interactions for group \(groupId). \(errors.map({ $0.localizedDescription }))")
                completionHandler(.failure(errors.first!))
                return
            }
            guard let remoteInteractions else {
                self.log.warning("error retrieving remote interactions for group \(groupId)")
                completionHandler(.failure(SHBackgroundOperationError.fatalError("error retrieving interactions for group \(groupId)")))
                return
            }
            
            ///
            /// Add the E2EE encryption details for the group locally if missing
            ///
            
            if shouldCreateE2EEDetailsLocally {
                let recipientEncryptionDetails = RecipientEncryptionDetailsDTO(
                    recipientUserIdentifier: self.user.identifier,
                    ephemeralPublicKey: remoteInteractions.ephemeralPublicKey,
                    encryptedSecret: remoteInteractions.encryptedSecret,
                    secretPublicSignature: remoteInteractions.secretPublicSignature,
                    senderPublicSignature: remoteInteractions.senderPublicSignature
                )
                
                dispatchGroup.enter()
                self.serverProxy.localServer.setGroupEncryptionDetails(
                    groupId: groupId,
                    recipientsEncryptionDetails: [recipientEncryptionDetails]
                ) { setE2EEDetailsResult in
                    switch setE2EEDetailsResult {
                    case .success(_):
                        break
                    case .failure(let err):
                        self.log.critical("Cache interactions for group \(groupId) won't be readable because setting the E2EE details for such group failed: \(err.localizedDescription)")
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
                    anchor: .group,
                    anchorId: groupId,
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
                    anchor: .group,
                    anchorId: groupId,
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
                        self.log.warning("error while syncing messages and reactions retrieved from server on local for group \(groupId): \(errors.map({ $0.localizedDescription }))")
                        completionHandler(.failure(errors.first!))
                        return
                    }
                    
                    completionHandler(.success(()))
                }
            }
        }
    }
}
