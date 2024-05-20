import Foundation

let GroupLastInteractionSyncLimit = 50

extension SHInteractionsSyncOperation {
    
    /// Determine the full set of unique group ids from the descriptor and call `syncGroupInteractions(groupIds:qos:)`
    /// to sync the interactions in these groups
    /// - Parameters:
    ///   - remoteDescriptors: the descriptors
    ///   - qos: the thread priority
    ///   - completionHandler: the callback
    public func syncGroupInteractions(
        remoteDescriptors: [any SHAssetDescriptor],
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        ///
        /// Extract unique group ids from the descriptors
        ///
        let allSharedGroupIds = Array(remoteDescriptors.reduce([String: Int](), { partialResult, descriptor in
            var result = partialResult
            
            var isShared = false
            let userIdsSharedWith: Set<String> = Set(descriptor.sharingInfo.sharedWithUserIdentifiersInGroup.keys)
            let myUserId = self.user.identifier
            if descriptor.sharingInfo.sharedByUserIdentifier == myUserId {
                if userIdsSharedWith.subtracting([myUserId]).count > 0 {
                    isShared = true
                }
            } else if userIdsSharedWith.contains(myUserId) {
                isShared = true
            }
            
            guard isShared else {
                /// If not shared, do nothing (do not update the partial result)
                return result
            }
            
            for (userId, groupId) in descriptor.sharingInfo.sharedWithUserIdentifiersInGroup {
                if userId != self.user.identifier {
                    result[groupId] = 1
                }
            }
            return result
        }).keys)
        
        self.syncGroupInteractions(groupIds: allSharedGroupIds, qos: qos) { result in
            if case .failure(let err) = result {
                self.log.error("failed to sync interactions: \(err.localizedDescription)")
                completionHandler(.failure(err))
            }
            else {
                completionHandler(.success(()))
            }
        }
    }
    
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
        log.debug("[sync] syncing interactions in group \(groupId)")
        
        let dispatchGroup = DispatchGroup()
        var errors = [Error]()
        
        ///
        /// Retrieve the REMOTE interactions
        ///
        
        var remoteInteractions: InteractionsGroupDTO? = nil
        dispatchGroup.enter()
        self.serverProxy.retrieveRemoteInteractions(
            inGroup: groupId,
            ofType: nil,
            underMessage: nil,
            before: nil,
            limit: GroupLastInteractionSyncLimit
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
        serverProxy.retrieveLocalInteractions(
            inGroup: groupId,
            ofType: nil,
            underMessage: nil,
            before: nil,
            limit: GroupLastInteractionSyncLimit
        ) { localResult in
            switch localResult {
            case .failure(let err):
                if case SHBackgroundOperationError.missingE2EEDetailsForGroup(_) = err {
                    shouldCreateE2EEDetailsLocally = true
                } else {
                    self.log.error("failed to retrieve local interactions for groupId \(groupId): \(err.localizedDescription)")
                    errors.append(err)
                }
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
