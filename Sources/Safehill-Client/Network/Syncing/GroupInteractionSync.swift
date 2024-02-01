import Foundation

extension SHSyncOperation {
    
    ///
    /// Best attempt to sync the interactions from the server to the local server proxy by calling SHUserInteractionController::retrieveInteractions
    ///
    /// - Parameter descriptorsByGlobalIdentifier: the descriptors retrieved from server, from which to collect all unique groups
    ///
    func syncGroupInteractions(
        remoteDescriptors: [any SHAssetDescriptor],
        completionHandler: @escaping (Result<Void, Error>) -> ()
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
        
        let dispatchGroup = DispatchGroup()
        var error: Error? = nil
        
        ///
        /// For each group …
        ///
        
        for groupId in allSharedGroupIds {
            
            var remoteInteractions: InteractionsGroupDTO? = nil
            
            ///
            /// Retrieve the REMOTE interactions
            ///
            
            dispatchGroup.enter()
            self.serverProxy.retrieveRemoteInteractions(
                inGroup: groupId,
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
                inGroup: groupId,
                underMessage: nil,
                per: 10000, page: 1
            ) { localResult in
                switch localResult {
                case .failure(let err):
                    if case SHBackgroundOperationError.missingE2EEDetailsForGroup(_) = err {
                        shouldCreateE2EEDetailsLocally = true
                    }
                    self.log.error("failed to retrieve local interactions for groupId \(groupId)")
                case .success(let localInteractions):
                    localMessages = localInteractions.messages
                    localReactions = localInteractions.reactions
                }
                dispatchGroup.leave()
            }
            
            var dispatchResult = dispatchGroup.wait(timeout: .now() + .milliseconds(SHDefaultNetworkTimeoutInMilliseconds))
            guard dispatchResult == .success else {
                completionHandler(.failure(SHBackgroundOperationError.timedOut))
                return
            }
            guard error == nil else {
                log.error("error syncing interactions for group \(groupId). \(error!.localizedDescription)")
                continue
            }
            guard let remoteInteractions = remoteInteractions else {
                log.error("error retrieving remote interactions for group \(groupId)")
                continue
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
                serverProxy.localServer.setGroupEncryptionDetails(
                    groupId: groupId,
                    recipientsEncryptionDetails: [recipientEncryptionDetails]
                ) { setE2EEDetailsResult in
                    switch setE2EEDetailsResult {
                    case .success(_):
                        break
                    case .failure(let err):
                        self.log.error("Cache interactions for group \(groupId) won't be readable because setting the E2EE details for such group failed: \(err.localizedDescription)")
                    }
                    dispatchGroup.leave()
                }
            }
            
            dispatchResult = dispatchGroup.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds))
            if dispatchResult != .success {
                log.warning("timeout while setting E2EE details for groupId \(groupId)")
            }
            
            do {
                ///
                /// Sync (create, update and delete) reactions
                ///
                
                let remoteReactions = remoteInteractions.reactions
                try self.syncReactions(
                    anchor: .group,
                    anchorId: groupId,
                    localReactions: localReactions,
                    remoteReactions: remoteReactions
                )
                
                ///
                /// Sync (create, update and delete) messages
                ///
                
                let remoteMessages = remoteInteractions.messages
                try self.syncMessages(
                    anchor: .group,
                    anchorId: groupId,
                    localMessages: localMessages,
                    remoteMessages: remoteMessages
                )
            } catch {
                log.warning("error while syncing messages and reactions retrieved from server on local")
            }
        }
        
        completionHandler(.success(()))
    }
}
