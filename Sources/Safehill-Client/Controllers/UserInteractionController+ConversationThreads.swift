import Foundation
import Safehill_Crypto
import CryptoKit

extension SHUserInteractionController {
    
    public func getThread(
        withId threadId: String,
        completionHandler: @escaping (Result<ConversationThreadOutputDTO?, Error>) -> ()
    ) {
        self.serverProxy.getThread(
            withId: threadId,
            completionHandler: completionHandler
        )
    }
    
    internal func getExistingThread(
        with users: [any SHServerUser],
        and phoneNumbers: [String],
        completionHandler: @escaping (Result<ConversationThreadOutputDTO?, Error>) -> Void
    ) {
        self.serverProxy.getThread(withUsers: users, and: phoneNumbers) { result in
            switch result {
            case .failure(let error):
                log.error("failed to fetch thread with users \(users.map({ $0.identifier })) and phone numbers \(phoneNumbers) from remote server")
                completionHandler(.failure(error))
            case .success(let conversationThread):
                completionHandler(.success(conversationThread))
            }
        }
    }
    
    public func setupThread(
        with usersAndSelf: [any SHServerUser],
        and phoneNumbers: [String],
        completionHandler: @escaping (Result<ConversationThreadOutputDTO, Error>) -> Void
    ) {
        guard usersAndSelf.count > 1 || phoneNumbers.count > 0 else {
            completionHandler(.failure(SHInteractionsError.noSafehillUsersInThread))
            return
        }
        
        E2eCreationSerialQueue.async {
            guard let authedUser = self.user as? SHAuthenticatedLocalUser else {
                completionHandler(.failure(SHLocalUserError.notAuthenticated))
                return
            }
            
            guard usersAndSelf.contains(where: { $0.identifier == authedUser.identifier }) else {
                completionHandler(.failure(SHBackgroundOperationError.fatalError("users can only create groups they are part of")))
                return
            }
            
            self.getExistingThread(with: usersAndSelf, and: phoneNumbers) { result in
                switch result {
                case .failure(let error):
                    log.error("failed to fetch thread with users \(usersAndSelf.map({ $0.identifier })) and phone numbers \(phoneNumbers) from remote server")
                    completionHandler(.failure(error))
                case .success(let conversationThread):
                    let symmetricKey: SymmetricKey
                    
                    if let conversationThread {
                        log.info("found thread with users \(usersAndSelf.map({ $0.identifier })) and phone numbers \(phoneNumbers) from local or remote")
                        completionHandler(.success(conversationThread))
                    } else {
                        log.info("creating new thread, because one could not be found on remote with users \(usersAndSelf.map({ $0.identifier })) and phone numbers \(phoneNumbers)")
                        symmetricKey = createNewSecret()
                        
                        do {
                            let recipientsEncryptionDetails = try newRecipientEncryptionDetails(
                                from: symmetricKey,
                                for: usersAndSelf,
                                anchor: .thread,
                                anchorId: conversationThread?.threadId
                            )
                            log.debug("generated recipients encryptionDetails \(recipientsEncryptionDetails.map({ "R=\($0.recipientUserIdentifier) ES=\($0.encryptedSecret), EPK=\($0.ephemeralPublicKey) SSig=\($0.secretPublicSignature) USig=\($0.senderPublicSignature)" }))")
                            log.info("creating or updating threads on server with recipient encryption details for users \(recipientsEncryptionDetails.map({ $0.recipientUserIdentifier })) and phone numbers \(phoneNumbers)")
                            self.serverProxy.createOrUpdateThread(
                                name: nil,
                                recipientsEncryptionDetails: recipientsEncryptionDetails,
                                invitedPhoneNumbers: phoneNumbers,
                                completionHandler: completionHandler
                            )
                        } catch {
                            log.critical("""
failed to initialize E2EE details for thread \(conversationThread?.threadId ?? "<NEW>"). error=\(error.localizedDescription)
""")
                            completionHandler(.failure(error))
                            return
                        }
                    }
                }
            }
        }
    }
    
    public func updateThread(
        _ threadId: String,
        newName: String?,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        guard self.user as? SHAuthenticatedLocalUser != nil else {
            completionHandler(.failure(SHLocalUserError.notAuthenticated))
            return
        }
        
        self.serverProxy.updateThread(
            threadId,
            newName: newName,
            completionHandler: completionHandler
        )
    }
    
    public func updateThreadMembers(
        for threadId: String,
        recipientsToAdd: [UserIdentifier],
        membersPublicIdentifierToRemove: [UserIdentifier],
        phoneNumbersToAdd: [String],
        phoneNumbersToRemove: [String],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        guard let user = self.user as? SHAuthenticatedLocalUser else {
            completionHandler(.failure(SHLocalUserError.notAuthenticated))
            return
        }
        
        let updateMembers = { (newUsersEncryptionDetails: [RecipientEncryptionDetailsDTO]) in
            let update = ConversationThreadMembersUpdateDTO(
                recipientsToAdd: newUsersEncryptionDetails,
                membersPublicIdentifierToRemove: membersPublicIdentifierToRemove,
                phoneNumbersToAdd: phoneNumbersToAdd,
                phoneNumbersToRemove: phoneNumbersToRemove
            )
            
            self.serverProxy.updateThreadMembers(
                for: threadId,
                update,
                completionHandler: completionHandler
            )
        }
        
        if recipientsToAdd.isEmpty {
            ///
            /// If no members to add, no need to fetch the symmetric encryption key for the thread
            /// and encrypt it for the new recipients
            ///
            updateMembers([])
        } else {
            let symmetricKey: SymmetricKey?
            do {
                symmetricKey = try self.fetchSymmetricKey(forAnchor: .thread, anchorId: threadId)
            } catch {
                symmetricKey = nil
            }
            
            if let symmetricKey {
                Task {
                    let users = try await SHUsersController(localUser: user).getUsers(withIdentifiers: recipientsToAdd)
                    guard users.count == recipientsToAdd.count else {
                        completionHandler(.failure(SHInteractionsError.failedToFetchUsers))
                        return
                    }
                    
                    let newRecipientsEncryptionDetails = try newRecipientEncryptionDetails(
                        from: symmetricKey,
                        for: Array(users.values),
                        anchor: .thread,
                        anchorId: threadId
                    )
                    
                    updateMembers(newRecipientsEncryptionDetails)
                    
                    completionHandler(.success(()))
                }
            } else {
                completionHandler(.failure(SHInteractionsError.noSuchThread))
            }
        }
    }
    
    public func deleteThread(threadId: String, completionHandler: @escaping (Result<Void, Error>) -> ()) {
        guard self.user as? SHAuthenticatedLocalUser != nil else {
            completionHandler(.failure(SHLocalUserError.notAuthenticated))
            return
        }
        
        SHUserInteractionController.encryptionDetailsCache.evict(anchor: .thread, anchorId: threadId)
        self.serverProxy.deleteThread(withId: threadId, completionHandler: completionHandler)
    }
    
    public func leaveThread(threadId: String, completionHandler: @escaping (Result<Void, Error>) -> ()) {
        self.removeUser(self.user.identifier, from: threadId, completionHandler: completionHandler)
    }
    
    public func removeUser(
        _ userIdentifier: UserIdentifier,
        from threadId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        guard self.user as? SHAuthenticatedLocalUser != nil else {
            completionHandler(.failure(SHLocalUserError.notAuthenticated))
            return
        }
        
        self.getThread(withId: threadId) { getThreadResult in
            switch getThreadResult {
            case .failure(let error):
                completionHandler(.failure(error))
                
            case .success(let maybeThread):
                guard let thread = maybeThread else {
                    completionHandler(.failure(SHInteractionsError.noSuchThread))
                    return
                }
                
                if userIdentifier == self.user.identifier {
                    guard thread.creatorPublicIdentifier != self.user.identifier
                    else {
                        completionHandler(.failure(SHInteractionsError.leavingCreatedThreadNotAllowed))
                        return
                    }
                } else {
                    guard thread.creatorPublicIdentifier == userIdentifier
                    else {
                        completionHandler(.failure(SHInteractionsError.noPrivileges))
                        return
                    }
                }
                
                guard thread.membersPublicIdentifier.contains(userIdentifier)
                else {
                    completionHandler(.failure(SHInteractionsError.userNotInThread))
                    return
                }
                
                let update = ConversationThreadMembersUpdateDTO(
                    recipientsToAdd: [],
                    membersPublicIdentifierToRemove: [userIdentifier],
                    phoneNumbersToAdd: [],
                    phoneNumbersToRemove: []
                )
                
                self.serverProxy.updateThreadMembers(
                    for: threadId,
                    update
                ) { result in
                    switch result {
                        
                    case .success:
                        SHUserInteractionController.encryptionDetailsCache.evict(anchor: .thread, anchorId: threadId)
                        
                        self.serverProxy.deleteLocalThread(withId: threadId) { _ in
                            completionHandler(.success(()))
                        }
                    
                    case .failure(let failure):
                        completionHandler(.failure(failure))
                    }
                }
            }
        }
    }
    
    public func fetchThreadsInteractionsSummary() async throws -> [String: InteractionsThreadSummaryDTO] {
        return try await self.serverProxy.topLevelThreadsInteractionsSummary()
    }
    
    public func getAssets(
        inThread threadId: String,
        completionHandler: @escaping (Result<ConversationThreadAssetsDTO, Error>) -> ()
    ) {
        self.serverProxy.getAssets(inThread: threadId, completionHandler: completionHandler)
    }
    
    public func retrieveCachedInteractions(
        inThread threadId: String,
        ofType type: InteractionType? = nil,
        limit: Int,
        completionHandler: @escaping (Result<any SHInteractionsCollectionProtocol, Error>) -> Void
    ) {
        self.serverProxy.retrieveLocalInteractions(
            inThread: threadId,
            ofType: type,
            underMessage: nil,
            before: nil,
            limit: limit
        ) { result in
            switch result {
            case .success(let interactionsGroup):
                self.decryptMessages(
                    in: interactionsGroup,
                    for: .thread,
                    anchorId: threadId
                ) {
                    processResult in
                    switch processResult {
                    case .success(let processedResult):
                        let threadInteractions = SHConversationThreadInteractions(
                            threadId: threadId,
                            messages: processedResult.messages,
                            reactions: processedResult.reactions
                        )
                        completionHandler(.success(threadInteractions))
                    case .failure(let error):
                        log.error("failed to process interactions in thread \(threadId, privacy: .public)")
                        completionHandler(.failure(error))
                    }
                }
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    public func retrieveInteractions(
        inThread threadId: String,
        ofType type: InteractionType? = nil,
        underMessage messageId: String? = nil,
        before: Date? = nil,
        limit: Int,
        completionHandler: @escaping (Result<any SHInteractionsCollectionProtocol, Error>) -> ()
    ) {
        self.retrieveInteractions(
            inAnchor: .thread,
            anchorId: threadId,
            ofType: type,
            underMessage: messageId,
            before: before,
            limit: limit
        ) { result in
            switch result {
            case .success(let res):
                completionHandler(.success(res as! SHConversationThreadInteractions))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    public func retrieveLocalInteraction(
        inThread threadId: String,
        withId interactionIdentifier: String,
        completionHandler: @escaping (Result<any SHInteractionsCollectionProtocol, Error>) -> ()
    ) {
        self.serverProxy.retrieveLocalInteraction(
            inThread: threadId,
            withId: interactionIdentifier
        ) { firstResult in
            switch firstResult {
            case .success(let localInteractionsGroup):
                self.decryptMessages(
                    in: localInteractionsGroup,
                    for: .thread,
                    anchorId: threadId
                ) { secondResult in
                    switch secondResult {
                    case .success(let res):
                        completionHandler(.success(res as! SHConversationThreadInteractions))
                    case .failure(let err):
                        completionHandler(.failure(err))
                    }
                }
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    public func send(
        message: String,
        inThread threadId: String,
        inReplyToAssetGlobalIdentifier: String? = nil,
        inReplyToInteractionId: String? = nil,
        completionHandler: @escaping (Result<MessageOutputDTO, Error>) -> ()
    ) {
        self.send(
            message: message,
            inAnchor: .thread,
            anchorId: threadId,
            completionHandler: completionHandler
        )
    }
}
