import Foundation
import Safehill_Crypto
import CryptoKit

public typealias InteractionsCounts = (reactions: [ReactionType: [UserIdentifier]], messages: Int)


public struct SHUserInteractionController {
    
    let user: SHLocalUserProtocol
    private var serverProxy: SHServerProxyProtocol
    
    private static let encryptionDetailsCache = RecipientEncryptionDetailsCache()
    
    public init(user: SHLocalUserProtocol,
                serverProxy: SHServerProxyProtocol? = nil) {
        self.user = user
        self.serverProxy = serverProxy ?? user.serverProxy
    }
    
    func createNewSecret() -> SymmetricKey {
        SymmetricKey(size: .bits256)
    }
    
    public func listThreads(
        completionHandler: @escaping (Result<[ConversationThreadOutputDTO], Error>) -> Void
    ) {
        self.serverProxy.listThreads(completionHandler: completionHandler)
    }
    
    public func listLocalThreads(
        completionHandler: @escaping (Result<[ConversationThreadOutputDTO], Error>) -> Void
    ) {
        self.serverProxy.listLocalThreads(completionHandler: completionHandler)
    }
    
    public func setupThread(
        with users: [SHServerUser],
        completionHandler: @escaping (Result<ConversationThreadOutputDTO, Error>) -> Void
    ) {
        guard let authedUser = self.user as? SHAuthenticatedLocalUser else {
            completionHandler(.failure(SHLocalUserError.notAuthenticated))
            return
        }
        
        guard users.contains(where: { $0.identifier == self.user.identifier }) else {
            completionHandler(.failure(SHBackgroundOperationError.fatalError("users can only create groups they are part of")))
            return
        }
        
        self.serverProxy.getThread(withUsers: users) { result in
            switch result {
            case .failure(let error):
                log.error("failed to fetch thread with users \(users.map({ $0.identifier })) from remote server")
                completionHandler(.failure(error))
            case .success(let conversationThread):
                let symmetricKey: SymmetricKey
                
                if let conversationThread {
                    log.info("found thread with users \(users.map({ $0.identifier })) from remote")
                    do {
                        let encryptionDetails = conversationThread.encryptionDetails
                        let shareablePayload = SHShareablePayload(
                            ephemeralPublicKeyData: Data(base64Encoded: encryptionDetails.ephemeralPublicKey)!,
                            cyphertext: Data(base64Encoded: encryptionDetails.encryptedSecret)!,
                            signature: Data(base64Encoded: encryptionDetails.secretPublicSignature)!
                        )
                        let decryptedSecret: Data
                        
                        do {
                            decryptedSecret = try SHUserContext(user: authedUser.shUser).decryptSecret(
                                usingEncryptedSecret: shareablePayload,
                                protocolSalt: authedUser.encryptionProtocolSalt,
                                signedWith: Data(base64Encoded: encryptionDetails.senderPublicSignature)!
                            )
                        } catch SHCypher.DecryptionError.authenticationError {
                            log.warning("group details were force-updated on the server. Attempting to decrypt the secret using this user's public signature instead of the one recorded by the server")
                            decryptedSecret = try SHUserContext(user: authedUser.shUser).decryptSecret(
                                usingEncryptedSecret: shareablePayload,
                                protocolSalt: authedUser.encryptionProtocolSalt,
                                signedWith: authedUser.publicSignatureData
                            )
                        }
                        
                        symmetricKey = SymmetricKey(data: decryptedSecret)
                    } catch {
                        log.critical("""
failed to initialize E2EE details for new users in thread \(conversationThread.threadId). error=\(error.localizedDescription)
""")
                        completionHandler(.failure(error))
                        return
                    }
                } else {
                    log.info("creating new thread, because one could not be found on remote with users \(users.map({ $0.identifier }))")
                    symmetricKey = createNewSecret()
                }
                
                var usersAndSelf = users
                if users.contains(where: { $0.identifier == authedUser.identifier }) == false {
                    usersAndSelf.append(authedUser)
                }
                
                do {
                    let recipientsEncryptionDetails = try newRecipientEncryptionDetails(
                        from: symmetricKey,
                        for: usersAndSelf,
                        anchor: .thread,
                        anchorId: conversationThread?.threadId
                    )
                    log.debug("generated recipients encryptionDetails \(recipientsEncryptionDetails.map({ "R=\($0.recipientUserIdentifier) ES=\($0.encryptedSecret), EPK=\($0.ephemeralPublicKey) SSig=\($0.secretPublicSignature) USig=\($0.senderPublicSignature)" }))")
                    log.info("creating or updating threads on server with recipient encryption details for users \(recipientsEncryptionDetails.map({ $0.recipientUserIdentifier }))")
                    self.serverProxy.createOrUpdateThread(
                        name: nil,
                        recipientsEncryptionDetails: recipientsEncryptionDetails,
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
    
    public func updateThreadName(_ name: String, completionHandler: @escaping (Result<ConversationThreadOutputDTO, Error>) -> ()
    ) {
        guard self.user as? SHAuthenticatedLocalUser != nil else {
            completionHandler(.failure(SHLocalUserError.notAuthenticated))
            return
        }
        
        self.serverProxy.createOrUpdateThread(
            name: name,
            recipientsEncryptionDetails: nil,
            completionHandler: completionHandler
        )
    }
    
    public func deleteThread(threadId: String, completionHandler: @escaping (Result<Void, Error>) -> ()) {
        guard self.user as? SHAuthenticatedLocalUser != nil else {
            completionHandler(.failure(SHLocalUserError.notAuthenticated))
            return
        }
        
        SHUserInteractionController.encryptionDetailsCache.evict(anchor: .thread, anchorId: threadId)
        self.serverProxy.deleteThread(withId: threadId, completionHandler: completionHandler)
    }
    
    /// Creates the E2EE details for the group for all users involved, or updates such details if they already exist with the information for the missing users.
    /// - Parameters:
    ///   - groupId: the share group identifier
    ///   - users: the users in the share
    ///   - completionHandler: the callback method
    public func setupGroupEncryptionDetails(
        groupId: String,
        with users: [SHServerUser],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        guard self.user as? SHAuthenticatedLocalUser != nil else {
            completionHandler(.failure(SHLocalUserError.notAuthenticated))
            return
        }
        
        guard users.contains(where: { $0.identifier == self.user.identifier }) else {
            completionHandler(.failure(SHBackgroundOperationError.fatalError("users can only create groups they are part of")))
            return
        }
        
        var symmetricKey: SymmetricKey?
        
        do {
            symmetricKey = try self.fetchSymmetricKey(forAnchor: .group, anchorId: groupId)
        } catch {
            log.critical("""
failed to fetch symmetric key for self user for existing group \(groupId): \(error.localizedDescription)
""")
            completionHandler(.failure(error))
            return
        }
        
        if symmetricKey == nil {
            log.debug("generating a new secret for group with id \(groupId)")
            symmetricKey = createNewSecret()
        }
        
        do {
            self.serverProxy.setupGroupEncryptionDetails(
                groupId: groupId,
                recipientsEncryptionDetails: try self.newRecipientEncryptionDetails(
                    from: symmetricKey!,
                    for: users,
                    anchor: .group,
                    anchorId: groupId
                ),
                completionHandler: completionHandler
            )
        } catch {
            log.critical("""
failed to add E2EE details to group \(groupId) for users \(users.map({ $0.identifier })). error=\(error.localizedDescription)
""")
            completionHandler(.failure(error))
            return
        }
    }
    
    private func newRecipientEncryptionDetails(
        from secret: SymmetricKey,
        for users: [any SHServerUser],
        anchor: SHInteractionAnchor,
        anchorId: String?
    ) throws -> [RecipientEncryptionDetailsDTO] {
        var recipientEncryptionDetails = [RecipientEncryptionDetailsDTO]()
        
        for user in users {
            if let anchorId,
               let cached = SHUserInteractionController.encryptionDetailsCache.details(for: anchor, anchorId: anchorId, userIdentifier: user.identifier) {
                recipientEncryptionDetails.append(cached)
            } else {
                let encryptedSecretForOther = try self.user.createShareablePayload(
                    from: secret.rawRepresentation,
                    toShareWith: user
                )
                let recipientEncryptionForUser = RecipientEncryptionDetailsDTO(
                    recipientUserIdentifier: user.identifier,
                    ephemeralPublicKey: encryptedSecretForOther.ephemeralPublicKeyData.base64EncodedString(),
                    encryptedSecret: encryptedSecretForOther.cyphertext.base64EncodedString(),
                    secretPublicSignature: encryptedSecretForOther.signature.base64EncodedString(),
                    senderPublicSignature: self.user.publicSignatureData.base64EncodedString()
                )
                recipientEncryptionDetails.append(recipientEncryptionForUser)
                
                if let anchorId {
                    SHUserInteractionController.encryptionDetailsCache.cacheDetails(
                        recipientEncryptionForUser,
                        for: user.identifier,
                        in: anchor,
                        anchorId: anchorId
                    )
                }
            }
        }
        
        return recipientEncryptionDetails
    }
    
    public func deleteGroup(groupId: String, completionHandler: @escaping (Result<Void, Error>) -> ()) {
        guard self.user as? SHAuthenticatedLocalUser != nil else {
            completionHandler(.failure(SHLocalUserError.notAuthenticated))
            return
        }
        
        SHUserInteractionController.encryptionDetailsCache.evict(anchor: .group, anchorId: groupId)
        self.serverProxy.deleteGroup(groupId: groupId, completionHandler: completionHandler)
    }
    
    public func countInteractions(
        inGroup groupId: String,
        completionHandler: @escaping (Result<InteractionsCounts, Error>) -> ()
    ) {
        self.serverProxy.countLocalInteractions(
            inGroup: groupId,
            completionHandler: completionHandler
        )
    }
    
    public func retrieveLastMessage(
        inThread threadId: String,
        completionHandler: @escaping (Result<SHDecryptedMessage?, Error>) -> Void
    ) {
        self.serverProxy.retrieveLastMessage(inThread: threadId) { result in
            switch result {
            case .success(let interactionsGroup):
                let encryptionDetails = EncryptionDetailsClass(
                    ephemeralPublicKey: interactionsGroup.ephemeralPublicKey,
                    encryptedSecret: interactionsGroup.encryptedSecret,
                    secretPublicSignature: interactionsGroup.secretPublicSignature,
                    senderPublicSignature: interactionsGroup.senderPublicSignature
                )
                
                let messages: [SHDecryptedMessage]
                do {
                    messages = try self.decryptMessages(
                        interactionsGroup.messages,
                        usingEncryptionDetails: encryptionDetails
                    )
                } catch {
                    log.error("failed to decrypt messages in thread \(threadId, privacy: .public)")
                    completionHandler(.failure(error))
                    return
                }
                
                completionHandler(.success(messages.first))
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    public func retrieveInteractions(
        inGroup groupId: String,
        underMessage messageId: String? = nil,
        before: Date? = nil,
        limit: Int,
        completionHandler: @escaping (Result<SHAssetsGroupInteractions, Error>) -> ()
    ) {
        self.retrieveInteractions(
            inAnchor: .group,
            anchorId: groupId,
            underMessage: messageId,
            before: before,
            limit: limit
        ) { result in
            switch result {
            case .success(let res):
                completionHandler(.success(res as! SHAssetsGroupInteractions))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    public func retrieveInteractions(
        inThread threadId: String,
        underMessage messageId: String? = nil,
        before: Date? = nil,
        limit: Int,
        completionHandler: @escaping (Result<SHConversationThreadInteractions, Error>) -> ()
    ) {
        self.retrieveInteractions(
            inAnchor: .thread,
            anchorId: threadId,
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
    
    func retrieveInteractions(
        inAnchor anchor: SHInteractionAnchor,
        anchorId: String,
        underMessage messageId: String? = nil,
        before: Date?,
        limit: Int,
        completionHandler: @escaping (Result<SHInteractionsCollectionProtocol, Error>) -> ()
    ) {
        log.debug("""
[SHUserInteractionController] retriving interactions for \(anchor.rawValue) \(anchorId) before=\(before?.iso8601withFractionalSeconds ?? "nil") underMessage=\(messageId ?? "nil") (limit=\(limit))
""")
        
        let process = { (interactionsGroup: InteractionsGroupDTO) in
            let encryptionDetails = EncryptionDetailsClass(
                ephemeralPublicKey: interactionsGroup.ephemeralPublicKey,
                encryptedSecret: interactionsGroup.encryptedSecret,
                secretPublicSignature: interactionsGroup.secretPublicSignature,
                senderPublicSignature: interactionsGroup.senderPublicSignature
            )
            
            let messages: [SHDecryptedMessage]
            do {
                messages = try self.decryptMessages(
                    interactionsGroup.messages,
                    usingEncryptionDetails: encryptionDetails
                )
            } catch {
                log.error("failed to retrieve user information for \(anchor.rawValue, privacy: .public) \(anchorId, privacy: .public)")
                completionHandler(.failure(error))
                return
            }
            
            let reactions: [SHReaction]
            do {
                let usersController = SHUsersController(localUser: self.user)
                let userIds: [UserIdentifier] = interactionsGroup.reactions.map({ $0.senderUserIdentifier! })
                let usersDict: [UserIdentifier: any SHServerUser] = try usersController
                    .getUsers(withIdentifiers: userIds)
                
                reactions = interactionsGroup.reactions.compactMap({
                    reaction in
                    guard let sender = usersDict[reaction.senderUserIdentifier!] else {
                        return nil
                    }
                    
                    return SHReaction(
                        interactionId: reaction.interactionId!,
                        sender: sender,
                        inReplyToAssetGlobalIdentifier: reaction.inReplyToAssetGlobalIdentifier,
                        inReplyToInteractionId: reaction.inReplyToInteractionId,
                        reactionType: reaction.reactionType,
                        addedAt: reaction.addedAt!.iso8601withFractionalSeconds!
                    )
                })
            } catch {
                log.error("failed to fetch reactions senders in \(anchor.rawValue) \(anchorId, privacy: .public)")
                completionHandler(.failure(error))
                return
            }
            
            let result: SHInteractionsCollectionProtocol
            switch anchor {
            case .thread:
                result = SHConversationThreadInteractions(
                    threadId: anchorId,
                    messages: messages,
                    reactions: reactions
                )
            case .group:
                result = SHAssetsGroupInteractions(
                    groupId: anchorId,
                    messages: messages,
                    reactions: reactions
                )
            }
            
            completionHandler(.success(result))
        }
        
        let cacheAndProcess = { (localInteractionsGroup: InteractionsGroupDTO) in
            if localInteractionsGroup.messages.count >= limit,
               localInteractionsGroup.reactions.count >= limit {
                process(localInteractionsGroup)
            }
            else {
                log.debug("""
[SHUserInteractionController] reached limit on local interactions for \(anchor.rawValue) \(anchorId) before=\(before?.iso8601withFractionalSeconds ?? "nil") underMessage=\(messageId ?? "nil") (limit=\(limit)). Fetching more from the remote server.
""")
                
                var interactionsGroup = localInteractionsGroup
                
                let lastLocalMessageAt = localInteractionsGroup
                    .messages
                    .sorted(by: { a, b in
                        (a.createdAt?.iso8601withFractionalSeconds ?? .distantPast)
                            .compare(b.createdAt?.iso8601withFractionalSeconds ?? .distantPast)
                        == .orderedDescending
                    })
                    .first?
                    .createdAt?.iso8601withFractionalSeconds
                
                ///
                /// If we couldn't get to `limit` by just pulling from local server,
                /// check there aren't more from the remote server.
                /// If so, save them locally after fetching them from the remote server
                ///
                
                let dispatchGroup = DispatchGroup()
                
                dispatchGroup.enter()
                
                self.startCachingInteractions(
                    anchor,
                    anchorId: anchorId,
                    underMessage: messageId,
                    before: lastLocalMessageAt ?? before,
                    limit: limit,
                    localInteractionsGroup: localInteractionsGroup
                ) { result in
                    switch result {
                    case .success((let missingMessages, let missingReactions)):
                        if missingMessages.count + missingReactions.count > 0 {
                            var messages = localInteractionsGroup.messages
                            if messages.count < limit {
                                for missing in missingMessages {
                                    messages.append(missing)
                                    if messages.count == limit {
                                        break
                                    }
                                }
                            }
                            var reactions = localInteractionsGroup.reactions
                            if reactions.count < limit {
                                for missing in missingReactions {
                                    reactions.append(missing)
                                    if reactions.count == limit {
                                        break
                                    }
                                }
                            }
                            
                            log.debug("""
[SHUserInteractionController] Found \(missingMessages.count) more messages and \(missingReactions.count) reactions on the remote server. Cached them.
""")
                            
                            interactionsGroup = InteractionsGroupDTO(
                                messages: messages,
                                reactions: reactions,
                                ephemeralPublicKey: localInteractionsGroup.ephemeralPublicKey,
                                encryptedSecret: localInteractionsGroup.encryptedSecret,
                                secretPublicSignature: localInteractionsGroup.secretPublicSignature,
                                senderPublicSignature: localInteractionsGroup.senderPublicSignature
                            )
                        }
                    case .failure:
                        break
                    }
                    dispatchGroup.leave()
                }
                
                dispatchGroup.notify(queue: .global()) {
                    process(interactionsGroup)
                }
            }
        }
        
        switch anchor {
        case .thread:
            self.serverProxy.retrieveInteractions(
                inThread: anchorId,
                underMessage: messageId,
                before: before,
                limit: limit
            ) { result in
                switch result {
                case .success(let localInteractionsGroup):
                    cacheAndProcess(localInteractionsGroup)
                case .failure(let error):
                    completionHandler(.failure(error))
                }
            }
        case .group:
            self.serverProxy.retrieveInteractions(
                inGroup: anchorId,
                underMessage: messageId,
                before: before,
                limit: limit
            ) { result in
                switch result {
                case .success(let localInteractionsGroup):
                    cacheAndProcess(localInteractionsGroup)
                case .failure(let error):
                    completionHandler(.failure(error))
                }
            }
        }
    }
    
    private func send(
        message: String,
        inAnchor anchor: SHInteractionAnchor,
        anchorId: String,
        inReplyToAssetGlobalIdentifier: String? = nil,
        inReplyToInteractionId: String? = nil,
        completionHandler: @escaping (Result<MessageOutputDTO, Error>) -> ()
    ) {
        guard self.user as? SHAuthenticatedLocalUser != nil else {
            completionHandler(.failure(SHLocalUserError.notAuthenticated))
            return
        }
        
        guard let messageData = message.data(using: .utf8) else {
            completionHandler(.failure(SHBackgroundOperationError.unexpectedData(message)))
            return
        }
        
        do {
            guard let symmetricKey = try self.fetchSymmetricKey(forAnchor: anchor, anchorId: anchorId)
            else {
                switch anchor {
                case .thread:
                    completionHandler(.failure(SHBackgroundOperationError.missingE2EEDetailsForThread(anchorId)))
                case .group:
                    completionHandler(.failure(SHBackgroundOperationError.missingE2EEDetailsForGroup(anchorId)))
                }
                return
            }
            
            let encryptedData = try SHEncryptedData(privateSecret: symmetricKey, clearData: messageData)
            let messageInput = MessageInputDTO(
                inReplyToAssetGlobalIdentifier: inReplyToAssetGlobalIdentifier,
                inReplyToInteractionId: inReplyToInteractionId,
                encryptedMessage: encryptedData.encryptedData.base64EncodedString(),
                senderPublicSignature: self.user.publicSignatureData.base64EncodedString()
            )
            
            switch anchor {
            case .thread:
                self.serverProxy.addMessage(messageInput, inThread: anchorId, completionHandler: completionHandler)
            case .group:
                self.serverProxy.addMessage(messageInput, inGroup: anchorId, completionHandler: completionHandler)
            }
        } catch {
            completionHandler(.failure(error))
        }
    }
    
    public func send(
        message: String,
        inGroup groupId: String,
        inReplyToAssetGlobalIdentifier: String? = nil,
        inReplyToInteractionId: String? = nil,
        completionHandler: @escaping (Result<MessageOutputDTO, Error>) -> ()
    ) {
        self.send(message: message, inAnchor: .group, anchorId: groupId, completionHandler: completionHandler)
    }
    
    public func send(
        message: String,
        inThread threadId: String,
        inReplyToAssetGlobalIdentifier: String? = nil,
        inReplyToInteractionId: String? = nil,
        completionHandler: @escaping (Result<MessageOutputDTO, Error>) -> ()
    ) {
        self.send(message: message, inAnchor: .thread, anchorId: threadId, completionHandler: completionHandler)
    }
    
    public func addReaction(
        _ reactionType: ReactionType,
        inGroup groupId: String,
        inReplyToAssetGlobalIdentifier: String? = nil,
        inReplyToInteractionId: String? = nil,
        completionHandler: @escaping (Result<[ReactionOutputDTO], Error>) -> ()
    ) {
        let reactionInput = ReactionInputDTO(
            inReplyToAssetGlobalIdentifier: inReplyToAssetGlobalIdentifier,
            inReplyToInteractionId: inReplyToInteractionId,
            reactionType: reactionType
        )
        self.serverProxy.addReactions([reactionInput], inGroup: groupId, completionHandler: completionHandler)
    }
    
    public func removeReaction(
        _ reaction: ReactionInput,
        fromGroup groupId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        self.serverProxy.removeReaction(
            reaction,
            inGroup: groupId,
            completionHandler: completionHandler
        )
    }
}

extension SHUserInteractionController {
    
    func fetchSelfEncryptionDetails(forAnchor anchor: SHInteractionAnchor, anchorId: String) throws -> RecipientEncryptionDetailsDTO? {
        let semaphore = DispatchSemaphore(value: 0)
        
        var encryptionDetails: RecipientEncryptionDetailsDTO? = nil
        var error: Error? = nil
        
        switch anchor {
        case .group:
            self.serverProxy.retrieveUserEncryptionDetails(forGroup: anchorId) { result in
                switch result {
                case .success(let e):
                    encryptionDetails = e
                case .failure(let err):
                    error = err
                }
                semaphore.signal()
            }
        case .thread:
            self.serverProxy.retrieveUserEncryptionDetails(forThread: anchorId) { result in
                switch result {
                case .success(let e):
                    encryptionDetails = e
                case .failure(let err):
                    error = err
                }
                semaphore.signal()
            }
        }
        
        let dispatchResult = semaphore.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds))
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
        guard error == nil else {
            throw error!
        }
        
        return encryptionDetails
    }
    
    func fetchSymmetricKey(forAnchor anchor: SHInteractionAnchor, anchorId: String) throws -> SymmetricKey? {
        guard let salt = self.user.maybeEncryptionProtocolSalt else {
            throw SHLocalUserError.missingProtocolSalt
        }
        
        let encryptionDetails: RecipientEncryptionDetailsDTO?
        
        do {
            encryptionDetails = try self.fetchSelfEncryptionDetails(forAnchor: anchor, anchorId: anchorId)
        } catch {
            throw SHBackgroundOperationError.fatalError("trying to decrypt a symmetric key for non-existent E2EE details for \(anchor.rawValue) \(anchorId)")
        }
        
        guard let encryptionDetails else {
            return nil
        }
        
        let shareablePayload = SHShareablePayload(
            ephemeralPublicKeyData: Data(base64Encoded: encryptionDetails.ephemeralPublicKey)!,
            cyphertext: Data(base64Encoded: encryptionDetails.encryptedSecret)!,
            signature: Data(base64Encoded: encryptionDetails.secretPublicSignature)!
        )
        let decryptedSecret = try SHUserContext(user: self.user.shUser).decryptSecret(
            usingEncryptedSecret: shareablePayload,
            protocolSalt: salt,
            signedWith: Data(base64Encoded: encryptionDetails.senderPublicSignature)!
        )
        return SymmetricKey(data: decryptedSecret)
    }
    
    public func decryptMessages(
        _ encryptedMessages: [MessageOutputDTO],
        usingEncryptionDetails encryptionDetails: EncryptionDetailsClass
    ) throws -> [SHDecryptedMessage] {
        guard let salt = self.user.maybeEncryptionProtocolSalt else {
            throw SHLocalUserError.missingProtocolSalt
        }
        
        var decryptedMessages = [SHDecryptedMessage]()
        
        let shareablePayload = SHShareablePayload(
            ephemeralPublicKeyData: Data(base64Encoded: encryptionDetails.ephemeralPublicKey)!,
            cyphertext: Data(base64Encoded: encryptionDetails.encryptedSecret)!,
            signature: Data(base64Encoded: encryptionDetails.secretPublicSignature)!
        )
        
        let usersWithMessagesKeyedById = try SHUsersController(localUser: self.user).getUsers(
            withIdentifiers: encryptedMessages.map({ $0.senderUserIdentifier! })
        )
        
        for encryptedMessage in encryptedMessages {
            guard let sender = usersWithMessagesKeyedById[encryptedMessage.senderUserIdentifier!] else {
                log.warning("couldn't find user with identifier \(encryptedMessage.senderUserIdentifier!)")
                continue
            }
            guard let createdAt = encryptedMessage.createdAt?.iso8601withFractionalSeconds else {
                log.warning("message doesn't have a valid timestamp \(encryptedMessage.createdAt ?? "nil")")
                continue
            }
            
            let decryptedData: Data
            
            do {
                decryptedData = try SHUserContext(user: self.user.shUser).decrypt(
                    Data(base64Encoded: encryptedMessage.encryptedMessage)!,
                    usingEncryptedSecret: shareablePayload,
                    protocolSalt: salt,
                    signedWith: Data(base64Encoded: encryptionDetails.senderPublicSignature)!
                )
            } catch {
                log.critical("failed to decrypt message \(encryptedMessage.interactionId!) from \(encryptedMessage.senderUserIdentifier!). error=\(error.localizedDescription)")
                continue
            }
            guard let decryptedMessage = String(data: decryptedData, encoding: .utf8) else {
                log.warning("decoding of message with interactionId=\(encryptedMessage.interactionId!) failed")
                continue
            }
            
            let decryptedMessageObject = SHDecryptedMessage(
                interactionId: encryptedMessage.interactionId!,
                sender: sender,
                inReplyToAssetGlobalIdentifier: encryptedMessage.inReplyToAssetGlobalIdentifier,
                inReplyToInteractionId: encryptedMessage.inReplyToInteractionId,
                message: decryptedMessage,
                createdAt: createdAt
            )
            decryptedMessages.append(decryptedMessageObject)
        }
        
        return decryptedMessages
    }
}


extension SHUserInteractionController {
    
    func startCachingInteractions(
        _ anchor: SHInteractionAnchor,
        anchorId: String,
        underMessage messageId: String?,
        before: Date?,
        limit: Int,
        localInteractionsGroup: InteractionsGroupDTO,
        completionHandler: @escaping (Result<([MessageOutputDTO], [ReactionOutputDTO]), Error>) -> Void
    ) {
        let process = { (remoteInteractionsGroup: InteractionsGroupDTO) in
            
            let localReactionIds = localInteractionsGroup.reactions.map({ $0.interactionId })
            let localMessageIds = localInteractionsGroup.messages.map({ $0.interactionId })
            let missingReactions = remoteInteractionsGroup.reactions.filter({
                localReactionIds.contains($0.interactionId) == false
            })
            let missingMessages = remoteInteractionsGroup.messages.filter({
                localMessageIds.contains($0.interactionId) == false
            })
            
            switch anchor {
            case .thread:
                self.serverProxy.addLocalReactions(
                    missingReactions,
                    inThread: anchorId
                ) { res in
                    if case .failure(let failure) = res {
                        log.critical("failed to add remote reactions to local \(anchor.rawValue) \(anchorId): \(failure.localizedDescription)")
                    }
                }
                
                self.serverProxy.addLocalMessages(
                    missingMessages,
                    inThread: anchorId
                ) { res in
                    if case .failure(let failure) = res {
                        log.critical("failed to add remote messages to local \(anchor.rawValue) \(anchorId): \(failure.localizedDescription)")
                    }
                }
            case .group:
                self.serverProxy.addLocalReactions(
                    missingReactions,
                    inGroup: anchorId
                ) { res in
                    if case .failure(let failure) = res {
                        log.critical("failed to add remote reactions to local \(anchor.rawValue) \(anchorId): \(failure.localizedDescription)")
                    }
                }
                
                self.serverProxy.addLocalMessages(
                    missingMessages,
                    inGroup: anchorId
                ) { res in
                    if case .failure(let failure) = res {
                        log.critical("failed to add remote messages to local \(anchor.rawValue) \(anchorId): \(failure.localizedDescription)")
                    }
                }
            }
            
            completionHandler(.success((missingMessages, missingReactions)))
        }
        
        switch anchor {
        case .group:
            self.serverProxy.retrieveRemoteInteractions(
                inGroup: anchorId,
                underMessage: messageId,
                before: before,
                limit: limit
            ) { result in
                
                switch result {
                case .success(let remoteInteractionsGroup):
                    process(remoteInteractionsGroup)
                case .failure(let err):
                    completionHandler(.failure(err))
                }
            }
        case .thread:
            self.serverProxy.retrieveRemoteInteractions(
                inThread: anchorId,
                underMessage: messageId,
                before: before,
                limit: limit
            ) { result in
                
                switch result {
                case .success(let remoteInteractionsGroup):
                    process(remoteInteractionsGroup)
                case .failure(let err):
                    completionHandler(.failure(err))
                }
            }
        }
    }
}
