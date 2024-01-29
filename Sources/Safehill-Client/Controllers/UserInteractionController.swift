import Foundation
import Safehill_Crypto
import CryptoKit

public typealias InteractionsCounts = (reactions: [ReactionType: [UserIdentifier]], messages: Int)


public struct SHUserInteractionController {
    let user: SHLocalUser
    let protocolSalt: Data
    private var _serverProxy: SHServerProxyProtocol? = nil
    
    public init(user: SHLocalUser, protocolSalt: Data, serverProxy: SHServerProxyProtocol? = nil) {
        self.user = user
        self.protocolSalt = protocolSalt
        self._serverProxy = serverProxy
    }
    
    var serverProxy: SHServerProxyProtocol {
        if let sp = self._serverProxy {
            return sp
        }
        return SHServerProxy(user: self.user)
    }
    
    func createNewSecret() -> SymmetricKey {
        SymmetricKey(size: .bits256)
    }
    
    public func listThreads(
        completionHandler: @escaping (Result<[ConversationThreadOutputDTO], Error>) -> Void
    ) {
        self.serverProxy.listThreads(completionHandler: completionHandler)
    }
    
    public func setupThread(
        with users: [SHServerUser],
        completionHandler: @escaping (Result<ConversationThreadOutputDTO, Error>) -> Void
    ) {
        self.serverProxy.getThread(withUsers: users) { result in
            switch result {
            case .failure(let error):
                completionHandler(.failure(error))
            case .success(let conversationThread):
                let symmetricKey: SymmetricKey
                
                if let conversationThread {
                    do {
                        let encryptionDetails = conversationThread.encryptionDetails
                        let shareablePayload = SHShareablePayload(
                            ephemeralPublicKeyData: Data(base64Encoded: encryptionDetails.ephemeralPublicKey)!,
                            cyphertext: Data(base64Encoded: encryptionDetails.encryptedSecret)!,
                            signature: Data(base64Encoded: encryptionDetails.secretPublicSignature)!
                        )
                        let decryptedSecret = try SHCypher.decrypt(
                            shareablePayload,
                            encryptionKeyData: user.shUser.privateKeyData,
                            protocolSalt: protocolSalt,
                            from: user.publicSignatureData
                        )
                        symmetricKey = SymmetricKey(data: decryptedSecret)
                    } catch {
                        log.critical("""
failed to initialize E2EE details for new users in thread \(conversationThread.threadId). error=\(error.localizedDescription)
""")
                        completionHandler(.failure(error))
                        return
                    }
                } else {
                    symmetricKey = createNewSecret()
                }
                
                var usersAndSelf = users
                if users.contains(where: { $0.identifier == user.identifier }) == false {
                    usersAndSelf.append(user)
                }
                
                do {
                    self.serverProxy.createOrUpdateThread(
                        name: nil,
                        recipientsEncryptionDetails: try recipientEncryptionDetails(from: symmetricKey, for: usersAndSelf),
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
        self.serverProxy.createOrUpdateThread(
            name: name,
            recipientsEncryptionDetails: nil,
            completionHandler: completionHandler
        )
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
            symmetricKey = createNewSecret()
        }
        
        var usersAndSelf = users
        if users.contains(where: { $0.identifier == user.identifier }) == false {
            usersAndSelf.append(user)
        }
        do {
            serverProxy.setupGroupEncryptionDetails(
                groupId: groupId,
                recipientsEncryptionDetails: try self.recipientEncryptionDetails(from: symmetricKey!, for: users),
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
    
    private func recipientEncryptionDetails(
        from secret: SymmetricKey, 
        for users: [any SHServerUser]
    ) throws -> [RecipientEncryptionDetailsDTO] {
        var recipientEncryptionDetails = [RecipientEncryptionDetailsDTO]()
        
        for user in users {
            let encryptedSecretForOther = try SHUserContext(user: self.user.shUser).shareable(
                data: secret.rawRepresentation,
                protocolSalt: protocolSalt,
                with: user
            )
            let recipientEncryptionForUser = RecipientEncryptionDetailsDTO(
                userIdentifier: user.identifier,
                ephemeralPublicKey: encryptedSecretForOther.ephemeralPublicKeyData.base64EncodedString(),
                encryptedSecret: encryptedSecretForOther.cyphertext.base64EncodedString(),
                secretPublicSignature: encryptedSecretForOther.signature.base64EncodedString()
            )
            recipientEncryptionDetails.append(recipientEncryptionForUser)
        }
        
        return recipientEncryptionDetails
    }
    
    public func deleteGroup(groupId: String, completionHandler: @escaping (Result<Void, Error>) -> ()) {
        serverProxy.deleteGroup(groupId: groupId, completionHandler: completionHandler)
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
    
    public func retrieveInteractions(
        inGroup groupId: String,
        underMessage messageId: String? = nil,
        per: Int,
        page: Int,
        completionHandler: @escaping (Result<SHUserGroupInteractions, Error>) -> ()
    ) {
        self.serverProxy.retrieveInteractions(
            inGroup: groupId,
            underMessage: messageId,
            per: per,
            page: page
        ) { result in
            switch result {
            case .success(let interactionsGroup):
                let shareablePayload = SHShareablePayload(
                    ephemeralPublicKeyData: Data(base64Encoded: interactionsGroup.ephemeralPublicKey)!,
                    cyphertext: Data(base64Encoded: interactionsGroup.encryptedSecret)!,
                    signature: Data(base64Encoded: interactionsGroup.secretPublicSignature)!
                )
                
                do {
                    let messages: [SHDecryptedMessage] = try self.decryptMessages(in: interactionsGroup, using: shareablePayload)
                    
                    let usersController = SHUsersController(localUser: self.user)
                    let userIds: [UserIdentifier] = interactionsGroup.reactions.map({ $0.senderUserIdentifier! })
                    let usersDict: [UserIdentifier: SHServerUser] = try usersController
                        .getUsers(withIdentifiers: userIds)
                        .reduce([:], { partialResult, user in
                            var result = partialResult
                            result[user.identifier] = user
                            return result
                        })
                    
                    let reactions: [SHReaction] = interactionsGroup.reactions.compactMap({
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
                    
                    completionHandler(.success(
                        SHUserGroupInteractions(
                            groupId: groupId,
                            messages: messages,
                            reactions: reactions
                        )
                    ))
                } catch {
                    log.error("failed to retrive messages or reactions in group \(groupId)")
                    completionHandler(.failure(error))
                }
                
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    public func retrieveInteractions(
        inThread threadId: String,
        underMessage messageId: String? = nil,
        per: Int,
        page: Int,
        completionHandler: @escaping (Result<SHConversationThreadInteractions, Error>) -> ()
    ) {
        self.serverProxy.retrieveInteractions(
            inThread: threadId,
            underMessage: messageId,
            per: per,
            page: page
        ) { result in
            switch result {
            case .success(let interactionsGroup):
                let shareablePayload = SHShareablePayload(
                    ephemeralPublicKeyData: Data(base64Encoded: interactionsGroup.ephemeralPublicKey)!,
                    cyphertext: Data(base64Encoded: interactionsGroup.encryptedSecret)!,
                    signature: Data(base64Encoded: interactionsGroup.secretPublicSignature)!
                )
                
                do {
                    let messages: [SHDecryptedMessage] = try self.decryptMessages(in: interactionsGroup, using: shareablePayload)
                    
                    let usersController = SHUsersController(localUser: self.user)
                    let userIds: [UserIdentifier] = interactionsGroup.reactions.map({ $0.senderUserIdentifier! })
                    let usersDict: [UserIdentifier: SHServerUser] = try usersController
                        .getUsers(withIdentifiers: userIds)
                        .reduce([:], { partialResult, user in
                            var result = partialResult
                            result[user.identifier] = user
                            return result
                        })
                    
                    let reactions: [SHReaction] = interactionsGroup.reactions.compactMap({
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
                    
                    completionHandler(.success(
                        SHConversationThreadInteractions(
                            threadId: threadId,
                            messages: messages,
                            reactions: reactions
                        )
                    ))
                } catch {
                    log.error("failed to retrive messages or reactions in thread \(threadId)")
                    completionHandler(.failure(error))
                }
                
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    private func send(
        message: String,
        inAnchor anchor: InteractionAnchor,
        anchorId: String,
        inReplyToAssetGlobalIdentifier: String? = nil,
        inReplyToInteractionId: String? = nil,
        completionHandler: @escaping (Result<MessageOutputDTO, Error>) -> ()
    ) {
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
                serverProxy.addMessage(messageInput, inThread: anchorId, completionHandler: completionHandler)
            case .group:
                serverProxy.addMessage(messageInput, inGroup: anchorId, completionHandler: completionHandler)
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
        serverProxy.addReactions([reactionInput], inGroup: groupId, completionHandler: completionHandler)
    }
    
    public func removeReaction(
        _ reaction: ReactionInput,
        fromGroup groupId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        serverProxy.removeReaction(
            reaction,
            inGroup: groupId,
            completionHandler: completionHandler
        )
    }
}

extension SHUserInteractionController {
    
    func fetchSelfEncryptionDetails(forAnchor anchor: InteractionAnchor, anchorId: String) throws -> RecipientEncryptionDetailsDTO? {
        let semaphore = DispatchSemaphore(value: 0)
        
        var encryptionDetails: RecipientEncryptionDetailsDTO? = nil
        var error: Error? = nil
        
        switch anchor {
        case .group:
            serverProxy.retrieveUserEncryptionDetails(forGroup: anchorId) { result in
                switch result {
                case .success(let e):
                    encryptionDetails = e
                case .failure(let err):
                    error = err
                }
                semaphore.signal()
            }
        case .thread:
            serverProxy.retrieveUserEncryptionDetails(forThread: anchorId) { result in
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
    
    func fetchSymmetricKey(forAnchor anchor: InteractionAnchor, anchorId: String) throws -> SymmetricKey? {
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
        let decryptedSecret = try SHCypher.decrypt(
            shareablePayload,
            encryptionKeyData: user.shUser.privateKeyData,
            protocolSalt: protocolSalt,
            from: user.publicSignatureData
        )
        return SymmetricKey(data: decryptedSecret)
    }
    
    func decryptMessages(in interactionsGroup: InteractionsGroupDTO,
                         using shareablePayload: SHShareablePayload) throws -> [SHDecryptedMessage] {
        var decryptedMessages = [SHDecryptedMessage]()
        
        let usersWithMessages = try SHUsersController(localUser: self.user).getUsers(
            withIdentifiers: interactionsGroup.messages.map({ $0.senderUserIdentifier! })
        ).reduce([UserIdentifier: SHServerUser]()) { partialResult, serverUser in
            var result = partialResult
            result[serverUser.identifier] = serverUser
            return result
        }
        
        for encryptedMessageContainer in interactionsGroup.messages {
            guard let sender = usersWithMessages[encryptedMessageContainer.senderUserIdentifier!] else {
                log.warning("couldn't find user with identifier \(encryptedMessageContainer.senderUserIdentifier!)")
                continue
            }
            guard let createdAt = encryptedMessageContainer.createdAt?.iso8601withFractionalSeconds else {
                log.warning("message doesn't have a valid timestamp \(encryptedMessageContainer.createdAt ?? "nil")")
                continue
            }
            
            let decryptedData = try SHUserContext(user: self.user.shUser).decrypt(
                Data(base64Encoded: encryptedMessageContainer.encryptedMessage)!,
                usingEncryptedSecret: shareablePayload,
                protocolSalt: protocolSalt,
                receivedFrom: sender
            )
            guard let decryptedMessage = String(data: decryptedData, encoding: .utf8) else {
                log.warning("decoding of message with interactionId=\(encryptedMessageContainer.interactionId!) failed")
                continue
            }
            
            let decryptedMessageObject = SHDecryptedMessage(
                interactionId: encryptedMessageContainer.interactionId!,
                sender: sender,
                inReplyToAssetGlobalIdentifier: encryptedMessageContainer.inReplyToAssetGlobalIdentifier,
                inReplyToInteractionId: encryptedMessageContainer.inReplyToInteractionId,
                message: decryptedMessage,
                createdAt: createdAt
            )
            decryptedMessages.append(decryptedMessageObject)
        }
        
        return decryptedMessages
    }
}
