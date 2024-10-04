import Foundation
import Safehill_Crypto
import CryptoKit

public enum InteractionType: String {
    case message = "message", reaction = "reaction"
}

let E2eCreationSerialQueue = DispatchQueue(
    label: "com.safehill.encryptAndShare.e2eCreation",
    qos: .userInteractive
)

public enum SHInteractionsError: Error, LocalizedError {
    case noSafehillUsersInThread
    case noSuchThread
    case failedToFetchUsers
    case leavingCreatedThreadNotAllowed
    case userNotInThread
    case noPrivileges
    case threadConflict(ConversationThreadOutputDTO)
    
    public var errorDescription: String? {
        switch self {
        case .noSafehillUsersInThread:
            return "A thread must contain at least one Safehill user"
        case .noSuchThread:
            return "A thread with the specified identifier does not exist"
        case .failedToFetchUsers:
            return "Some of the users don't exist or can not be fetched right now"
        case .leavingCreatedThreadNotAllowed:
            return "The one and only admin can't leave the Thread"
        case .userNotInThread:
            return "The user is not currently in this Thread"
        case .noPrivileges:
            return "Only an administrator of this Thread can perform this operation"
        case .threadConflict:
            return "You created Thread with these users already"
        }
    }
}

public struct SHUserInteractionController {
    
    let user: SHLocalUserProtocol
    internal var serverProxy: SHServerProxyProtocol
    
    internal static let encryptionDetailsCache = RecipientEncryptionDetailsCache()
    
    public init(user: SHLocalUserProtocol) {
        self.user = user
        self.serverProxy = user.serverProxy
    }
    
    internal init(user: SHLocalUserProtocol,
                serverProxy: SHServerProxyProtocol? = nil) {
        self.user = user
        self.serverProxy = serverProxy ?? user.serverProxy
    }
    
    func createNewSecret() -> SymmetricKey {
        SymmetricKey(size: .bits256)
    }
    
    internal func newRecipientEncryptionDetails(
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
    
    internal func decryptMessages(
        in interactionsGroup: InteractionsGroupDTO,
        for anchor: SHInteractionAnchor,
        anchorId: String,
        completionHandler: @escaping (Result<any SHInteractionsCollectionProtocol, Error>) -> ()
    ) {
        let encryptionDetails = EncryptionDetailsClass(
            ephemeralPublicKey: interactionsGroup.ephemeralPublicKey,
            encryptedSecret: interactionsGroup.encryptedSecret,
            secretPublicSignature: interactionsGroup.secretPublicSignature,
            senderPublicSignature: interactionsGroup.senderPublicSignature
        )
        
        var messages = [SHDecryptedMessage]()
        var reactions = [SHReaction]()
        var error: Error? = nil
        
        let dispatchGroup = DispatchGroup()
        
        dispatchGroup.enter()
        self.decryptMessages(
            interactionsGroup.messages,
            usingEncryptionDetails: encryptionDetails
        ) { result in
            switch result {
            case .failure(let err):
                log.error("failed to retrieve user information for \(anchor.rawValue, privacy: .public) \(anchorId, privacy: .public): \(err.localizedDescription)")
                error = err
            case .success(let msgs):
                messages = msgs
            }
            dispatchGroup.leave()
        }
        
        dispatchGroup.enter()
        let usersController = SHUsersController(localUser: self.user)
        let userIds = Set<UserIdentifier>(
            interactionsGroup.reactions.map({ $0.senderPublicIdentifier! })
            + interactionsGroup.messages.map({ $0.senderPublicIdentifier! })
        )
        
        usersController.getUsers(withIdentifiers: Array(userIds)) {
            result in
            switch result {
            case .failure(let err):
                log.error("failed to fetch reactions senders in \(anchor.rawValue) \(anchorId, privacy: .public): \(err.localizedDescription)")
                error = err
            case .success(let usersDict):
                reactions = interactionsGroup.reactions.compactMap({
                    reaction in
                    guard let sender = usersDict[reaction.senderPublicIdentifier!] else {
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
            }
            dispatchGroup.leave()
        }
        
        dispatchGroup.notify(queue: .global()) {
            if let error {
                completionHandler(.failure(error))
            } else {
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
        }
    }
    
    func retrieveInteractions(
        inAnchor anchor: SHInteractionAnchor,
        anchorId: String,
        ofType type: InteractionType?,
        underMessage messageId: String? = nil,
        before: Date?,
        limit: Int,
        completionHandler: @escaping (Result<any SHInteractionsCollectionProtocol, Error>) -> ()
    ) {
        log.debug("""
[SHUserInteractionController] retrieving interactions (\(type?.rawValue ?? "messages+reactions")) for \(anchor.rawValue) \(anchorId) before=\(before?.iso8601withFractionalSeconds ?? "nil") underMessage=\(messageId ?? "nil") (limit=\(limit))
""")
        
        switch anchor {
        case .thread:
            self.serverProxy.retrieveInteractions(
                inThread: anchorId,
                ofType: type,
                underMessage: messageId,
                before: before,
                limit: limit
            ) { result in
                switch result {
                case .success(let localInteractionsGroup):
                    self.decryptMessages(
                        in: localInteractionsGroup,
                        for: anchor,
                        anchorId: anchorId,
                        completionHandler: completionHandler
                    )
                case .failure(let error):
                    completionHandler(.failure(error))
                }
            }
        case .group:
            self.serverProxy.retrieveInteractions(
                inGroup: anchorId,
                ofType: type,
                underMessage: messageId,
                before: before,
                limit: limit
            ) { result in
                switch result {
                case .success(let localInteractionsGroup):
                    self.decryptMessages(
                        in: localInteractionsGroup,
                        for: anchor,
                        anchorId: anchorId,
                        completionHandler: completionHandler
                    )
                case .failure(let error):
                    completionHandler(.failure(error))
                }
            }
        }
    }
    
    internal func send(
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
                self.serverProxy.addMessage(
                    messageInput,
                    toThread: anchorId,
                    completionHandler: completionHandler
                )
            case .group:
                self.serverProxy.addMessage(
                    messageInput,
                    toGroup: anchorId,
                    completionHandler: completionHandler
                )
            }
        } catch {
            completionHandler(.failure(error))
        }
    }
    
    internal func fetchSelfEncryptionDetails(forAnchor anchor: SHInteractionAnchor, anchorId: String) throws -> RecipientEncryptionDetailsDTO? {
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
    
    internal func fetchSymmetricKey(forAnchor anchor: SHInteractionAnchor, anchorId: String) throws -> SymmetricKey? {
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
        in anchor: SHInteractionAnchor,
        anchorId: String
    ) async throws -> [SHDecryptedMessage] {
        try await withUnsafeThrowingContinuation { continuation in
            self.decryptMessages(encryptedMessages, in: anchor, anchorId: anchorId) {
                result in
                continuation.resume(with: result)
            }
        }
    }
    
    public func decryptMessages(
        _ encryptedMessages: [MessageOutputDTO],
        in anchor: SHInteractionAnchor,
        anchorId: String,
        completionHandler: @escaping (Result<[SHDecryptedMessage], Error>) -> Void
    ) {
        let encryptionDetails: RecipientEncryptionDetailsDTO
        do {
            let maybeEncryptionDetails = try self.fetchSelfEncryptionDetails(forAnchor: anchor, anchorId: anchorId)
            guard let maybeEncryptionDetails else {
                switch anchor {
                case .thread:
                    completionHandler(.failure(SHBackgroundOperationError.missingE2EEDetailsForThread(anchorId)))
                case .group:
                    completionHandler(.failure(SHBackgroundOperationError.missingE2EEDetailsForGroup(anchorId)))
                }
                return
            }
            encryptionDetails = maybeEncryptionDetails
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        let encryptionDetailsClass = EncryptionDetailsClass(
            ephemeralPublicKey: encryptionDetails.ephemeralPublicKey,
            encryptedSecret: encryptionDetails.encryptedSecret,
            secretPublicSignature: encryptionDetails.secretPublicSignature,
            senderPublicSignature: encryptionDetails.senderPublicSignature
        )
        
        decryptMessages(
            encryptedMessages,
            usingEncryptionDetails: encryptionDetailsClass,
            completionHandler: completionHandler
        )
    }
    
    private func decryptMessages(
        _ encryptedMessages: [MessageOutputDTO],
        usingEncryptionDetails encryptionDetails: EncryptionDetailsClass,
        completionHandler: @escaping (Result<[SHDecryptedMessage], Error>) -> Void
    ) {
        guard let salt = self.user.maybeEncryptionProtocolSalt else {
            completionHandler(.failure(SHLocalUserError.missingProtocolSalt))
            return
        }
        
        let shareablePayload = SHShareablePayload(
            ephemeralPublicKeyData: Data(base64Encoded: encryptionDetails.ephemeralPublicKey)!,
            cyphertext: Data(base64Encoded: encryptionDetails.encryptedSecret)!,
            signature: Data(base64Encoded: encryptionDetails.secretPublicSignature)!
        )
        
         SHUsersController(localUser: self.user).getUsers(
            withIdentifiers: encryptedMessages.map({ $0.senderPublicIdentifier! })
         ) { result in
             switch result {
             case .failure(let error):
                 completionHandler(.failure(error))
             case .success(let usersWithMessagesKeyedById):
                 var decryptedMessages = [SHDecryptedMessage]()
                 
                 for encryptedMessage in encryptedMessages {
                     guard let sender = usersWithMessagesKeyedById[encryptedMessage.senderPublicIdentifier!] else {
                         log.warning("couldn't find user with identifier \(encryptedMessage.senderPublicIdentifier!)")
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
                         log.critical("failed to decrypt message \(encryptedMessage.interactionId!) from \(encryptedMessage.senderPublicIdentifier!). error=\(error.localizedDescription)")
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
                 
                 completionHandler(.success(decryptedMessages))
             }
         }
    }
}
