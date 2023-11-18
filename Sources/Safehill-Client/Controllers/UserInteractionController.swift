import Foundation
import Safehill_Crypto
import CryptoKit


public struct SHUserInteractionController {
    let user: SHLocalUser
    let protocolSalt: Data
    private var _serverProxy: SHServerProxyProtocol? = nil
    
    init(user: SHLocalUser, protocolSalt: Data, serverProxy: SHServerProxyProtocol? = nil) {
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
    
    public func initializeGroup(
        groupId: String,
        with users: [SHServerUser],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        do {
            let secret = createNewSecret()
            let encryptedSecretForSelf = try SHUserContext(user: user.shUser).shareable(
                data: secret.rawRepresentation,
                protocolSalt: protocolSalt,
                with: user
            )
            
            var recipientEncryptionDetails = [
                RecipientEncryptionDetailsDTO(
                    userIdentifier: self.user.identifier,
                    ephemeralPublicKey: encryptedSecretForSelf.ephemeralPublicKeyData.base64EncodedString(),
                    encryptedSecret: encryptedSecretForSelf.cyphertext.base64EncodedString(),
                    secretPublicSignature: encryptedSecretForSelf.signature.base64EncodedString()
                )
            ]
            
            for otherUser in users {
                let encryptedSecretForOther = try SHUserContext(user: self.user.shUser).shareable(
                    data: secret.rawRepresentation,
                    protocolSalt: protocolSalt,
                    with: otherUser
                )
                let recipientEncryptionForUser = RecipientEncryptionDetailsDTO(
                    userIdentifier: otherUser.identifier,
                    ephemeralPublicKey: encryptedSecretForOther.ephemeralPublicKeyData.base64EncodedString(),
                    encryptedSecret: encryptedSecretForOther.ephemeralPublicKeyData.base64EncodedString(),
                    secretPublicSignature: encryptedSecretForOther.signature.base64EncodedString()
                )
                recipientEncryptionDetails.append(recipientEncryptionForUser)
            }
            
            serverProxy.createGroup(
                groupId: groupId,
                recipientsEncryptionDetails: recipientEncryptionDetails,
                completionHandler: completionHandler
            )
        } catch {
            log.error("failed to initialize E2EE details for group \(groupId). error=\(error.localizedDescription)")
            completionHandler(.failure(error))
            return
        }
    }
    
    func add(users: [SHServerUser],
             toGroup groupId: String,
             completionHandler: @escaping (Result<Void, Error>) -> ()) {
        
        do {
            let symmetricKey = try self.fetchSymmetricKey(forGroup: groupId)
            
            var recipientEncryptionDetails = [RecipientEncryptionDetailsDTO]()
            for otherUser in users {
                let encryptedSecretForOther = try SHUserContext(user: self.user.shUser).shareable(
                    data: symmetricKey.rawRepresentation,
                    protocolSalt: protocolSalt,
                    with: otherUser
                )
                let recipientEncryptionForUser = RecipientEncryptionDetailsDTO(
                    userIdentifier: otherUser.identifier,
                    ephemeralPublicKey: encryptedSecretForOther.ephemeralPublicKeyData.base64EncodedString(),
                    encryptedSecret: encryptedSecretForOther.ephemeralPublicKeyData.base64EncodedString(),
                    secretPublicSignature: encryptedSecretForOther.signature.base64EncodedString()
                )
                recipientEncryptionDetails.append(recipientEncryptionForUser)
            }
            
            serverProxy.addToGroup(groupId: groupId,
                                   recipientsEncryptionDetails: recipientEncryptionDetails,
                                   completionHandler: completionHandler)
        } catch {
            log.error("failed to encrypt E2EE details when adding \(users.count) users to \(groupId). error=\(error.localizedDescription)")
            completionHandler(.failure(error))
            return
        }
    }
    
    public func countInteractions(
        inGroup groupId: String,
        completionHandler: @escaping (Result<(reactions: Int, messages: Int), Error>) -> ()
    ) {
        self.serverProxy.retrieveInteractions(
            inGroup: groupId,
            per: 10000,
            page: 1
        ) { result in
            switch result {
            case .success(let interactionsGroup):
                completionHandler(.success((
                    reactions: interactionsGroup.reactions.count,
                    messages: interactionsGroup.messages.count
                )))
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    public func retrieveInteractions(
        inGroup groupId: String,
        per: Int,
        page: Int,
        completionHandler: @escaping (Result<SHUserGroupInteractions, Error>) -> ()
    ) {
        self.serverProxy.retrieveInteractions(
            inGroup: groupId,
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
    
    func send(message: String,
              inGroup groupId: String,
              inReplyToAssetGlobalIdentifier: String? = nil,
              inReplyToInteractionId: String? = nil,
              completionHandler: @escaping (Result<MessageOutputDTO, Error>) -> ()) {
        guard let messageData = message.data(using: .utf8) else {
            completionHandler(.failure(SHBackgroundOperationError.unexpectedData(message)))
            return
        }
        
        do {
            let symmetricKey = try self.fetchSymmetricKey(forGroup: groupId)
            
            let encryptedData = try SHEncryptedData(privateSecret: symmetricKey, clearData: messageData)
            let messageInput = MessageInputDTO(
                inReplyToAssetGlobalIdentifier: inReplyToAssetGlobalIdentifier,
                inReplyToInteractionId: inReplyToInteractionId,
                encryptedMessage: encryptedData.encryptedData.base64EncodedString(),
                senderPublicSignature: self.user.publicSignatureData.base64EncodedString()
            )
            serverProxy.addMessage(messageInput, toGroupId: groupId, completionHandler: completionHandler)
        } catch {
            completionHandler(.failure(error))
        }
    }
    
    func addReaction(_ reactionType: ReactionType,
                     inGroup groupId: String,
                     inReplyToAssetGlobalIdentifier: String? = nil,
                     inReplyToInteractionId: String? = nil,
                     completionHandler: @escaping (Result<[ReactionOutputDTO], Error>) -> ()) {
        let reactionInput = ReactionInputDTO(
            inReplyToAssetGlobalIdentifier: inReplyToAssetGlobalIdentifier,
            inReplyToInteractionId: inReplyToInteractionId,
            reactionType: reactionType
        )
        serverProxy.addReactions([reactionInput], toGroupId: groupId, completionHandler: completionHandler)
    }
    
    func removeReaction(with interactionIdentifier: String,
                        fromGroup groupId: String,
                        completionHandler: @escaping (Result<Void, Error>) -> ()) {
        serverProxy.removeReaction(
            withIdentifier: self.user.identifier,
            fromGroupId: groupId,
            completionHandler: completionHandler
        )
    }
}

extension SHUserInteractionController {
    
    func fetchEncryptionDetails(forGroup groupId: String) throws -> RecipientEncryptionDetailsDTO {
        let semaphore = DispatchSemaphore(value: 0)
        
        var encryptionDetails: RecipientEncryptionDetailsDTO? = nil
        var error: Error? = nil
        
        serverProxy.retrieveGroupUserEncryptionDetails(forGroup: groupId) { result in
            switch result {
            case .success(let e):
                encryptionDetails = e
            case .failure(let err):
                error = err
            }
            semaphore.signal()
        }
        
        let dispatchResult = semaphore.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds))
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
        guard error == nil else {
            throw error!
        }
        guard let encryptionDetails = encryptionDetails else {
            throw SHBackgroundOperationError.unexpectedData(nil)
        }
        
        return encryptionDetails
    }
    
    func fetchShareableSecretPayload(forGroup groupId: String) throws -> SHShareablePayload {
        let encryptionDetails = try self.fetchEncryptionDetails(forGroup: groupId)
        return SHShareablePayload(
            ephemeralPublicKeyData: Data(base64Encoded: encryptionDetails.ephemeralPublicKey)!,
            cyphertext: Data(base64Encoded: encryptionDetails.encryptedSecret)!,
            signature: Data(base64Encoded: encryptionDetails.secretPublicSignature)!
        )
    }
    
    func fetchSymmetricKey(forGroup groupId: String) throws -> SymmetricKey {
        let shareablePayload = try self.fetchShareableSecretPayload(forGroup: groupId)
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
