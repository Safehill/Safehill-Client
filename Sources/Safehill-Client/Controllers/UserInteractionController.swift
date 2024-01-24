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
        do {
            let symmetricKey = try self.fetchSymmetricKey(forGroup: groupId)
            
            if let symmetricKey {
                do {
                    try self.userGroupEncryptionSetup(
                        groupId: groupId,
                        secret: symmetricKey,
                        users: users,
                        completionHandler: completionHandler
                    )
                } catch {
                    log.critical("""
failed to add E2EE details to group \(groupId) for users \(users.map({ $0.identifier })). error=\(error.localizedDescription)
""")
                    completionHandler(.failure(error))
                    return
                }
            } else {
                do {
                    try self.createNewGroupEncryptionDetails(
                        groupId: groupId,
                        with: users,
                        completionHandler: completionHandler
                    )
                } catch {
                    log.critical("""
failed to initialize E2EE details for group \(groupId). error=\(error.localizedDescription)
""")
                    completionHandler(.failure(error))
                    return
                }
            }
        } catch {
            log.critical("""
failed to fetch symmetric key for self user for existing group \(groupId): \(error.localizedDescription)
""")
            completionHandler(.failure(error))
        }
    }
    
    public func createNewGroupEncryptionDetails(
        groupId: String,
        with users: [SHServerUser],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) throws {
        let secret: SymmetricKey = createNewSecret()
        var usersAndSelf = users
        usersAndSelf.append(user)
        
        try self.userGroupEncryptionSetup(
            groupId: groupId,
            secret: secret,
            users: usersAndSelf,
            completionHandler: completionHandler
        )
    }
    
    private func userGroupEncryptionSetup(
        groupId: String,
        secret: SymmetricKey,
        users: [SHServerUser],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) throws {
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
        
        serverProxy.setupGroupEncryptionDetails(
            groupId: groupId,
            recipientsEncryptionDetails: recipientEncryptionDetails,
            completionHandler: completionHandler
        )
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
    
    public func send(
        message: String,
        inGroup groupId: String,
        inReplyToAssetGlobalIdentifier: String? = nil,
        inReplyToInteractionId: String? = nil,
        completionHandler: @escaping (Result<MessageOutputDTO, Error>) -> ()
    ) {
        guard let messageData = message.data(using: .utf8) else {
            completionHandler(.failure(SHBackgroundOperationError.unexpectedData(message)))
            return
        }
        
        do {
            guard let symmetricKey = try self.fetchSymmetricKey(forGroup: groupId)
            else {
                completionHandler(.failure(SHBackgroundOperationError.missingE2EEDetailsForGroup(groupId)))
                return
            }
            
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
        serverProxy.addReactions([reactionInput], toGroupId: groupId, completionHandler: completionHandler)
    }
    
    public func removeReaction(
        _ reaction: ReactionInput,
        fromGroup groupId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        serverProxy.removeReaction(
            reaction,
            fromGroupId: groupId,
            completionHandler: completionHandler
        )
    }
}

extension SHUserInteractionController {
    
    func fetchSelfEncryptionDetails(forGroup groupId: String) throws -> RecipientEncryptionDetailsDTO? {
        let semaphore = DispatchSemaphore(value: 0)
        
        var encryptionDetails: RecipientEncryptionDetailsDTO? = nil
        var error: Error? = nil
        
        serverProxy.retrieveSelfGroupUserEncryptionDetails(forGroup: groupId) { result in
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
        
        return encryptionDetails
    }
    
    func fetchSymmetricKey(forGroup groupId: String) throws -> SymmetricKey? {
        let encryptionDetails: RecipientEncryptionDetailsDTO?
        
        do {
            encryptionDetails = try self.fetchSelfEncryptionDetails(forGroup: groupId)
        } catch {
            throw SHBackgroundOperationError.fatalError("trying to decrypt a symmetric key for non-existent E2EE details for group \(groupId)")
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
