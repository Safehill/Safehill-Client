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
    
    public var errorDescription: String? {
        switch self {
        case .noSafehillUsersInThread:
            return "A thread must contain at least one Safehill user"
        }
    }
}

public struct SHUserInteractionController {
    
    let user: SHLocalUserProtocol
    private var serverProxy: SHServerProxyProtocol
    
    private static let encryptionDetailsCache = RecipientEncryptionDetailsCache()
    
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
        guard self.user as? SHAuthenticatedLocalUser != nil else {
            completionHandler(.failure(SHLocalUserError.notAuthenticated))
            return
        }
        
        let update = ConversationThreadMembersUpdateDTO(
            recipientsToAdd: [],
            membersPublicIdentifierToRemove: [],
            phoneNumbersToAdd: phoneNumbersToAdd,
            phoneNumbersToRemove: []
        )
        
        self.serverProxy.updateThreadMembers(
            for: threadId,
            update,
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
        E2eCreationSerialQueue.async {
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
    
    public func fetchThreadsInteractionsSummary() async throws -> [String: InteractionsThreadSummaryDTO] {
        return try await self.serverProxy.topLevelThreadsInteractionsSummary()
    }
    
    public func fetchGroupsInteractionsSummary() async throws -> [String: InteractionsGroupSummaryDTO] {
        return try await self.serverProxy.topLevelGroupsInteractionsSummary()
    }
    
    public func reloadLocalInteractionsSummary(
        for groupId: String
    ) async throws -> InteractionsGroupSummaryDTO {
        return try await self.serverProxy.topLevelLocalInteractionsSummary(for: groupId)
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
                self.decryptMessages(in: interactionsGroup, for: .thread, anchorId: threadId) {
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
        inGroup groupId: String,
        ofType type: InteractionType? = nil,
        underMessage messageId: String? = nil,
        before: Date? = nil,
        limit: Int,
        completionHandler: @escaping (Result<any SHInteractionsCollectionProtocol, Error>) -> ()
    ) {
        self.retrieveInteractions(
            inAnchor: .group,
            anchorId: groupId,
            ofType: type,
            underMessage: messageId,
            before: before,
            limit: limit
        ) { result in
            switch result {
            case .success(let res):
                completionHandler(.success(res))
            case .failure(let err):
                completionHandler(.failure(err))
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
    
    public func retrieveLocalInteraction(
        inGroup groupId: String,
        withId interactionIdentifier: String,
        completionHandler: @escaping (Result<any SHInteractionsCollectionProtocol, Error>) -> ()
    ) {
        self.serverProxy.retrieveLocalInteraction(
            inGroup: groupId,
            withId: interactionIdentifier
        ) { firstResult in
            switch firstResult {
            case .success(let localInteractionsGroup):
                self.decryptMessages(
                    in: localInteractionsGroup,
                    for: .group,
                    anchorId: groupId
                ) { secondResult in
                    switch secondResult {
                    case .success(let res):
                        completionHandler(.success(res))
                    case .failure(let err):
                        completionHandler(.failure(err))
                    }
                }
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    private func decryptMessages(
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
        completionHandler: @escaping (Result<ReactionOutputDTO, Error>) -> ()
    ) {
        let reactionInput = ReactionInputDTO(
            inReplyToAssetGlobalIdentifier: inReplyToAssetGlobalIdentifier,
            inReplyToInteractionId: inReplyToInteractionId,
            reactionType: reactionType
        )
        self.serverProxy.addReactions([reactionInput], toGroup: groupId) {
            result in
            switch result {
            case .success(let reactionOutputs):
                completionHandler(.success(reactionOutputs.first!))
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    public func removeReaction(
        _ reactionType: ReactionType,
        inReplyToAssetGlobalIdentifier: GlobalIdentifier?,
        inReplyToInteractionId: String?,
        fromGroup groupId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        self.serverProxy.removeReaction(
            reactionType,
            inReplyToAssetGlobalIdentifier: inReplyToAssetGlobalIdentifier,
            inReplyToInteractionId: inReplyToInteractionId,
            fromGroup: groupId,
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
