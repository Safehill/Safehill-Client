import Foundation
import Safehill_Crypto
import CryptoKit

extension SHUserInteractionController {
    
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
    
    public func deleteGroup(groupId: String, completionHandler: @escaping (Result<Void, Error>) -> ()) {
        guard self.user as? SHAuthenticatedLocalUser != nil else {
            completionHandler(.failure(SHLocalUserError.notAuthenticated))
            return
        }
        
        SHUserInteractionController.encryptionDetailsCache.evict(anchor: .group, anchorId: groupId)
        self.serverProxy.deleteGroup(groupId: groupId, completionHandler: completionHandler)
    }
    
    public func fetchGroupsInteractionsSummary() async throws -> [String: InteractionsGroupSummaryDTO] {
        return try await self.serverProxy.topLevelGroupsInteractionsSummary()
    }
    
    public func reloadLocalInteractionsSummary(
        for groupId: String
    ) async throws -> InteractionsGroupSummaryDTO {
        return try await self.serverProxy.topLevelLocalInteractionsSummary(for: groupId)
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
    
    public func send(
        message: String,
        inGroup groupId: String,
        inReplyToAssetGlobalIdentifier: String? = nil,
        inReplyToInteractionId: String? = nil,
        completionHandler: @escaping (Result<MessageOutputDTO, Error>) -> ()
    ) {
        self.send(message: message, inAnchor: .group, anchorId: groupId, completionHandler: completionHandler)
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
