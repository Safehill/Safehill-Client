import Foundation

internal protocol SHServerProxyProtocol {
    init(user: SHLocalUserProtocol)
    
    func listThreads() async throws -> [ConversationThreadOutputDTO]
    
    func listLocalThreads(
        withIdentifiers threadIds: [String]?
    ) async throws -> [ConversationThreadOutputDTO]
    
    func createOrUpdateThread(
        name: String?,
        recipientsEncryptionDetails: [RecipientEncryptionDetailsDTO]?,
        invitedPhoneNumbers: [String]?,
        completionHandler: @escaping (Result<ConversationThreadOutputDTO, Error>) -> ()
    )
    
    func updateThread(
        _ threadId: String,
        newName: String?,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    )
    
    func deleteThread(
        withId threadId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    )
    
    func setupGroupEncryptionDetails(
        groupId: String,
        recipientsEncryptionDetails: [RecipientEncryptionDetailsDTO],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    )
    
    func deleteGroup(
        groupId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    )
    
    func getAssets(
        inThread threadId: String,
        completionHandler: @escaping (Result<ConversationThreadAssetsDTO, Error>) -> ()
    )
    
    func addReactions(
        _ reactions: [ReactionInput],
        toGroup groupId: String,
        completionHandler: @escaping (Result<[ReactionOutputDTO], Error>) -> ()
    )
    
    func removeReaction(
        _: ReactionType,
        inReplyToAssetGlobalIdentifier: GlobalIdentifier?,
        inReplyToInteractionId: String?,
        fromGroup groupId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    )
    
    func addMessage(
        _ message: MessageInputDTO,
        toGroup groupId: String,
        completionHandler: @escaping (Result<MessageOutputDTO, Error>) -> ()
    )
    
    func addMessage(
        _ message: MessageInputDTO,
        toThread threadId: String,
        completionHandler: @escaping (Result<MessageOutputDTO, Error>) -> ()
    )
    
    func addLocalMessages(
        _ messages: [MessageInput],
        toGroup groupId: String,
        completionHandler: @escaping (Result<[MessageOutputDTO], Error>) -> ()
    )
    
    func addLocalMessages(
        _ messages: [MessageInput],
        toThread threadId: String,
        completionHandler: @escaping (Result<[MessageOutputDTO], Error>) -> ()
    )
    
    func addLocalReactions(
        _ reactions: [ReactionInput],
        toGroup groupId: String,
        completionHandler: @escaping (Result<[ReactionOutputDTO], Error>) -> ()
    )
    
    func addLocalReactions(
        _ reactions: [ReactionInput],
        toThread threadId: String,
        completionHandler: @escaping (Result<[ReactionOutputDTO], Error>) -> ()
    )
    
    ///
    /// Retrieve the interactions from remote server first, because whatever was cached could be lacking messages.
    /// The ones retrieved according to the query will be cached locally, for offline access.
    ///
    /// - Parameters:
    ///   - groupId: the identifier of the share, aka the `groupId`
    ///   - type: (optional) filter the type of the interaction: message or reaction only
    ///   - messageId: (optional) if a sub-thread the message it's anchored to
    ///   - before: (optional) only messages before a specific date
    ///   - limit: limit the number of results
    ///   - completionHandler: the callback method with the encryption details and the result
    func retrieveInteractions(
        inGroup groupId: String,
        ofType type: InteractionType?,
        underMessage messageId: String?,
        before: Date?,
        limit: Int,
        completionHandler: @escaping (Result<InteractionsGroupDTO, Error>) -> ()
    )
    
    ///
    /// Retrieve the interactions from remote server first, because whatever was cached could be lacking messages.
    /// The ones retrieved according to the query will be cached locally, for offline access.
    ///
    /// - Parameters:
    ///   - threadId: the thread identifier
    ///   - type: (optional) filter the type of the interaction: message or reaction only
    ///   - messageId: (optional) if a sub-thread the message it's anchored to
    ///   - before: (optional) only messages before a specific date
    ///   - limit: limit the number of results
    ///   - completionHandler: the callback method with the encryption details and the result
    func retrieveInteractions(
        inThread threadId: String,
        ofType type: InteractionType?,
        underMessage messageId: String?,
        before: Date?,
        limit: Int,
        completionHandler: @escaping (Result<InteractionsGroupDTO, Error>) -> ()
    )
    
    func retrieveLocalInteractions(
        inGroup groupId: String,
        ofType type: InteractionType?,
        underMessage messageId: String?,
        before: Date?,
        limit: Int,
        completionHandler: @escaping (Result<InteractionsGroupDTO, Error>) -> ()
    )
    
    func retrieveLocalInteractions(
        inThread threadId: String,
        ofType type: InteractionType?,
        underMessage messageId: String?,
        before: Date?,
        limit: Int,
        completionHandler: @escaping (Result<InteractionsGroupDTO, Error>) -> ()
    )
    
    func retrieveRemoteInteractions(
        inGroup groupId: String,
        ofType type: InteractionType?,
        underMessage messageId: String?,
        before: Date?,
        limit: Int,
        completionHandler: @escaping (Result<InteractionsGroupDTO, Error>) -> ()
    )
    
    func retrieveRemoteInteractions(
        inThread threadId: String,
        ofType type: InteractionType?,
        underMessage messageId: String?,
        before: Date?,
        limit: Int,
        completionHandler: @escaping (Result<InteractionsGroupDTO, Error>) -> ()
    )
    
    func retrieveLocalInteraction(
        inThread threadId: String,
        withId interactionIdentifier: String,
        completionHandler: @escaping (Result<InteractionsGroupDTO, Error>) -> ()
    )
        
    func retrieveLocalInteraction(
        inGroup groupId: String,
        withId interactionIdentifier: String,
        completionHandler: @escaping (Result<InteractionsGroupDTO, Error>) -> ()
    )
    
    func retrieveUserEncryptionDetails(
        forGroup groupId: String,
        completionHandler: @escaping (Result<RecipientEncryptionDetailsDTO?, Error>) -> ()
    )
    
    func retrieveUserEncryptionDetails(
        forThread threadId: String,
        completionHandler: @escaping (Result<RecipientEncryptionDetailsDTO?, Error>) -> ()
    )
    
    func topLevelInteractionsSummary() async throws -> InteractionsSummaryDTO
    
    func topLevelThreadsInteractionsSummary() async throws -> [String: InteractionsThreadSummaryDTO]
    
    func topLevelGroupsInteractionsSummary() async throws -> [String: InteractionsGroupSummaryDTO]
    
    func topLevelLocalInteractionsSummary(for groupId: String) async throws -> InteractionsGroupSummaryDTO
    
    func getThread(
        withUsers users: [any SHServerUser],
        and phoneNumbers: [String],
        completionHandler: @escaping (Result<ConversationThreadOutputDTO?, Error>) -> ()
    )
    
    func invite(
        _ phoneNumbers: [String],
        to groupId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    )
    
    func uninvite(
        _ phoneNumbers: [String],
        from groupId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    )
}

