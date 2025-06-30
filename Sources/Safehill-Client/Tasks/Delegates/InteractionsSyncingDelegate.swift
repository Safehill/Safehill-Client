import Foundation

public protocol SHInteractionsSyncingDelegate: SHInboundAssetOperationDelegate {
    func didUpdateThreadsList(_: [ConversationThreadOutputDTO])
    
    func didFetchRemoteThreadSummary(_: [String: InteractionsThreadSummaryDTO])
    func didFetchRemoteGroupSummary(_: [String: InteractionsGroupSummaryDTO])
    
    func didAddThread(_: ConversationThreadOutputDTO)
    
    func didRemoveThread(with threadId: String)
    
    func didReceiveTextMessages(_ messages: [MessageOutputDTO],
                                inGroup groupId: String)
    func didReceiveTextMessages(_: [MessageOutputDTO],
                                inThread threadId: String)
    
    func reactionsDidChange(inThread threadId: String)
    func reactionsDidChange(inGroup groupId: String)
    
    func didAddReaction(_: ReactionOutputDTO,
                        toGroup groupId: String)
    func didAddReaction(_: ReactionOutputDTO,
                        toThread threadId: String)
    func didRemoveReaction(_ reactionType: ReactionType,
                           addedBy senderPublicIdentifier: UserIdentifier,
                           inReplyToAssetGlobalIdentifier: GlobalIdentifier?,
                           inReplyToInteractionId: String?,
                           fromGroup groupId: String)
    func didRemoveReaction(_ reactionType: ReactionType,
                           addedBy senderPublicIdentifier: UserIdentifier,
                           inReplyToAssetGlobalIdentifier: GlobalIdentifier?,
                           inReplyToInteractionId: String?,
                           fromThread threadId: String)
}
