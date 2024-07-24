import Foundation

public struct SHReaction {
    public let interactionId: String
    public let sender: SHServerUser
    public let inReplyToAssetGlobalIdentifier: GlobalIdentifier?
    public let inReplyToInteractionId: String?
    public let reactionType: ReactionType
    public let addedAt: Date
}
