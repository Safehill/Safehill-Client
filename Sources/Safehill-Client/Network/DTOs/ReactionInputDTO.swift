import Foundation


public enum ReactionType: Int {
    case like = 1, sad = 2, love = 3, funny = 4
}

public struct ReactionInputDTO {
    let inReplyToAssetGlobalIdentifier: String?
    let inReplyToInteractionId: String?
    let reactionType: ReactionType
}
