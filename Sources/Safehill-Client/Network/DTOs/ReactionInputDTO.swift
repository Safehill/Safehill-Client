import Foundation


public enum ReactionType: Int {
    case like = 1, sad = 2, love = 3, funny = 4
}

public protocol ReactionInput {
    var interactionId: String? { get }
    var senderUserIdentifier: String? { get }
    var inReplyToAssetGlobalIdentifier: String? { get }
    var inReplyToInteractionId: String? { get }
    var reactionType: ReactionType { get }
    var addedAt: String? { get } // ISO8601 formatted datetime
}

public struct ReactionInputDTO {
    public let inReplyToAssetGlobalIdentifier: String?
    public let inReplyToInteractionId: String?
    public let reactionType: ReactionType
}

extension ReactionInputDTO : ReactionInput {
    public var interactionId: String? {
        return nil
    }
    
    public var senderUserIdentifier: String? {
        return nil
    }
    
    public var addedAt: String? {
        return nil
    }
}
