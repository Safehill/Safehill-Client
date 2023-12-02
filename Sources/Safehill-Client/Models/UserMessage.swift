import Foundation

public struct SHDecryptedMessage {
    public let interactionId: String
    public let sender: SHServerUser
    public let inReplyToAssetGlobalIdentifier: GlobalIdentifier?
    public let inReplyToInteractionId: String?
    public let message: String
    public let createdAt: Date
}

