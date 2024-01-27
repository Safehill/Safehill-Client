import Foundation

public struct SHConversationThreadInteractions {
    public let threadId: String
    public let messages: [SHDecryptedMessage]
    public let reactions: [SHReaction]
}
