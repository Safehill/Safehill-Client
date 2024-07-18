import Foundation

public struct SHConversationThreadInteractions : SHInteractionsCollectionProtocol {
    public let threadId: String
    public let messages: [SHDecryptedMessage]
    public let reactions: [SHReaction]
    
    public var collectionId: String { threadId }
}
