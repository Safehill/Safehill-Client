import Foundation

public struct SHAssetsGroupInteractions : SHInteractionsCollectionProtocol {
    public let groupId: String
    public let messages: [SHDecryptedMessage]
    public let reactions: [SHReaction]
    
    var collectionId: String { groupId }
}
