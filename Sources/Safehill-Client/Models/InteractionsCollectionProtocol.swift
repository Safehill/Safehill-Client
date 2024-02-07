import Foundation

protocol SHInteractionsCollectionProtocol {
    var collectionId: String { get }
    var messages: [SHDecryptedMessage] { get }
    var reactions: [SHReaction] { get }
}
