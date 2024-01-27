import Foundation

public struct SHUserGroupInteractions {
    public let groupId: String
    public let messages: [SHDecryptedMessage]
    public let reactions: [SHReaction]
}
