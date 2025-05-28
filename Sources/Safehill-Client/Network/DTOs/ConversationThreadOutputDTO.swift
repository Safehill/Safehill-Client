import Foundation

public protocol ConversationThreadUpdate {
    var threadId: String { get }
    var name: String? { get }
    var membersPublicIdentifier: [UserIdentifier] { get }
    var invitedUsersPhoneNumbers: [String: String] { get }
    var lastUpdatedAt: String? { get }
}

public struct ConversationThreadOutputDTO: ConversationThreadUpdate, Codable {
    public let threadId: String
    public let name: String?
    public let creatorPublicIdentifier: UserIdentifier?
    public let membersPublicIdentifier: [UserIdentifier]
    public let invitedUsersPhoneNumbers: [String: String]
    public let createdAt: String
    public let lastUpdatedAt: String?
    public let encryptionDetails: RecipientEncryptionDetailsDTO // for the user making the request
}
