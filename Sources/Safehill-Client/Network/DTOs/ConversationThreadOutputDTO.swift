import Foundation

public struct ConversationThreadOutputDTO: Codable {
    public let threadId: String
    public let name: String?
    public let creatorPublicIdentifier: UserIdentifier?
    public let membersPublicIdentifier: [UserIdentifier]
    public let createdAt: String
    public let lastUpdatedAt: String?
    public let encryptionDetails: RecipientEncryptionDetailsDTO // for the user making the request
}
