import Foundation

public struct ConversationThreadMembersUpdateDTO: Codable {
    public let recipientsToAdd: [RecipientEncryptionDetailsDTO]
    public let membersPublicIdentifierToRemove: [UserIdentifier]
    
    public let phoneNumbersToAdd: [String]
    public let phoneNumbersToRemove: [String]
}
