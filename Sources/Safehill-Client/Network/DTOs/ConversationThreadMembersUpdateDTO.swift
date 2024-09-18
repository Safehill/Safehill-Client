import Foundation

public struct ConversationThreadMembersUpdateDTO: Codable {
    let recipientsToAdd: [RecipientEncryptionDetailsDTO]
    let membersPublicIdentifierToRemove: [UserIdentifier]
    
    let phoneNumbersToAdd: [String]
    let phoneNumbersToRemove: [String]
}
