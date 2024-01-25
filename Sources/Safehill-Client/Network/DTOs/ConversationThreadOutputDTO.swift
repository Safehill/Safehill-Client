import Foundation

public struct ConversationThreadOutputDTO: Codable {
    let threadId: String
    let name: String?
    let membersPublicIdentifier: [String]
    let encryptionDetails: RecipientEncryptionDetailsDTO // for the user making the request
}
