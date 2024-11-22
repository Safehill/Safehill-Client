import Foundation

public struct InteractionsGroupDetailsResponseDTO: Codable {
    public let encryptedTitle: String? // base64EncodedData with the cipher
    public let encryptionDetails: RecipientEncryptionDetailsDTO
}

