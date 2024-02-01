import Foundation


public struct RecipientEncryptionDetailsDTO {
    let recipientUserIdentifier: String
    let ephemeralPublicKey: String // base64EncodedData with the ephemeral public part of the key used for the encryption
    let encryptedSecret: String // base64EncodedData with the secret to decrypt the encrypted content in this group for this user
    let secretPublicSignature: String // base64EncodedData with the public signature used for the encryption of the secret
    let senderPublicSignature: String // base64EncodedData with the public signature of the user sending it
}

public struct InteractionsGroupRecipientsDetailsDTO {
    var recipients: [RecipientEncryptionDetailsDTO]
}


// - MARK: SERDE


extension RecipientEncryptionDetailsDTO: Codable {
    enum CodingKeys: String, CodingKey {
        case recipientUserIdentifier
        case ephemeralPublicKey
        case encryptedSecret
        case secretPublicSignature
        case senderPublicSignature
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        recipientUserIdentifier = try container.decode(String.self, forKey: .recipientUserIdentifier)
        ephemeralPublicKey = try container.decode(String.self, forKey: .ephemeralPublicKey)
        encryptedSecret = try container.decode(String.self, forKey: .encryptedSecret)
        secretPublicSignature = try container.decode(String.self, forKey: .secretPublicSignature)
        senderPublicSignature = try container.decode(String.self, forKey: .senderPublicSignature)
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(recipientUserIdentifier, forKey: .recipientUserIdentifier)
        try container.encode(ephemeralPublicKey, forKey: .ephemeralPublicKey)
        try container.encode(encryptedSecret, forKey: .encryptedSecret)
        try container.encode(secretPublicSignature, forKey: .secretPublicSignature)
        try container.encode(senderPublicSignature, forKey: .senderPublicSignature)
    }
}
