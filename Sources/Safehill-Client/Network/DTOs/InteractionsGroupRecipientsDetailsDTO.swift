import Foundation


public struct RecipientEncryptionDetailsDTO {
    var userIdentifier: String
    var ephemeralPublicKey: String // base64EncodedData with the ephemeral public part of the key used for the encryption
    var encryptedSecret: String // base64EncodedData with the secret to decrypt the encrypted content in this group for this user
    var secretPublicSignature: String // base64EncodedData with the public signature of the user sending it
}

public struct InteractionsGroupRecipientsDetailsDTO {
    var recipients: [RecipientEncryptionDetailsDTO]
}


// - MARK: SERDE


extension RecipientEncryptionDetailsDTO: Codable {
    enum CodingKeys: String, CodingKey {
        case userIdentifier
        case ephemeralPublicKey
        case encryptedSecret
        case secretPublicSignature
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        userIdentifier = try container.decode(String.self, forKey: .userIdentifier)
        ephemeralPublicKey = try container.decode(String.self, forKey: .ephemeralPublicKey)
        encryptedSecret = try container.decode(String.self, forKey: .encryptedSecret)
        secretPublicSignature = try container.decode(String.self, forKey: .secretPublicSignature)
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(userIdentifier, forKey: .userIdentifier)
        try container.encode(ephemeralPublicKey, forKey: .ephemeralPublicKey)
        try container.encode(encryptedSecret, forKey: .encryptedSecret)
        try container.encode(secretPublicSignature, forKey: .secretPublicSignature)
    }
}
