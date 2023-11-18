import Foundation


public struct InteractionsGroupDTO {
    let messages: [MessageOutputDTO]
    public let reactions: [ReactionOutputDTO]
    
    var ephemeralPublicKey: String // base64EncodedData with the ephemeral public part of the key used for the encryption
    var encryptedSecret: String // base64EncodedData with the secret to decrypt the encrypted content in this group for this user
    var secretPublicSignature: String // base64EncodedData with the public signature of the user sending it
    
    init(messages: [MessageOutputDTO], reactions: [ReactionOutputDTO], ephemeralPublicKey: String, encryptedSecret: String, secretPublicSignature: String) {
        self.messages = messages
        self.reactions = reactions
        self.ephemeralPublicKey = ephemeralPublicKey
        self.encryptedSecret = encryptedSecret
        self.secretPublicSignature = secretPublicSignature
    }
}

// - MARK: SERDE

extension InteractionsGroupDTO: Codable {
    enum CodingKeys: String, CodingKey {
        case messages
        case reactions
        case ephemeralPublicKey
        case encryptedSecret
        case secretPublicSignature
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        messages = try container.decode([MessageOutputDTO].self, forKey: .messages)
        reactions = try container.decode([ReactionOutputDTO].self, forKey: .reactions)
        ephemeralPublicKey = try container.decode(String.self, forKey: .ephemeralPublicKey)
        encryptedSecret = try container.decode(String.self, forKey: .encryptedSecret)
        secretPublicSignature = try container.decode(String.self, forKey: .secretPublicSignature)
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(messages, forKey: .messages)
        try container.encode(reactions, forKey: .reactions)
        try container.encode(ephemeralPublicKey, forKey: .ephemeralPublicKey)
        try container.encode(encryptedSecret, forKey: .encryptedSecret)
        try container.encode(secretPublicSignature, forKey: .secretPublicSignature)
    }
}