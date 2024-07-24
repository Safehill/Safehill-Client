import Foundation

public typealias BearerToken = String

public struct SHAuthResponseMetadata: Codable {
    public let isPhoneNumberVerified: Bool
    public let forceReindex: Bool
}

public struct SHAuthResponse: Codable {
    public let user: SHRemoteUser
    public let bearerToken: BearerToken
    public let encryptionProtocolSalt: String // base64EncodedData
    public let metadata: SHAuthResponseMetadata?
    
    enum CodingKeys: String, CodingKey {
        case user
        case bearerToken
        case encryptionProtocolSalt
        case metadata
    }
    
    public init(user: SHRemoteUser,
                bearerToken: BearerToken,
                encryptionProtocolSalt: String,
                metadata: SHAuthResponseMetadata?) {
        self.user = user
        self.bearerToken = bearerToken
        self.encryptionProtocolSalt = encryptionProtocolSalt
        self.metadata = metadata
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        user = try container.decode(SHRemoteUser.self, forKey: .user)
        bearerToken = try container.decode(String.self, forKey: .bearerToken)
        encryptionProtocolSalt = try container.decode(String.self, forKey: .encryptionProtocolSalt)
        metadata = try container.decodeIfPresent(SHAuthResponseMetadata.self, forKey: .metadata)
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(user, forKey: .user)
        try container.encode(bearerToken, forKey: .bearerToken)
        try container.encode(encryptionProtocolSalt, forKey: .encryptionProtocolSalt)
        try container.encode(metadata, forKey: .metadata)
    }
}


public struct SHAuthChallenge: Codable {
    public let challenge: String
    public let ephemeralPublicKey: String // base64EncodedData
    public let ephemeralPublicSignature: String // base64EncodedData
    public let publicKey: String // base64EncodedData
    public let publicSignature: String // base64EncodedData
    public let protocolSalt: String // base64EncodedData
    /// Can be null when the IV is the first 16 bytes of the `challenge`
    public let iv: String?  // base64EncodedData
    
    enum CodingKeys: String, CodingKey {
        case challenge
        case ephemeralPublicKey
        case ephemeralPublicSignature
        case publicKey
        case publicSignature
        case protocolSalt
        case iv
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        challenge = try container.decode(String.self, forKey: .challenge)
        ephemeralPublicKey = try container.decode(String.self, forKey: .ephemeralPublicKey)
        ephemeralPublicSignature = try container.decode(String.self, forKey: .ephemeralPublicSignature)
        publicKey = try container.decode(String.self, forKey: .publicKey)
        publicSignature = try container.decode(String.self, forKey: .publicSignature)
        protocolSalt = try container.decode(String.self, forKey: .protocolSalt)
        iv = try? container.decodeIfPresent(String.self, forKey: .iv)
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(challenge, forKey: .challenge)
        try container.encode(ephemeralPublicKey, forKey: .ephemeralPublicKey)
        try container.encode(ephemeralPublicSignature, forKey: .ephemeralPublicSignature)
        try container.encode(publicKey, forKey: .publicKey)
        try container.encode(publicSignature, forKey: .publicSignature)
        try container.encode(protocolSalt, forKey: .protocolSalt)
        try container.encode(iv, forKey: .iv)
    }
}
