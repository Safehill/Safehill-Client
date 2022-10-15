import Foundation

public typealias BearerToken = String

public struct SHAuthResponse: Codable {
    public let user: SHRemoteUser
    public let bearerToken: BearerToken
    
    enum CodingKeys: String, CodingKey {
        case user
        case bearerToken
    }
    
    public init(user: SHRemoteUser, bearerToken: BearerToken) {
        self.user = user
        self.bearerToken = bearerToken
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        user = try container.decode(SHRemoteUser.self, forKey: .user)
        bearerToken = try container.decode(String.self, forKey: .bearerToken)
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(user, forKey: .user)
        try container.encode(bearerToken, forKey: .bearerToken)
    }
}


public struct SHAuthChallenge: Codable {
    public let challenge: String
    public let ephemeralPublicKey: String // base64EncodedData
    public let ephemeralPublicSignature: String // base64EncodedData
    public let publicKey: String // base64EncodedData
    public let publicSignature: String // base64EncodedData
    
    enum CodingKeys: String, CodingKey {
        case challenge
        case ephemeralPublicKey
        case ephemeralPublicSignature
        case publicKey
        case publicSignature
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        challenge = try container.decode(String.self, forKey: .challenge)
        ephemeralPublicKey = try container.decode(String.self, forKey: .ephemeralPublicKey)
        ephemeralPublicSignature = try container.decode(String.self, forKey: .ephemeralPublicSignature)
        publicKey = try container.decode(String.self, forKey: .publicKey)
        publicSignature = try container.decode(String.self, forKey: .publicSignature)
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(challenge, forKey: .challenge)
        try container.encode(ephemeralPublicKey, forKey: .ephemeralPublicKey)
        try container.encode(ephemeralPublicSignature, forKey: .ephemeralPublicSignature)
        try container.encode(publicKey, forKey: .publicKey)
        try container.encode(publicSignature, forKey: .publicSignature)
    }
}
