import Foundation

public struct UserDTO: Codable {
    var identifier: String
    var name: String
    var email: String?
    var phoneNumber: String?
    var publicKey: String // base64EncodedData
    var publicSignature: String // base64EncodedData
    
    enum CodingKeys: String, CodingKey {
        case identifier
        case name
        case email
        case phoneNumber
        case publicKey
        case publicSignature
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        identifier = try container.decode(String.self, forKey: .identifier)
        name = try container.decode(String.self, forKey: .name)
        email = try? container.decode(String?.self, forKey: .email)
        phoneNumber = try? container.decode(String?.self, forKey: .phoneNumber)
        publicKey = try container.decode(String.self, forKey: .publicKey)
        publicSignature = try container.decode(String.self, forKey: .publicSignature)
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        
        try container.encode(identifier, forKey: .identifier)
        try container.encode(name, forKey: .name)
        try container.encode(email, forKey: .name)
        try container.encode(phoneNumber, forKey: .name)
        try container.encode(publicKey, forKey: .name)
        try container.encode(publicSignature, forKey: .name)
    }
}
