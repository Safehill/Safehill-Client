import Foundation
import Safehill_Crypto

public protocol SHServerUser : SHCryptoUser {
    var identifier: String { get }
    var name: String { get }
    var phoneNumber: String? { get }
}


public struct SHRemoteUser : SHServerUser, Codable {
    public let identifier: String
    public let name: String
    public let publicKeyData: Data
    public let publicSignatureData: Data
    public let phoneNumber: String?
    
    enum CodingKeys: String, CodingKey {
        case identifier
        case name
        case publicKeyData = "publicKey"
        case publicSignatureData = "publicSignature"
        case phoneNumber
    }
    
    init(identifier: String,
         name: String,
         phoneNumber: String?,
         publicKeyData: Data,
         publicSignatureData: Data) {
        self.identifier = identifier
        self.publicKeyData = publicKeyData
        self.publicSignatureData = publicSignatureData
        self.name = name
        self.phoneNumber = phoneNumber
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        identifier = try container.decode(String.self, forKey: .identifier)
        name = try container.decode(String.self, forKey: .name)
        phoneNumber = try container.decode(String.self, forKey: .phoneNumber)
        let publicKeyDataBase64 = try container.decode(String.self, forKey: .publicKeyData)
        publicKeyData = Data(base64Encoded: publicKeyDataBase64)!
        let publicSignatureDataBase64 = try container.decode(String.self, forKey: .publicSignatureData)
        publicSignatureData = Data(base64Encoded: publicSignatureDataBase64)!
    }
}
