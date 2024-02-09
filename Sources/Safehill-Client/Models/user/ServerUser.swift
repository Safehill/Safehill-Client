import Foundation
import Safehill_Crypto

public protocol SHServerUser : SHCryptoUser {
    var identifier: String { get }
    var name: String { get }
}


public struct SHRemoteUser : SHServerUser, Codable {
    public let identifier: String
    public let name: String
    public let publicKeyData: Data
    public let publicSignatureData: Data
    
    enum CodingKeys: String, CodingKey {
        case identifier
        case name
        case publicKeyData = "publicKey"
        case publicSignatureData = "publicSignature"
    }
    
    init(identifier: String,
         name: String,
         publicKeyData: Data,
         publicSignatureData: Data) {
        self.identifier = identifier
        self.publicKeyData = publicKeyData
        self.publicSignatureData = publicSignatureData
        self.name = name
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        identifier = try container.decode(String.self, forKey: .identifier)
        name = try container.decode(String.self, forKey: .name)
        let publicKeyDataBase64 = try container.decode(String.self, forKey: .publicKeyData)
        publicKeyData = Data(base64Encoded: publicKeyDataBase64)!
        let publicSignatureDataBase64 = try container.decode(String.self, forKey: .publicSignatureData)
        publicSignatureData = Data(base64Encoded: publicSignatureDataBase64)!
    }
}
