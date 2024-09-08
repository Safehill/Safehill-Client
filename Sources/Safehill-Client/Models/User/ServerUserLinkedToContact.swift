import Foundation

public struct SHRemoteUserLinkedToContact: SHServerUser, Codable {
    public let identifier: String
    public let name: String
    public let publicKeyData: Data
    public let publicSignatureData: Data
    
    public let phoneNumber: String?
    public let linkedSystemContactId: String
    
    init(identifier: String,
         name: String,
         publicKeyData: Data,
         publicSignatureData: Data,
         phoneNumber: String,
         linkedSystemContactId: String) {
        self.identifier = identifier
        self.name = name
        self.publicKeyData = publicKeyData
        self.publicSignatureData = publicSignatureData
        self.phoneNumber = phoneNumber
        self.linkedSystemContactId = linkedSystemContactId
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.identifier = try container.decode(String.self, forKey: .identifier)
        self.name = try container.decode(String.self, forKey: .name)
        let publicKeyDataBase64 = try container.decode(String.self, forKey: .publicKeyData)
        self.publicKeyData = Data(base64Encoded: publicKeyDataBase64)!
        let publicSignatureDataBase64 = try container.decode(String.self, forKey: .publicSignatureData)
        self.publicSignatureData = Data(base64Encoded: publicSignatureDataBase64)!
        self.phoneNumber = try container.decode(String.self, forKey: .phoneNumber)
        self.linkedSystemContactId = try container.decode(String.self, forKey: .linkedSystemContactId)
    }
}
