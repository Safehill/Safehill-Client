import Foundation


/// A class (not a swift struct, such as SHRemoteUser) for SHServer objects
/// to conform to NSSecureCoding, and safely store sharing information in the KBStore.
/// This serialization method is  relevant when storing SHSerializableQueueItem
/// in the queue, and hold user sharing information.
public class SHRemoteUserClass: NSObject, NSSecureCoding {
    
    public static var supportsSecureCoding: Bool = true
    
    public let identifier: String
    public let name: String
    public let phoneNumber: String?
    public let publicKeyData: Data
    public let publicSignatureData: Data
    
    enum CodingKeys: String, CodingKey {
        case identifier = "userIdentifier"
        case name = "userName"
        case phoneNumber
        case publicKeyData = "publicKey"
        case publicSignatureData = "publicSignature"
    }
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.identifier, forKey: CodingKeys.identifier.rawValue)
        coder.encode(self.name, forKey: CodingKeys.name.rawValue)
        coder.encode(self.phoneNumber, forKey: CodingKeys.phoneNumber.rawValue)
        coder.encode(self.publicKeyData.base64EncodedString(), forKey: CodingKeys.publicKeyData.rawValue)
        coder.encode(self.publicSignatureData.base64EncodedString(), forKey: CodingKeys.publicSignatureData.rawValue)
    }
    
    public init(identifier: String, name: String, phoneNumber: String?, publicKeyData: Data, publicSignatureData: Data) {
        self.identifier = identifier
        self.name = name
        self.phoneNumber = phoneNumber
        self.publicKeyData = publicKeyData
        self.publicSignatureData = publicSignatureData
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let identifier = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.identifier.rawValue)
        let name = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.name.rawValue)
        let phoneNumber = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.phoneNumber.rawValue)
        let publicKeyDataBase64 = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.publicKeyData.rawValue)
        let publicSignatureDataBase64 = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.publicSignatureData.rawValue)
        
        guard let identifier = identifier as? String else {
            log.error("unexpected value for identifier when decoding SHRemoteUserClass object")
            return nil
        }
        guard let name = name as? String else {
            log.error("unexpected value for name when decoding SHRemoteUserClass object")
            return nil
        }
        guard let publicKeyDataBase64 = publicKeyDataBase64 as? String,
              let publicKeyData = Data(base64Encoded: publicKeyDataBase64) else {
            log.error("unexpected value for publicKey when decoding SHRemoteUserClass object")
            return nil
        }
        guard let publicSignatureDataBase64 = publicSignatureDataBase64 as? String,
              let publicSignatureData = Data(base64Encoded: publicSignatureDataBase64) else {
            log.error("unexpected value for publicSignature when decoding SHRemoteUserClass object")
            return nil
        }
        
        self.init(identifier: identifier,
                  name: name,
                  phoneNumber: phoneNumber as? String,
                  publicKeyData: publicKeyData,
                  publicSignatureData: publicSignatureData)
    }
}
