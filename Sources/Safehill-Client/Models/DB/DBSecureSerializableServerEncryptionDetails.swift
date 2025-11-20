import Foundation
import KnowledgeBase

internal class DBSecureSerializableServerEncryptionDetails: NSObject, NSSecureCoding {
    
    public static var supportsSecureCoding: Bool = true
    
    let publicKey: String
    let publicSignature: String
    let encryptionProtocolSalt: String
    
    enum CodingKeys: String, CodingKey {
        case publicKey
        case publicSignature
        case encryptionProtocolSalt
    }
    
    init(
        publicKey: String,
        publicSignature: String,
        encryptionProtocolSalt: String
    ) {
        self.publicKey = publicKey
        self.publicSignature = publicSignature
        self.encryptionProtocolSalt = encryptionProtocolSalt
    }
    
    required convenience init?(coder decoder: NSCoder) {
        let publicKey = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.publicKey.rawValue) as? String
        let publicSignature = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.publicSignature.rawValue) as? String
        let encryptionProtocolSalt = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.encryptionProtocolSalt.rawValue) as? String
        
        guard let publicKey = publicKey else {
            log.error("unexpected value for publicKey when decoding DBSecureSerializableServerEncryptionDetails object")
            return nil
        }
        guard let publicSignature = publicSignature else {
            log.error("unexpected value for publicSignature when decoding DBSecureSerializableServerEncryptionDetails object")
            return nil
        }
        guard let encryptionProtocolSalt = encryptionProtocolSalt else {
            log.error("unexpected value for encryptionProtocolSalt when decoding DBSecureSerializableServerEncryptionDetails object")
            return nil
        }
        
        self.init(
            publicKey: publicKey,
            publicSignature: publicSignature,
            encryptionProtocolSalt: encryptionProtocolSalt
        )
    }
    
    func encode(with coder: NSCoder) {
        coder.encode(publicKey, forKey: CodingKeys.publicKey.rawValue)
        coder.encode(publicSignature, forKey: CodingKeys.publicSignature.rawValue)
        coder.encode(encryptionProtocolSalt, forKey: CodingKeys.encryptionProtocolSalt.rawValue)
    }
    
    static func from(_ any: Any) throws -> DBSecureSerializableServerEncryptionDetails {
        guard let serialized = any as? Data else {
            throw SHBackgroundOperationError.unexpectedData(any)
        }
        
        let unarchiver: NSKeyedUnarchiver
        if #available(macOS 10.13, *) {
            unarchiver = try NSKeyedUnarchiver(forReadingFrom: serialized)
        } else {
            unarchiver = NSKeyedUnarchiver(forReadingWith: serialized)
        }
        guard let result = unarchiver.decodeObject(
            of: DBSecureSerializableServerEncryptionDetails.self,
            forKey: NSKeyedArchiveRootObjectKey
        ) else {
            throw SHBackgroundOperationError.unexpectedData(serialized)
        }
        
        return result
    }
}
