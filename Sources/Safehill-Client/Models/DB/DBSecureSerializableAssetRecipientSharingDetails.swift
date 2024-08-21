import Foundation
import KnowledgeBase

internal class DBSecureSerializableAssetRecipientSharingDetails: NSObject, NSSecureCoding {
    
    public static var supportsSecureCoding: Bool = true
    
    let groupId: String
    let groupName: String?
    let groupCreationDate: Date?
    let quality: SHAssetQuality?
    let senderEncryptedSecret: Data?
    let ephemeralPublicKey: Data?
    let publicSignature: Data?
    
    enum CodingKeys: String, CodingKey {
        case groupId
        case groupName
        case groupCreationDate
        case quality
        case senderEncryptedSecret
        case ephemeralPublicKey
        case publicSignature
    }
    
    init(
        groupId: String,
        groupName: String?,
        groupCreationDate: Date?,
        quality: SHAssetQuality?,
        senderEncryptedSecret: Data?,
        ephemeralPublicKey: Data?,
        publicSignature: Data?
    ) {
        self.groupId = groupId
        self.groupName = groupName
        self.groupCreationDate = groupCreationDate
        self.quality = quality
        self.senderEncryptedSecret = senderEncryptedSecret
        self.ephemeralPublicKey = ephemeralPublicKey
        self.publicSignature = publicSignature
    }
    
    required convenience init?(coder decoder: NSCoder) {
        let groupId = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.groupId.rawValue) as? String
        let groupName = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.groupName.rawValue) as? String
        let groupCreationDateStr = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.groupCreationDate.rawValue) as? String
        let qualityStr = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.quality.rawValue) as? String
        let senderEncryptedSecretBase64 = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.senderEncryptedSecret.rawValue) as? String
        let ephemeralPublicKeyBase64 = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.ephemeralPublicKey.rawValue) as? String
        let publicSignatureBase64 = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.publicSignature.rawValue)
         as? String
        
        guard let groupId = groupId else {
            log.error("unexpected value for groupId when decoding DBSecureSerializableAssetRecipientSharingDetails object")
            return nil
        }
        
        let groupCreationDate: Date?
        if let groupCreationDateStr {
            guard let date = groupCreationDateStr.iso8601withFractionalSeconds else {
                log.error("unexpected value for groupCreationDate when decoding DBSecureSerializableAssetRecipientSharingDetails object")
                return nil
            }
            groupCreationDate = date
        } else {
            groupCreationDate = nil
        }
        
        let quality: SHAssetQuality?
        if let qualityStr {
            guard let q = SHAssetQuality(rawValue: qualityStr) else {
                log.error("unexpected value for quality when decoding DBSecureSerializableAssetRecipientSharingDetails object")
                return nil
            }
            quality = q
        } else {
            quality = nil
        }
        
        let senderEncryptedSecret: Data?
        if let senderEncryptedSecretBase64 {
            guard let data = Data(base64Encoded: senderEncryptedSecretBase64) else {
                log.error("unexpected value for senderEncryptedSecret when decoding DBSecureSerializableAssetRecipientSharingDetails object")
                return nil
            }
            senderEncryptedSecret = data
        } else {
            senderEncryptedSecret = nil
        }
        
        let ephemeralPublicKey: Data?
        if let ephemeralPublicKeyBase64 {
            guard let data = Data(base64Encoded: ephemeralPublicKeyBase64) else {
                log.error("unexpected value for ephemeralPublicKey when decoding DBSecureSerializableAssetRecipientSharingDetails object")
                return nil
            }
            ephemeralPublicKey = data
        } else {
            ephemeralPublicKey = nil
        }
        
        let publicSignature: Data?
        if let publicSignatureBase64 {
            guard let data = Data(base64Encoded: publicSignatureBase64) else {
                log.error("unexpected value for publicSignature when decoding DBSecureSerializableAssetRecipientSharingDetails object")
                return nil
            }
            publicSignature = data
        } else {
            publicSignature = nil
        }
        
        self.init(
            groupId: groupId,
            groupName: groupName,
            groupCreationDate: groupCreationDate,
            quality: quality,
            senderEncryptedSecret: senderEncryptedSecret,
            ephemeralPublicKey: ephemeralPublicKey,
            publicSignature: publicSignature
        )
    }
    
    func encode(with coder: NSCoder) {
        coder.encode(groupId, forKey: CodingKeys.groupId.rawValue)
        coder.encode(groupName, forKey: CodingKeys.groupName.rawValue)
        coder.encode(groupCreationDate?.iso8601withFractionalSeconds, forKey: CodingKeys.groupCreationDate.rawValue)
        coder.encode(quality?.rawValue, forKey: CodingKeys.quality.rawValue)
        coder.encode(senderEncryptedSecret?.base64EncodedString(), forKey: CodingKeys.senderEncryptedSecret.rawValue)
        coder.encode(ephemeralPublicKey?.base64EncodedString(), forKey: CodingKeys.ephemeralPublicKey.rawValue)
        coder.encode(publicSignature?.base64EncodedString(), forKey: CodingKeys.publicSignature.rawValue)
    }
}


extension KBKVPairs {
    func toRecipientSharingDetails() throws -> [String: DBSecureSerializableAssetRecipientSharingDetails] {
        
        return try self.mapValues { value in
            guard let serialized = value as? Data else {
                throw SHBackgroundOperationError.unexpectedData(value)
            }
            
            let unarchiver: NSKeyedUnarchiver
            if #available(macOS 10.13, *) {
                unarchiver = try NSKeyedUnarchiver(forReadingFrom: serialized)
            } else {
                unarchiver = NSKeyedUnarchiver(forReadingWith: serialized)
            }
            guard let result = unarchiver.decodeObject(
                of: DBSecureSerializableAssetRecipientSharingDetails.self,
                forKey: NSKeyedArchiveRootObjectKey
            ) else {
                throw SHBackgroundOperationError.unexpectedData(serialized)
            }
            
            return result
        }
    }
}
