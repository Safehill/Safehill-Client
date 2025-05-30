import Foundation
import KnowledgeBase

internal class DBSecureSerializableAssetVersionMetadata: NSObject, NSSecureCoding {
    
    public static var supportsSecureCoding: Bool = true
    
    let globalIdentifier: GlobalIdentifier
    let localIdentifier: LocalIdentifier?
    let quality: SHAssetQuality
    let senderEncryptedSecret: Data
    let publicKey: Data
    let publicSignature: Data
    let creationDate: Date?
    let uploadState: SHAssetDescriptorUploadState
    
    enum CodingKeys: String, CodingKey {
        case globalIdentifier
        case localIdentifier
        case quality
        case senderEncryptedSecret
        case publicKey
        case publicSignature
        case creationDate
        case uploadState
    }
    
    init(
        globalIdentifier: GlobalIdentifier,
        localIdentifier: LocalIdentifier?,
        quality: SHAssetQuality,
        senderEncryptedSecret: Data,
        publicKey: Data,
        publicSignature: Data,
        creationDate: Date?,
        uploadState: SHAssetDescriptorUploadState
    ) {
        self.globalIdentifier = globalIdentifier
        self.localIdentifier = localIdentifier
        self.quality = quality
        self.senderEncryptedSecret = senderEncryptedSecret
        self.publicKey = publicKey
        self.publicSignature = publicSignature
        self.creationDate = creationDate
        self.uploadState = uploadState
    }
    
    required convenience init?(coder decoder: NSCoder) {
        let globalIdentifier = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.globalIdentifier.rawValue) as? String
        let localIdentifier = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.localIdentifier.rawValue) as? String
        let qualityStr = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.quality.rawValue) as? String
        let senderEncryptedSecretBase64 = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.senderEncryptedSecret.rawValue) as? String
        let publicKeyBase64 = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.publicKey.rawValue) as? String
        let publicSignatureBase64 = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.publicSignature.rawValue) as? String
        let creationDateStr = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.creationDate.rawValue) as? String
        let uploadStateStr = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.uploadState.rawValue) as? String
        
        guard let globalIdentifier = globalIdentifier else {
            log.error("unexpected value for globalIdentifier when decoding DBSecureSerializableAssetVersionMetadata object")
            return nil
        }
        
        guard let qualityStr,
              let quality = SHAssetQuality(rawValue: qualityStr) else {
            log.error("unexpected value for quality when decoding DBSecureSerializableAssetVersionMetadata object")
            return nil
        }
        
        guard let senderEncryptedSecretBase64,
              let senderEncryptedSecret = Data(base64Encoded: senderEncryptedSecretBase64) else {
            log.error("unexpected value for senderEncryptedSecret when decoding DBSecureSerializableAssetVersionMetadata object")
            return nil
        }
        
        guard let publicKeyBase64,
              let publicKey = Data(base64Encoded: publicKeyBase64) else {
            log.error("unexpected value for publicKey when decoding DBSecureSerializableAssetVersionMetadata object")
            return nil
        }
        
        guard let publicSignatureBase64,
              let publicSignature = Data(base64Encoded: publicSignatureBase64) else {
            log.error("unexpected value for publicSignature when decoding DBSecureSerializableAssetVersionMetadata object")
            return nil
        }
        
        let creationDate: Date?
        if let creationDateStr {
            guard let date = creationDateStr.iso8601withFractionalSeconds else {
                log.error("unexpected value for creationDate when decoding DBSecureSerializableAssetVersionMetadata object")
                return nil
            }
            creationDate = date
        } else {
            creationDate = nil
        }
        
        guard let uploadStateStr,
              let uploadState = SHAssetDescriptorUploadState(rawValue: uploadStateStr) else {
            log.error("unexpected value for uploadState when decoding DBSecureSerializableAssetVersionMetadata object")
            return nil
        }
        
        self.init(
            globalIdentifier: globalIdentifier,
            localIdentifier: localIdentifier,
            quality: quality,
            senderEncryptedSecret: senderEncryptedSecret,
            publicKey: publicKey,
            publicSignature: publicSignature,
            creationDate: creationDate,
            uploadState: uploadState
        )
    }
    
    func encode(with coder: NSCoder) {
        coder.encode(globalIdentifier, forKey: CodingKeys.globalIdentifier.rawValue)
        coder.encode(localIdentifier, forKey: CodingKeys.localIdentifier.rawValue)
        coder.encode(quality.rawValue, forKey: CodingKeys.quality.rawValue)
        coder.encode(senderEncryptedSecret.base64EncodedString(), forKey: CodingKeys.senderEncryptedSecret.rawValue)
        coder.encode(publicKey.base64EncodedString(), forKey: CodingKeys.publicKey.rawValue)
        coder.encode(publicSignature.base64EncodedString(), forKey: CodingKeys.publicSignature.rawValue)
        coder.encode(creationDate?.iso8601withFractionalSeconds, forKey: CodingKeys.creationDate.rawValue)
        coder.encode(uploadState.rawValue, forKey: CodingKeys.uploadState.rawValue)
    }

    static func from(_ any: Any) throws -> DBSecureSerializableAssetVersionMetadata {
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
            of: DBSecureSerializableAssetVersionMetadata.self,
            forKey: NSKeyedArchiveRootObjectKey
        ) else {
            throw SHBackgroundOperationError.unexpectedData(serialized)
        }
        
        return result
    }
}
