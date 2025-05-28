import Photos
import Foundation
import Safehill_Crypto


public class SHUploadableAsset : NSObject, NSSecureCoding {
    
    public static var supportsSecureCoding = true
    
    public let localIdentifier: LocalIdentifier?
    public let globalIdentifier: GlobalIdentifier
    public let fingerprint: String
    public let creationDate: Date?
    public let data: [SHAssetQuality: Data]
    
    enum CodingKeys: String, CodingKey {
        case localIdentifier
        case globalIdentifier
        case fingerprint
        case creationDate
        case data
    }
    
    public init(
        localIdentifier: LocalIdentifier?,
        globalIdentifier: GlobalIdentifier,
        fingerprint: String,
        creationDate: Date?,
        data: [SHAssetQuality: Data]
    ) {
        self.localIdentifier = localIdentifier
        self.globalIdentifier = globalIdentifier
        self.fingerprint = fingerprint
        self.creationDate = creationDate
        self.data = data
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let localIdentifier = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.localIdentifier.rawValue)
        let globalIdentifier = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.globalIdentifier.rawValue)
        let fingerprint = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.fingerprint.rawValue)
        let creationDateStr = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.creationDate.rawValue) as? String
        
        var dataDict = [SHAssetQuality: Data]()
        for quality in SHAssetQuality.all {
            if let qdata = decoder.decodeObject(of: NSData.self, forKey: CodingKeys.data.rawValue + "::" + quality.rawValue) as? Data {
                dataDict[quality] = qdata
            }
        }
        
        guard let globalIdentifier = globalIdentifier as? GlobalIdentifier else {
            log.error("unexpected value for globalIdentifier when decoding SHUploadableAsset object")
            return nil
        }
        
        guard let fingerprint = fingerprint as? String else {
            log.error("unexpected value for fingerprint when decoding SHUploadableAsset object")
            return nil
        }
        
        let creationDate: Date?
        if let creationDateStr {
            guard let date = creationDateStr.iso8601withFractionalSeconds else {
                log.error("unexpected value for creationDate when decoding SHUploadableAsset object")
                return nil
            }
            creationDate = date
        } else {
            creationDate = nil
        }
        
        self.init(
            localIdentifier: localIdentifier as? LocalIdentifier,
            globalIdentifier: globalIdentifier,
            fingerprint: fingerprint,
            creationDate: creationDate,
            data: dataDict
        )
    }
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.globalIdentifier, forKey: CodingKeys.globalIdentifier.rawValue)
        coder.encode(self.localIdentifier, forKey: CodingKeys.localIdentifier.rawValue)
        coder.encode(self.fingerprint, forKey: CodingKeys.fingerprint.rawValue)
        coder.encode(self.creationDate?.iso8601withFractionalSeconds, forKey: CodingKeys.creationDate.rawValue)
        
        for (version, data) in self.data {
            coder.encode(data, forKey: CodingKeys.data.rawValue + "::" + version.rawValue)
        }
    }
}


