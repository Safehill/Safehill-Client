import Photos
import Foundation
import Safehill_Crypto


public class SHUploadableAsset : NSObject, NSSecureCoding {
    
    public static var supportsSecureCoding = true
    
    public let localIdentifier: LocalIdentifier
    public let globalIdentifier: GlobalIdentifier
    public let creationDate: Date?
    let data: [SHAssetQuality: Data]
    
    enum CodingKeys: String, CodingKey {
        case localIdentifier
        case globalIdentifier
        case creationDate
        case data
    }
    
    public init(
        localIdentifier: LocalIdentifier,
        globalIdentifier: GlobalIdentifier,
        creationDate: Date?,
        data: [SHAssetQuality: Data]
    ) {
        self.localIdentifier = localIdentifier
        self.globalIdentifier = globalIdentifier
        self.creationDate = creationDate
        self.data = data
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let localIdentifier = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.localIdentifier.rawValue)
        let globalIdentifier = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.globalIdentifier.rawValue)
        let creationDateStr = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.creationDate.rawValue) as? String
        let data = decoder.decodeObject(of: NSDictionary.self, forKey: CodingKeys.creationDate.rawValue)
        
        guard let localIdentifier = localIdentifier as? String else {
            log.error("unexpected value for localIdentifier when decoding SHUploadableAsset object")
            return nil
        }
        
        guard let globalIdentifier = globalIdentifier as? String  else {
            log.error("unexpected value for globalIdentifier when decoding SHUploadableAsset object")
            return nil
        }
        
        guard let data = data as? [SHAssetQuality: Data] else {
            log.error("unexpected value for data when decoding SHUploadableAsset object")
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
            localIdentifier: localIdentifier,
            globalIdentifier: globalIdentifier,
            creationDate: creationDate,
            data: data
        )
    }
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.globalIdentifier, forKey: CodingKeys.globalIdentifier.rawValue)
        coder.encode(self.localIdentifier, forKey: CodingKeys.localIdentifier.rawValue)
        coder.encode(self.creationDate?.iso8601withFractionalSeconds, forKey: CodingKeys.creationDate.rawValue)
        coder.encode(self.data, forKey: CodingKeys.data.rawValue)
    }
}


