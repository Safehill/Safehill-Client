import Foundation

public class DBSecureSerializableAssetRef: NSObject, NSSecureCoding {
    
    public static var supportsSecureCoding: Bool = true
    
    public let globalIdentifier: String
    public let addedByUserIdentifier: String
    public let addedAt: String
    
    enum CodingKeys: String, CodingKey {
        case globalIdentifier
        case addedByUserIdentifier
        case addedAt
    }
    
    public init(
        globalIdentifier: String,
        addedByUserIdentifier: String,
        addedAt: String
    ) {
        self.globalIdentifier = globalIdentifier
        self.addedByUserIdentifier = addedByUserIdentifier
        self.addedAt = addedAt
    }
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.globalIdentifier, forKey: CodingKeys.globalIdentifier.rawValue)
        coder.encode(self.addedByUserIdentifier, forKey: CodingKeys.addedByUserIdentifier.rawValue)
        coder.encode(self.addedAt, forKey: CodingKeys.addedAt.rawValue)
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let globalIdentifier = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.globalIdentifier.rawValue)
        let addedByUserIdentifier = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.addedByUserIdentifier.rawValue)
        let addedAt = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.addedAt.rawValue)
        
        guard let globalIdentifier = globalIdentifier as? String else {
            log.error("unexpected value for globalIdentifier when decoding DBSecureSerializableAssetRef object")
            return nil
        }
        guard let addedByUserIdentifier = addedByUserIdentifier as? String else {
            log.error("unexpected value for addedByUserIdentifier when decoding DBSecureSerializableAssetRef object")
            return nil
        }
        guard let addedAt = addedAt as? String else {
            log.error("unexpected value for addedAt when decoding DBSecureSerializableAssetRef object")
            return nil
        }
        
        self.init(
            globalIdentifier: globalIdentifier,
            addedByUserIdentifier: addedByUserIdentifier,
            addedAt: addedAt
        )
    }
}
    
 
extension DBSecureSerializableAssetRef {
    
    func toDTO() -> AssetRefDTO {
        AssetRefDTO(
            globalIdentifier: self.globalIdentifier,
            addedByUserIdentifier: self.addedByUserIdentifier,
            addedAt: self.addedAt
        )
    }
    
    static func fromDTO(_ assetRef: AssetRefDTO) -> DBSecureSerializableAssetRef {
        DBSecureSerializableAssetRef(
            globalIdentifier: assetRef.globalIdentifier,
            addedByUserIdentifier: assetRef.addedByUserIdentifier,
            addedAt: assetRef.addedAt
        )
    }
    
    static func fromData(_ data: Data) throws -> DBSecureSerializableAssetRef? {
        let unarchiver: NSKeyedUnarchiver
        if #available(macOS 10.13, *) {
            unarchiver = try NSKeyedUnarchiver(forReadingFrom: data)
        } else {
            unarchiver = NSKeyedUnarchiver(forReadingWith: data)
        }
        guard let assetRef = unarchiver.decodeObject(
            of: DBSecureSerializableAssetRef.self,
            forKey: NSKeyedArchiveRootObjectKey
        ) else {
            log.critical("failed to decode photo message from data")
            return nil
        }
        return assetRef
    }
}
