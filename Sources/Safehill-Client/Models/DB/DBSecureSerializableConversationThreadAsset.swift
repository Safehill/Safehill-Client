import Foundation

public class DBSecureSerializableAssetGroupLinkage: NSObject, NSSecureCoding {
    
    public static var supportsSecureCoding: Bool = true
    
    public let globalIdentifier: String
    public let addedByUserIdentifier: String
    public let addedAt: String
    public let groupId: String
    
    enum CodingKeys: String, CodingKey {
        case globalIdentifier
        case addedByUserIdentifier
        case addedAt
        case groupId
    }
    
    public init(
        globalIdentifier: String,
        addedByUserIdentifier: String,
        addedAt: String,
        groupId: String
    ) {
        self.globalIdentifier = globalIdentifier
        self.addedByUserIdentifier = addedByUserIdentifier
        self.addedAt = addedAt
        self.groupId = groupId
    }
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.globalIdentifier, forKey: CodingKeys.globalIdentifier.rawValue)
        coder.encode(self.addedByUserIdentifier, forKey: CodingKeys.addedByUserIdentifier.rawValue)
        coder.encode(self.addedAt, forKey: CodingKeys.addedAt.rawValue)
        coder.encode(self.groupId, forKey: CodingKeys.groupId.rawValue)
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let globalIdentifier = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.globalIdentifier.rawValue)
        let addedByUserIdentifier = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.addedByUserIdentifier.rawValue)
        let addedAt = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.addedAt.rawValue)
        let groupId = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.groupId.rawValue)
        
        guard let globalIdentifier = globalIdentifier as? String else {
            log.error("unexpected value for globalIdentifier when decoding DBSecureSerializableAssetGroupLinkage object")
            return nil
        }
        guard let addedByUserIdentifier = addedByUserIdentifier as? String else {
            log.error("unexpected value for addedByUserIdentifier when decoding DBSecureSerializableAssetGroupLinkage object")
            return nil
        }
        guard let addedAt = addedAt as? String else {
            log.error("unexpected value for addedAt when decoding DBSecureSerializableAssetGroupLinkage object")
            return nil
        }
        guard let groupId = groupId as? String else {
            log.error("unexpected value for groupId when decoding DBSecureSerializableAssetGroupLinkage object")
            return nil
        }
        
        self.init(
            globalIdentifier: globalIdentifier,
            addedByUserIdentifier: addedByUserIdentifier,
            addedAt: addedAt,
            groupId: groupId
        )
    }
    
    func toDTO() -> AssetGroupLinkageDTO {
        AssetGroupLinkageDTO(
            globalIdentifier: self.globalIdentifier,
            addedByUserIdentifier: self.addedByUserIdentifier,
            addedAt: self.addedAt,
            groupId: self.groupId
        )
    }
    
    static func fromDTO(_ assetGroupLinkage: AssetGroupLinkageDTO) -> DBSecureSerializableAssetGroupLinkage {
        DBSecureSerializableAssetGroupLinkage(
            globalIdentifier: assetGroupLinkage.globalIdentifier,
            addedByUserIdentifier: assetGroupLinkage.addedByUserIdentifier,
            addedAt: assetGroupLinkage.addedAt,
            groupId: assetGroupLinkage.groupId
        )
    }
    
    static func deserialize(from data: Data) throws -> DBSecureSerializableAssetGroupLinkage {
        let unarchiver: NSKeyedUnarchiver
        if #available(macOS 10.13, *) {
            unarchiver = try NSKeyedUnarchiver(forReadingFrom: data)
        } else {
            unarchiver = NSKeyedUnarchiver(forReadingWith: data)
        }
        guard let photoMessage = unarchiver.decodeObject(
            of: DBSecureSerializableAssetGroupLinkage.self,
            forKey: NSKeyedArchiveRootObjectKey
        ) else {
            log.critical("failed to decode photo messages from data")
            throw SHBackgroundOperationError.unexpectedData(data)
        }
        return photoMessage
    }
}

