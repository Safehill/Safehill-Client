import Foundation
import KnowledgeBase

internal class DBSecureSerializableAssetCollectionInfo: NSObject, NSSecureCoding {

    public static var supportsSecureCoding: Bool = true

    let collectionId: String
    let collectionName: String
    let visibility: String
    let accessType: String
    let addedAt: String

    enum CodingKeys: String, CodingKey {
        case collectionId
        case collectionName
        case visibility
        case accessType
        case addedAt
    }

    init(
        collectionId: String,
        collectionName: String,
        visibility: String,
        accessType: String,
        addedAt: String
    ) {
        self.collectionId = collectionId
        self.collectionName = collectionName
        self.visibility = visibility
        self.accessType = accessType
        self.addedAt = addedAt
    }

    required convenience init?(coder decoder: NSCoder) {
        let collectionId = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.collectionId.rawValue) as? String
        let collectionName = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.collectionName.rawValue) as? String
        let visibility = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.visibility.rawValue) as? String
        let accessType = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.accessType.rawValue) as? String
        let addedAt = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.addedAt.rawValue) as? String

        guard let collectionId = collectionId else {
            log.error("unexpected value for collectionId when decoding DBSecureSerializableAssetCollectionInfo object")
            return nil
        }

        guard let collectionName = collectionName else {
            log.error("unexpected value for collectionName when decoding DBSecureSerializableAssetCollectionInfo object")
            return nil
        }

        guard let visibility = visibility else {
            log.error("unexpected value for visibility when decoding DBSecureSerializableAssetCollectionInfo object")
            return nil
        }

        guard let accessType = accessType else {
            log.error("unexpected value for accessType when decoding DBSecureSerializableAssetCollectionInfo object")
            return nil
        }

        guard let addedAt = addedAt else {
            log.error("unexpected value for addedAt when decoding DBSecureSerializableAssetCollectionInfo object")
            return nil
        }

        self.init(
            collectionId: collectionId,
            collectionName: collectionName,
            visibility: visibility,
            accessType: accessType,
            addedAt: addedAt
        )
    }

    func encode(with coder: NSCoder) {
        coder.encode(collectionId, forKey: CodingKeys.collectionId.rawValue)
        coder.encode(collectionName, forKey: CodingKeys.collectionName.rawValue)
        coder.encode(visibility, forKey: CodingKeys.visibility.rawValue)
        coder.encode(accessType, forKey: CodingKeys.accessType.rawValue)
        coder.encode(addedAt, forKey: CodingKeys.addedAt.rawValue)
    }

    static func from(_ any: Any) throws -> DBSecureSerializableAssetCollectionInfo {
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
            of: DBSecureSerializableAssetCollectionInfo.self,
            forKey: NSKeyedArchiveRootObjectKey
        ) else {
            throw SHBackgroundOperationError.unexpectedData(serialized)
        }

        return result
    }
}
