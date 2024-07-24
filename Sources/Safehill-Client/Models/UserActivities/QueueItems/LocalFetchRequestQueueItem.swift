import Foundation

public class SHLocalFetchRequestQueueItem: SHAbstractOutboundShareableGroupableQueueItem, NSSecureCoding {
    
    enum CodingKeys: String, CodingKey {
        case globalIdentifier = "globalAssetId"
        case shouldUpload
    }
    
    public static var supportsSecureCoding: Bool = true
    
    public let globalIdentifier: String?
    
    public let shouldUpload: Bool
    
    public init(localIdentifier: String,
                globalIdentifier: String? = nil,
                groupId: String,
                eventOriginator: SHServerUser,
                sharedWith users: [SHServerUser],
                shouldUpload: Bool,
                isPhotoMessage: Bool,
                isBackground: Bool = false) {
        self.globalIdentifier = globalIdentifier
        self.shouldUpload = shouldUpload
        super.init(localIdentifier: localIdentifier,
                   groupId: groupId,
                   eventOriginator: eventOriginator,
                   sharedWith: users,
                   isPhotoMessage: isPhotoMessage,
                   isBackground: isBackground)
    }
    
    public init(localIdentifier: String,
                globalIdentifier: String? = nil,
                versions: [SHAssetQuality],
                groupId: String,
                eventOriginator: SHServerUser,
                sharedWith users: [SHServerUser],
                shouldUpload: Bool,
                isPhotoMessage: Bool,
                isBackground: Bool = false) {
        self.globalIdentifier = globalIdentifier
        self.shouldUpload = shouldUpload
        super.init(localIdentifier: localIdentifier,
                   versions: versions,
                   groupId: groupId,
                   eventOriginator: eventOriginator,
                   sharedWith: users,
                   isPhotoMessage: isPhotoMessage,
                   isBackground: isBackground)
    }
    
    public override func encode(with coder: NSCoder) {
        super.encode(with: coder)
        coder.encode(self.globalIdentifier, forKey: CodingKeys.globalIdentifier.rawValue)
        coder.encode(NSNumber(booleanLiteral: self.shouldUpload), forKey: CodingKeys.shouldUpload.rawValue)
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        if let superSelf = SHAbstractOutboundShareableGroupableQueueItem(coder: decoder) {
            let globalAssetId = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.globalIdentifier.rawValue)
            let shouldUpload = decoder.decodeObject(of: NSNumber.self, forKey: CodingKeys.shouldUpload.rawValue)
            
            guard let su = shouldUpload else {
                log.error("unexpected value for shouldUpload when decoding SHLocalFetchRequestQueueItem object")
                return nil
            }
            
            self.init(localIdentifier: superSelf.localIdentifier,
                      globalIdentifier: globalAssetId as? String,
                      versions: superSelf.versions,
                      groupId: superSelf.groupId,
                      eventOriginator: superSelf.eventOriginator,
                      sharedWith: superSelf.sharedWith,
                      shouldUpload: su.boolValue,
                      isPhotoMessage: superSelf.isPhotoMessage,
                      isBackground: superSelf.isBackground)
            return
        }
       
        return nil
    }
}
