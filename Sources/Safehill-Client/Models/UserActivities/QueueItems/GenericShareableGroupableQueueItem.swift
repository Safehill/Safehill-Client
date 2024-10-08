import Foundation

public class SHGenericShareableGroupableQueueItem: SHAbstractOutboundShareableGroupableQueueItem, NSSecureCoding {
    
    public static var supportsSecureCoding: Bool = true
    
    enum CodingKeys: String, CodingKey {
        case globalAssetId
    }
    
    public let globalAssetId: String
    
    public init(localAssetId: String,
                globalAssetId: String,
                versions: [SHAssetQuality],
                groupId: String,
                eventOriginator: SHServerUser,
                sharedWith users: [SHServerUser] = [],
                invitedUsers: [String],
                asPhotoMessageInThreadId: String?,
                isBackground: Bool = false) {
        self.globalAssetId = globalAssetId
        super.init(localIdentifier: localAssetId,
                   versions: versions,
                   groupId: groupId,
                   eventOriginator: eventOriginator,
                   sharedWith: users,
                   invitedUsers: invitedUsers,
                   asPhotoMessageInThreadId: asPhotoMessageInThreadId,
                   isBackground: isBackground)
    }
    
    public override func encode(with coder: NSCoder) {
        super.encode(with: coder)
        coder.encode(self.globalAssetId, forKey: CodingKeys.globalAssetId.rawValue)
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        if let superSelf = SHAbstractOutboundShareableGroupableQueueItem(coder: decoder) {
            let globalAssetId = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.globalAssetId.rawValue)
            
            guard let globalAssetId = globalAssetId as? String else {
                log.error("unexpected value for globalAssetId when decoding SHConcreteShareableGroupableQueueItem object")
                return nil
            }
            
            self.init(
                localAssetId: superSelf.localIdentifier,
                globalAssetId: globalAssetId,
                versions: superSelf.versions,
                groupId: superSelf.groupId,
                eventOriginator: superSelf.eventOriginator,
                sharedWith: superSelf.sharedWith,
                invitedUsers: superSelf.invitedUsers,
                asPhotoMessageInThreadId: superSelf.asPhotoMessageInThreadId,
                isBackground: superSelf.isBackground
            )
            return
        }
        
        return nil
    }
}
