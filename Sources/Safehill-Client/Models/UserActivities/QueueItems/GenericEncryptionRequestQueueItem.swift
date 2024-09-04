import Foundation

/// On-disk representation of an upload queue item.
/// References a group id, which is the unique identifier of the request.
public class SHGenericEncryptionRequestQueueItem: SHAbstractOutboundShareableGroupableQueueItem, NSSecureCoding {
    
    enum CodingKeys: String, CodingKey {
        case asset
    }
    
    public static var supportsSecureCoding: Bool = true
    
    public let asset: SHApplePhotoAsset
    
    public init(asset: SHApplePhotoAsset,
                versions: [SHAssetQuality],
                groupId: String,
                eventOriginator: any SHServerUser,
                sharedWith users: [any SHServerUser] = [],
                invitedUsers: [String],
                asPhotoMessageInThreadId: String?,
                isBackground: Bool = false) {
        self.asset = asset
        super.init(localIdentifier: asset.phAsset.localIdentifier,
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
        coder.encode(self.asset, forKey: CodingKeys.asset.rawValue)
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        if let superSelf = SHAbstractOutboundShareableGroupableQueueItem(coder: decoder) {
            let asset = decoder.decodeObject(of: SHApplePhotoAsset.self, forKey: CodingKeys.asset.rawValue)
            
            guard let asset = asset else {
                log.error("unexpected value for asset when decoding SHEncryptionRequestQueueItem object")
                return nil
            }
            
            self.init(asset: asset,
                      versions: superSelf.versions,
                      groupId: superSelf.groupId,
                      eventOriginator: superSelf.eventOriginator,
                      sharedWith: superSelf.sharedWith,
                      invitedUsers: superSelf.invitedUsers,
                      asPhotoMessageInThreadId: superSelf.asPhotoMessageInThreadId,
                      isBackground: superSelf.isBackground)
            return
        }
       
        return nil
    }
}
