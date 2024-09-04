import Foundation

public class SHFailedQueueItem: SHAbstractOutboundShareableGroupableQueueItem, NSSecureCoding {
    public static var supportsSecureCoding: Bool = true
    
    public required convenience init?(coder decoder: NSCoder) {
        if let superSelf = SHAbstractOutboundShareableGroupableQueueItem(coder: decoder) {
            self.init(localIdentifier: superSelf.localIdentifier,
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
