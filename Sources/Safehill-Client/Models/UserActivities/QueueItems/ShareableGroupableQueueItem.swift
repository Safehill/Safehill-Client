import Foundation

public protocol SHShareableGroupableQueueItem: SHGroupableQueueItem {
    /// Helper to determine if the sender (eventOriginator)
    /// is sharing it with recipients other than self
    var isSharingWithOtherSafehillUsers: Bool { get }
    
    /// Helper to determine if the sender (eventOriginator)
    /// is sharing it with recipients other than self or inviting users via phone number
    var isSharingWithOrInvitingOtherUsers: Bool { get }
    
    /// Helper to determine if the sender (eventOriginator)
    /// is not sharing sharing it with Safehill users but only inviting users via phone number
    var isOnlyInvitingUsers: Bool { get }
}
