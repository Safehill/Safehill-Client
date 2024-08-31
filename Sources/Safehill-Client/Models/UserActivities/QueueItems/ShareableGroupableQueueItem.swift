import Foundation

public protocol SHShareableGroupableQueueItem: SHGroupableQueueItem {
    var localIdentifier: String { get }
    var eventOriginator: any SHServerUser { get }
    var sharedWith: [any SHServerUser] { get }
    var invitedUsers: [String] { get }
    
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

public extension SHShareableGroupableQueueItem {
    var isSharingWithOtherSafehillUsers: Bool {
        return invitedUsers.count > 0
    }
    
    var isSharingWithOrInvitingOtherUsers: Bool {
        return sharedWith.count + invitedUsers.count > 0
    }
    
    var isOnlyInvitingUsers: Bool {
        return sharedWith.count == 0 && invitedUsers.count > 0
    }
}

