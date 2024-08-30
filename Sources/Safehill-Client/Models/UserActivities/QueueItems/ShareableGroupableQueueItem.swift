import Foundation

public protocol SHShareableGroupableQueueItem: SHGroupableQueueItem {
    var localIdentifier: String { get }
    var eventOriginator: any SHServerUser { get }
    var sharedWith: [any SHServerUser] { get }
    var invitedUsers: [String] { get }
    
    /// Helper to determine if the sender (eventOriginator)
    /// is sharing it with recipients other than self
    var isSharingWithOtherUsers: Bool { get }
}

public extension SHShareableGroupableQueueItem {
    var isSharingWithOtherUsers: Bool {
        return sharedWith.count > 0
    }
}

