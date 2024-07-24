import Foundation

public protocol SHGroupableQueueItem: SHSerializableQueueItem {
    var groupId: String { get }
}

