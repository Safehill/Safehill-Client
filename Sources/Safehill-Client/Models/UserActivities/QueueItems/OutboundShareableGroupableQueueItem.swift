import Foundation

public protocol SHOutboundShareableGroupableQueueItem: SHShareableGroupableQueueItem {
    var isPhotoMessage: Bool { get }
}
