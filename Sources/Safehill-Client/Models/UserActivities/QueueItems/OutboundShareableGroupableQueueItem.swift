import Foundation

public protocol SHOutboundShareableGroupableQueueItem: SHShareableGroupableQueueItem {
    var asPhotoMessageInThreadId: String? { get }
}
