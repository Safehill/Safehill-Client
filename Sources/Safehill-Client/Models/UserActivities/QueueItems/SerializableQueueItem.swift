import Foundation
import KnowledgeBase

public protocol SHSerializableQueueItem: NSCoding {
    func enqueue(in queue: KBQueueStore, with identifier: String) throws
}

extension SHSerializableQueueItem {
    public func enqueue(in queue: KBQueueStore, with identifier: String) throws {
        let data = try? NSKeyedArchiver.archivedData(withRootObject: self, requiringSecureCoding: true)
        if let data = data {
            try queue.enqueue(data, withIdentifier: identifier)
        } else {
            throw SHBackgroundOperationError.fatalError("failed to enqueue item with id \(identifier)")
        }
    }
    
    internal func insert(in queue: KBQueueStore, with identifier: String, at date: Date) throws {
        let data = try? NSKeyedArchiver.archivedData(withRootObject: self, requiringSecureCoding: true)
        if let data = data {
            try queue.insert(data, withIdentifier: identifier, timestamp: date)
        } else {
            throw SHBackgroundOperationError.fatalError("failed to insert item with id \(identifier)")
        }
    }
}
