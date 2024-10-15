import Foundation
import KnowledgeBase


public enum SHQueueOperation {
    
    public static func queueIdentifier(
        for globalIdentifier: GlobalIdentifier,
        groupId: String? = nil
    ) -> String {
        return [globalIdentifier, groupId ?? ""].joined(separator: "+")
    }
    
    public static func removeItems(
        correspondingTo globalIdentifiers: [GlobalIdentifier], groupId: String? = nil
    ) throws {
        try SHQueueOperation.removeUploadItems(correspondingTo: globalIdentifiers)
        try SHQueueOperation.removeShareItems(correspondingTo: globalIdentifiers, groupId: groupId)
    }
    
    public static func removeUploadItems(
        correspondingTo globalIdentifiers: [GlobalIdentifier]
    ) throws {
        guard globalIdentifiers.isEmpty == false else {
            return
        }
        
        let condition = globalIdentifiers.reduce(
            KBGenericCondition(value: false),
            { partialResult, globalId in
                return partialResult.or(
                    KBGenericCondition(
                        .beginsWith,
                        value: SHQueueOperation.queueIdentifier(for: globalId)
                    )
                )
            }
        )
        
        var removed = try BackgroundOperationQueue.of(type: .encryption).removeValues(forKeysMatching: condition)
        removed += try BackgroundOperationQueue.of(type: .upload).removeValues(forKeysMatching: condition)
        removed += try BackgroundOperationQueue.of(type: .failedUpload).removeValues(forKeysMatching: condition)
        
        if removed.count > 0 {
            log.info("removed \(removed.count) related items from the queues")
        }
    }
    
    public static func removeShareItems(
        correspondingTo globalIdentifiers: [GlobalIdentifier],
        groupId: String? = nil
    ) throws {
        guard globalIdentifiers.isEmpty == false else {
            return
        }
        
        let condition = globalIdentifiers.reduce(
            KBGenericCondition(value: false),
            { partialResult, globalId in
                return partialResult.or(
                    KBGenericCondition(
                        .beginsWith,
                        value: SHQueueOperation.queueIdentifier(for: globalId, groupId: groupId)
                    )
                )
            }
        )
        
        var removed = try BackgroundOperationQueue.of(type: .share).removeValues(forKeysMatching: condition)
        removed += try BackgroundOperationQueue.of(type: .failedShare).removeValues(forKeysMatching: condition)
        if removed.count > 0 {
            log.info("removed \(removed.count) related items from the queues")
        }
    }
}
