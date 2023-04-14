import Foundation
import KnowledgeBase


public enum SHQueueOperation {
    
    static func removeItems(correspondingTo assetLocalIdentifiers: [String], groupId: String? = nil) {
        SHQueueOperation.removeUploadItems(correspondingTo: assetLocalIdentifiers)
        SHQueueOperation.removeShareItems(correspondingTo: assetLocalIdentifiers, groupId: groupId)
    }
    
    static func removeUploadItems(correspondingTo assetLocalIdentifiers: [String]) {
        guard assetLocalIdentifiers.count > 0 else {
            return
        }
        
        let condition = assetLocalIdentifiers.reduce(KBGenericCondition(value: false), { partialResult, localIdentifier in
            return partialResult.or(KBGenericCondition(.beginsWith, value: [localIdentifier, ""].joined(separator: "+")))
        })
        
        do {
            var removed =  try FetchQueue.removeValues(forKeysMatching: condition)
            removed += try EncryptionQueue.removeValues(forKeysMatching: condition)
            removed += try UploadQueue.removeValues(forKeysMatching: condition)
            removed += try FailedUploadQueue.removeValues(forKeysMatching: condition)
            removed += try UploadHistoryQueue.removeValues(forKeysMatching: condition)
            
            if removed.count > 0 {
                log.info("removed \(removed.count) related items from the queues")
            }
        } catch {
            log.error("failed to remove related items from the queues")
        }
    }
    
    static func removeShareItems(correspondingTo assetLocalIdentifiers: [String], groupId: String? = nil) {
        guard assetLocalIdentifiers.count > 0 else {
            return
        }
        
        let condition = assetLocalIdentifiers.reduce(KBGenericCondition(value: false), { partialResult, localIdentifier in
            return partialResult.or(KBGenericCondition(.beginsWith, value: [localIdentifier, groupId ?? ""].joined(separator: "+")))
        })
        
        do {
            var removed = try ShareQueue.removeValues(forKeysMatching: condition)
            removed += try FailedShareQueue.removeValues(forKeysMatching: condition)
            removed += try ShareHistoryQueue.removeValues(forKeysMatching: condition)
            if removed.count > 0 {
                log.info("removed \(removed.count) related items from the queues")
            }
        } catch {
            log.critical("failed to remove related items from the queues")
        }
    }
}
