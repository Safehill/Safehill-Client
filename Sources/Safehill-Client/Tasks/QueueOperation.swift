import Foundation
import KnowledgeBase


public enum SHQueueOperation {
    
    public static func queueIdentifier(for localIdentifier: String, groupId: String? = nil) -> String {
        return [localIdentifier, groupId ?? ""].joined(separator: "+")
    }
    
    public static func removeItems(correspondingTo assetLocalIdentifiers: [String], groupId: String? = nil) throws {
        try SHQueueOperation.removeUploadItems(correspondingTo: assetLocalIdentifiers)
        try SHQueueOperation.removeShareItems(correspondingTo: assetLocalIdentifiers, groupId: groupId)
    }
    
    public static func removeUploadItems(correspondingTo assetLocalIdentifiers: [String]) throws {
        guard assetLocalIdentifiers.count > 0 else {
            return
        }
        
        let condition = assetLocalIdentifiers.reduce(KBGenericCondition(value: false), { partialResult, localIdentifier in
            return partialResult.or(KBGenericCondition(.beginsWith, value: SHQueueOperation.queueIdentifier(for: localIdentifier)))
        })
        
        var removed =  try BackgroundOperationQueue.of(type: .fetch).removeValues(forKeysMatching: condition)
        removed += try BackgroundOperationQueue.of(type: .encryption).removeValues(forKeysMatching: condition)
        removed += try BackgroundOperationQueue.of(type: .upload).removeValues(forKeysMatching: condition)
        removed += try BackgroundOperationQueue.of(type: .failedUpload).removeValues(forKeysMatching: condition)
        removed += try BackgroundOperationQueue.of(type: .successfulUpload).removeValues(forKeysMatching: condition)
        
        if removed.count > 0 {
            log.info("removed \(removed.count) related items from the queues")
        }
    }
    
    public static func removeShareItems(correspondingTo assetLocalIdentifiers: [String], groupId: String? = nil) throws {
        guard assetLocalIdentifiers.count > 0 else {
            return
        }
        
        let condition = assetLocalIdentifiers.reduce(KBGenericCondition(value: false), { partialResult, localIdentifier in
            return partialResult.or(KBGenericCondition(.beginsWith, value: SHQueueOperation.queueIdentifier(for: localIdentifier, groupId: groupId)))
        })
        
        var removed = try BackgroundOperationQueue.of(type: .share).removeValues(forKeysMatching: condition)
        removed += try BackgroundOperationQueue.of(type: .failedShare).removeValues(forKeysMatching: condition)
        removed += try BackgroundOperationQueue.of(type: .successfulShare).removeValues(forKeysMatching: condition)
        if removed.count > 0 {
            log.info("removed \(removed.count) related items from the queues")
        }
    }
    
    public static func removeDownloadItems(with assetGlobalIdentifiers: [GlobalIdentifier], for user: SHLocalUser) throws {
        guard assetGlobalIdentifiers.count > 0 else {
            return
        }
        
        try SHAssetDownloadController(user: user).removePendingDownloadsFromQueues(for: assetGlobalIdentifiers)
    }
}
