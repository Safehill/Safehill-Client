import Foundation
import KnowledgeBase

///
/// All queues for background operation
///
public struct BackgroundOperationQueue {
    
    public enum OperationType {
        case fetch, encryption, upload, share, successfulUpload, successfulShare, failedUpload, failedShare, download
        
        var identifier: String {
            switch self {
            case .fetch:
                return "com.gf.safehill.PhotoAssetFetchQueue"
            case .encryption:
                return "com.gf.safehill.PhotoAssetEncryptionQueue"
            case .upload:
                return "com.gf.safehill.PhotoAssetUploadQueue"
            case .share:
                return "com.gf.safehill.PhotoAssetShareQueue"
            case .successfulUpload:
                return "com.gf.safehill.PhotoAssetUploadHistoryQueue"
            case .successfulShare:
                return "com.gf.safehill.PhotoAssetShareHistoryQueue"
            case .failedUpload:
                return "com.gf.safehill.PhotoAssetFailedUploadQueue"
            case .failedShare:
                return "com.gf.safehill.PhotoAssetFailedShareQueue"
            case .download:
                return "com.gf.safehill.PhotoAssetDownloadQueue"
            }
        }
    }
    
    public static func of(type: OperationType) throws -> KBQueueStore {
        if let q = KBQueueStore.store(withName: type.identifier, type: .fifo) {
            return q
        }
        
        return try BackgroundOperationQueue.initWithRetries(type: type)
    }
    
    internal static func initWithRetries(type: OperationType) throws -> KBQueueStore {
        var queue: KBQueueStore? = nil
        let semaphore = DispatchSemaphore(value: 0)
        var timer: Timer? = nil
        
        DispatchQueue.global(qos: .userInitiated).async {
            timer = Timer.scheduledTimer(withTimeInterval: 1, repeats: true, block: { _ in
                if let q = KBQueueStore.store(withName: type.identifier, type: .fifo) {
                    queue = q
                    semaphore.signal()
                    timer?.invalidate()
                    timer = nil
                }
            })
            timer!.fire()
        }
        
        let dispatchResult = semaphore.wait(timeout: .now() + .seconds(5))
        guard dispatchResult == .success else {
            throw KBError.databaseException("Could not connect to queue database. The application will force quit now")
        }
        
        return queue!
    }
}

///
/// Queue item reservations
///
enum ProcessingState { case fetching, encrypting, uploading, sharing }

private let ProcessingStateUpdateQueue = DispatchQueue(label: "com.gf.safehill.ProcessingStateUpdateQueue", attributes: .concurrent)

private var ItemIdentifiersInProcessByState: [ProcessingState: Set<String>] = [
    .fetching: Set<String>(),
    .encrypting: Set<String>(),
    .uploading: Set<String>(),
    .sharing: Set<String>(),
]

func processingState(for assetIdentifier: String) -> ProcessingState? {
    var state: ProcessingState? = nil
    ProcessingStateUpdateQueue.sync(flags: .barrier) {
        for everyState in ItemIdentifiersInProcessByState.keys {
            if ItemIdentifiersInProcessByState[everyState]!.contains(assetIdentifier) {
                state = everyState
                break
            }
        }
    }
    return state
}
func setProcessingState(_ state: ProcessingState?, for assetIdentifier: String) {
    ProcessingStateUpdateQueue.sync(flags: .barrier) {
        for everyState in ItemIdentifiersInProcessByState.keys {
            if let state = state {
                if everyState == state {
                    ItemIdentifiersInProcessByState[state]!.insert(assetIdentifier)
                } else {
                    ItemIdentifiersInProcessByState[everyState]!.remove(assetIdentifier)
                }
            } else {
                ItemIdentifiersInProcessByState[everyState]!.remove(assetIdentifier)
            }
        }
    }
}
