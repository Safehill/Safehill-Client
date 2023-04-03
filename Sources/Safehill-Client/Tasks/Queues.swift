import Foundation
import KnowledgeBase

///
/// All queues for background operation
///

public let FetchQueue = KBQueueStore.store(withName: "com.gf.safehill.PhotoAssetFetchQueue", type: .fifo)

public let EncryptionQueue = KBQueueStore.store(withName: "com.gf.safehill.PhotoAssetEncryptionQueue", type: .fifo)

public let UploadQueue = KBQueueStore.store(withName: "com.gf.safehill.PhotoAssetUploadQueue", type: .fifo)

public let ShareQueue = KBQueueStore.store(withName: "com.gf.safehill.PhotoAssetShareQueue", type: .fifo)

public let UploadHistoryQueue = KBQueueStore.store(withName: "com.gf.safehill.PhotoAssetUploadHistoryQueue", type: .fifo)

public let ShareHistoryQueue = KBQueueStore.store(withName: "com.gf.safehill.PhotoAssetShareHistoryQueue", type: .fifo)

public let FailedUploadQueue = KBQueueStore.store(withName: "com.gf.safehill.PhotoAssetFailedUploadQueue", type: .fifo)

public let FailedShareQueue = KBQueueStore.store(withName: "com.gf.safehill.PhotoAssetFailedShareQueue", type: .fifo)

public let DownloadQueue = KBQueueStore.store(withName: "com.gf.safehill.PhotoAssetDownloadQueue", type: .fifo)

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

///
/// Asset <=> User knowledge graph
///

public let KnowledgeGraph = KBKnowledgeStore.store(withName: "com.gf.safehill.KnowledgeGraph")

public enum KGPredicates: String {
    case shares = "shares"
    case sharedWith = "sharedWith"
}
