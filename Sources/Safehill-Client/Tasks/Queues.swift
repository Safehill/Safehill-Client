import Foundation
import KnowledgeBase

///
/// All queues for background operation
///
public struct BackgroundOperationQueue {
    
    public enum OperationType: CaseIterable {
        case encryption, // encrypting assets
             upload, // uploading encrypted assets to server and CDN
             share, // share assets with other users
             failedUpload, // failed upload history
             failedShare // failed share history
        
        var identifier: String {
            switch self {
            case .encryption:
                return "com.gf.safehill.PhotoAssetEncryptionQueue"
            case .upload:
                return "com.gf.safehill.PhotoAssetUploadQueue"
            case .share:
                return "com.gf.safehill.PhotoAssetShareQueue"
            case .failedUpload:
                return "com.gf.safehill.PhotoAssetFailedUploadQueue"
            case .failedShare:
                return "com.gf.safehill.PhotoAssetFailedShareQueue"
            }
        }
    }
    
    public static func of(type: OperationType) throws -> KBQueueStore {
        guard let queue = SHDBManager.sharedInstance.queue(of: type) else {
            throw KBError.databaseNotReady
        }
        return queue
    }
}

///
/// Queue item reservations
///
enum ProcessingState { case encrypting, uploading, sharing }

private let ProcessingStateUpdateQueue = DispatchQueue(label: "com.gf.safehill.ProcessingStateUpdateQueue", attributes: .concurrent)

private var ItemIdentifiersInProcessByState: [ProcessingState: Set<String>] = [
    .encrypting: Set<String>(),
    .uploading: Set<String>(),
    .sharing: Set<String>(),
]

func items(inState state: ProcessingState) -> Set<String>? {
    var set: Set<String>? = nil
    ProcessingStateUpdateQueue.sync(flags: .barrier) {
        set = ItemIdentifiersInProcessByState[state]
    }
    return set
}

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
