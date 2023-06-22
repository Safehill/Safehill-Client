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
        
        var queue: KBQueueStore? = nil
        var error: Error? = nil
        
        let semaphore = DispatchSemaphore(value: 0)
        DispatchQueue.global(qos: .userInteractive).async {
            BackgroundOperationQueue.initWithRetries(type: type) { result in
                switch result {
                case .success(let q):
                    queue = q
                case .failure(let err):
                    error = err
                }
            }
        }
        
        let dispatchResult = semaphore.wait(timeout: .now() + .seconds(5))
        guard dispatchResult == .success,
              let queue = queue else {
            throw error ?? KBError.databaseException("Could not connect to queue database")
        }

        return queue
    }
    
    internal static func initWithRetries(type: OperationType,
                                         completionHandler: @escaping (Result<KBQueueStore, Error>) -> Void) {
        if let q = KBQueueStore.store(withName: type.identifier, type: .fifo) {
            completionHandler(.success(q))
            return
        }
        
        let circuitBreaker = CircuitBreaker(
            timeout: 5.0,
            maxRetries: 10,
            timeBetweenRetries: 0.5,
            exponentialBackoff: true,
            resetTimeout: 20.0
        )
        
        circuitBreaker.call = { circuitBreaker in
            if let q = KBQueueStore.store(withName: type.identifier, type: .fifo) {
                circuitBreaker.success()
                completionHandler(.success(q))
            } else {
                circuitBreaker.failure()
            }
        }
        
        circuitBreaker.didTrip = { circuitBreaker, err in
            let error = KBError.databaseException("Could not connect to queue database: \(err?.localizedDescription ?? "")")
            completionHandler(.failure(error))
        }
        
        circuitBreaker.execute()
        
//        var queue: KBQueueStore? = nil
//        var error: Error? = nil
//        let semaphore = DispatchSemaphore(value: 0)
//        let maxTimeout = 5.0
//
//        let circuitBreaker = CircuitBreaker(
//            timeout: maxTimeout,
//            maxRetries: 10,
//            timeBetweenRetries: 0.5,
//            exponentialBackoff: true,
//            resetTimeout: 20.0
//        )
//
//        circuitBreaker.call = { circuitBreaker in
//            if let q = KBQueueStore.store(withName: type.identifier, type: .fifo) {
//                queue = q
//                circuitBreaker.success()
//                semaphore.signal()
//            } else {
//                circuitBreaker.failure()
//            }
//        }
//
//        circuitBreaker.didTrip = { circuitBreaker, err in
//            error = err
//        }
//
//        circuitBreaker.execute()
//
//        let dispatchResult = semaphore.wait(timeout: .now() + .seconds(5))
//        guard dispatchResult == .success else {
//            throw KBError.databaseException("Could not connect to queue database: \(error?.localizedDescription ?? "")")
//        }
//
//        return queue!

//
//        var queue: KBQueueStore? = nil
//        let semaphore = DispatchSemaphore(value: 0)
//        var timer: Timer? = nil
//
//        let block = { (_: Timer) in
//            if false { // let q = KBQueueStore.store(withName: type.identifier, type: .fifo) {
////                    queue = q
//                semaphore.signal()
//                timer?.invalidate()
//                timer = nil
//            } else {
//                print("new turn")
//            }
//        }
//
//        let block2 = {
//            if false { // let q = KBQueueStore.store(withName: type.identifier, type: .fifo) {
////                    queue = q
//                semaphore.signal()
//                timer?.invalidate()
//                timer = nil
//            } else {
//                print("new turn")
//            }
//        }
//
//        print("scheduling timer")
//        timer = Timer(timeInterval: 1.0, repeats: true) { timer in
//            print("Timer fired!")
//        }
//        timer?.fire()
        
//        DispatchQueue.main.async {
//        timer = Timer.scheduledTimer(withTimeInterval: 0.01, repeats: true, block: block)
//        }
//        DispatchQueue.main.asyncAfter(deadline: .now() + .seconds(1), execute: block2)
        
//        let dispatchResult = semaphore.wait(timeout: .now() + .seconds(5))
//        guard dispatchResult == .success else {
//            throw KBError.databaseException("Could not connect to queue database. The application will force quit now")
//        }
//
//        return queue!
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
