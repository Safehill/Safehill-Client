import Foundation
import KnowledgeBase

extension KBKVStore {
    static func initKVStoreWithRetries(dbName name: String,
                                       completionHandler: @escaping (Result<KBKVStore, Error>) -> Void) {
        if let s = KBKVStore.store(withName: name) {
            completionHandler(.success(s))
            return
        }
        
        let circuitBreaker = CircuitBreaker(
            timeout: 5.0,
            maxRetries: 3,
            timeBetweenRetries: 0.5,
            exponentialBackoff: true,
            resetTimeout: 20.0
        )
        
        circuitBreaker.call = { circuitBreaker in
            log.debug("[db] attempting a connection to kvstore \(name)")
            if let store = KBKVStore.store(withName: name) {
                circuitBreaker.success()
                log.debug("[db] successful connection to kvstore \(name)")
                completionHandler(.success(store))
            } else {
                circuitBreaker.failure()
            }
        }
        
        circuitBreaker.didTrip = { circuitBreaker, err in
            let error = KBError.databaseException("connection to kvstore \(name) failed: \(err?.localizedDescription ?? "")")
            log.error("[db] FAILED connection to kvstore \(name)")
            completionHandler(.failure(error))
        }
        
        circuitBreaker.execute()
    }
}

extension KBQueueStore {
    static func initKBQueueStoreWithRetries(dbName name: String,
                                            type: KBQueueStore.QueueType,
                                            completionHandler: @escaping (Result<KBQueueStore, Error>) -> Void) {
        if let q = KBQueueStore.store(withName: name, type: type) {
            completionHandler(.success(q))
            return
        }
        
        let circuitBreaker = CircuitBreaker(
            timeout: 5.0,
            maxRetries: 3,
            timeBetweenRetries: 0.5,
            exponentialBackoff: true,
            resetTimeout: 20.0
        )
        
        circuitBreaker.call = { circuitBreaker in
            log.debug("[db] attempting a connection to queue store \(name)")
            if let q = KBQueueStore.store(withName: name, type: type) {
                circuitBreaker.success()
                log.debug("[db] successful connection to queue store \(name)")
                completionHandler(.success(q))
            } else {
                circuitBreaker.failure()
            }
        }
        
        circuitBreaker.didTrip = { circuitBreaker, err in
            let error = KBError.databaseException("connection to queue store \(name) failed: \(err?.localizedDescription ?? "")")
            log.error("[db] FAILED connection to queue store \(name)")
            completionHandler(.failure(error))
        }
        
        circuitBreaker.execute()
    }
}


public class SHDBManager {
    
    private enum DBName: String {
        case userStore = "com.gf.safehill.LocalServer.users"
        case assetStore = "com.gf.safehill.LocalServer.assets"
        case reactionStore = "com.gf.safehill.LocalServer.reactions"
        case messageQueue = "com.gf.safehill.LocalServer.messages"
        case knowledgeGraph = "com.gf.safehill.KnowledgeGraph"
    }
    
    private static func getKVStore(named name: String) -> KBKVStore? {
        var keyValueStore: KBKVStore? = nil
        let semaphore = DispatchSemaphore(value: 0)
        
        KBKVStore.initKVStoreWithRetries(dbName: name) { result in
            if case .success(let kvStore) = result {
                keyValueStore = kvStore
                semaphore.signal()
            }
        }
        
        let _ = semaphore.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds))
        return keyValueStore
    }
    
    private static func getQueueStore(name: String, type: KBQueueStore.QueueType) -> KBQueueStore? {
        var queueStore: KBQueueStore? = nil
        let semaphore = DispatchSemaphore(value: 0)
        
        KBQueueStore.initKBQueueStoreWithRetries(dbName: name, type: type) { result in
            if case .success(let q) = result {
                queueStore = q
                semaphore.signal()
            }
        }
        
        let _ = semaphore.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds))
        return queueStore
    }
    
    public static func queue(of type: BackgroundOperationQueue.OperationType) -> KBQueueStore? {
        SHDBManager.getQueueStore(name: type.identifier, type: .fifo)
    }
    
    public static var userStore: KBKVStore? = {
        SHDBManager.getKVStore(named: DBName.userStore.rawValue)
    }()
    public static var assetStore: KBKVStore? = {
        SHDBManager.getKVStore(named: DBName.assetStore.rawValue)
    }()
    public static var reactionStore: KBKVStore? = {
        SHDBManager.getKVStore(named: DBName.reactionStore.rawValue)
    }()
    public static var messageQueue: KBQueueStore? = {
        SHDBManager.getQueueStore(name: DBName.messageQueue.rawValue, type: .lifo)
    }()
    public static var graph: KBKnowledgeStore? = {
        if let backingKVStore = SHDBManager.getKVStore(named: DBName.knowledgeGraph.rawValue) {
            return KBKnowledgeStore.store(backingKVStore.location)
        }
        return nil
    }()
}
