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
    
    public static let sharedInstance = SHDBManager()
    
    private enum DBName: String {
        case userStore = "com.gf.safehill.LocalServer.users"
        case assetStore = "com.gf.safehill.LocalServer.assets"
        case reactionStore = "com.gf.safehill.LocalServer.reactions"
        case messageQueue = "com.gf.safehill.LocalServer.messages"
        case knowledgeGraph = "com.gf.safehill.KnowledgeGraph"
    }
    
    private var _userStore: KBKVStore?
    private var _assetStore: KBKVStore?
    private var _reactionStore: KBKVStore?
    private var _messageQueue: KBQueueStore?
    private var _knowledgeGraph: KBKnowledgeStore?
    private var _backgroundQueues: [BackgroundOperationQueue.OperationType: KBQueueStore?]
    
    private let backgroundQueuesAccessQueue = DispatchQueue(
        label: "com.gf.safehill.SHDBManager.backgroundQueue.access",
        attributes: .concurrent
    )
    
    init() {
        self._backgroundQueues = [:]
        self.disconnect()
    }
    
    deinit {
        self.disconnect()
    }
    
    public func disconnect() {
        self._userStore = nil
        self._assetStore = nil
        self._reactionStore = nil
        self._messageQueue = nil
        self._knowledgeGraph = nil
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
    
    public func queue(of type: BackgroundOperationQueue.OperationType) -> KBQueueStore? {
        let queue: KBQueueStore? = nil
        backgroundQueuesAccessQueue.sync(flags: .barrier) {
            if self._backgroundQueues[type] == nil {
                self._backgroundQueues[type] = SHDBManager.getQueueStore(name: type.identifier, type: .fifo)
            }
            queue = self._backgroundQueues[type]!
        }
        return queue
    }
    
    public var userStore: KBKVStore? {
        if self._userStore == nil {
            self._userStore = SHDBManager.getKVStore(named: DBName.userStore.rawValue)
        }
        return self._userStore
    }
    public var assetStore: KBKVStore? {
        if self._assetStore == nil {
            self._assetStore = SHDBManager.getKVStore(named: DBName.assetStore.rawValue)
        }
        return self._assetStore
    }
    public var reactionStore: KBKVStore? {
        if self._reactionStore == nil {
            self._reactionStore = SHDBManager.getKVStore(named: DBName.reactionStore.rawValue)
        }
        return self._reactionStore
    }
    public var messageQueue: KBQueueStore? {
        if self._messageQueue == nil {
            self._messageQueue = SHDBManager.getQueueStore(name: DBName.messageQueue.rawValue, type: .lifo)
        }
        return self._messageQueue
    }
    public var graph: KBKnowledgeStore? {
        if self._knowledgeGraph == nil,
           let backingKVStore = SHDBManager.getKVStore(named: DBName.knowledgeGraph.rawValue) {
            self._knowledgeGraph = KBKnowledgeStore.store(backingKVStore.location)
        }
        return self._knowledgeGraph
    }
}
