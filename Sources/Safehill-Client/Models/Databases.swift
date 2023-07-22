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
            maxRetries: 10,
            timeBetweenRetries: 0.5,
            exponentialBackoff: true,
            resetTimeout: 20.0
        )
        
        circuitBreaker.call = { circuitBreaker in
            if let store = KBKVStore.store(withName: name) {
                circuitBreaker.success()
                completionHandler(.success(store))
            } else {
                circuitBreaker.failure()
            }
        }
        
        circuitBreaker.didTrip = { circuitBreaker, err in
            let error = KBError.databaseException("Could not connect to queue database: \(err?.localizedDescription ?? "")")
            completionHandler(.failure(error))
        }
        
        circuitBreaker.execute()
    }
}

extension BackgroundOperationQueue {
    static func initWithRetries(type: OperationType,
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
    }
}

public class SHDBManager {
    
    private enum DBName: String {
        case userStore = "com.gf.safehill.LocalServer.users"
        case assetsStore = "com.gf.safehill.LocalServer.assets"
        case knowledgeGraph = "com.gf.safehill.KnowledgeGraph"
    }
    
    public static var sharedInstance = SHDBManager()
    
    private var _userStore: KBKVStore? = nil
    private var _assetStore: KBKVStore? = nil
    private var _queues: [BackgroundOperationQueue.OperationType: KBQueueStore] = [:]
    
    public func queue(of type: BackgroundOperationQueue.OperationType) throws -> KBQueueStore {
        if let q = _queues[type] {
            return q
        } else {
            throw KBError.databaseException("handler for queue \(type.identifier) could not be initialized")
        }
    }
    
    public func userStore() throws -> KBKVStore {
        if let s = _userStore {
            return s
        } else {
            throw KBError.databaseException("\(DBName.userStore.rawValue) handler could not be initialized")
        }
    }
    public func assetStore() throws -> KBKVStore {
        if let s = _assetStore {
            return s
        } else {
            throw KBError.databaseException("\(DBName.assetsStore.rawValue) handler could not be initialized")
        }
    }
    
    private var _graph: KBKnowledgeStore? = nil
    
    public func graph() throws -> KBKnowledgeStore {
        if let g = _graph {
            return g
        } else {
            throw KBError.databaseException("\(DBName.assetsStore.rawValue) handler could not be initialized")
        }
    }
    
    private init() {
        // Initialize user store
        if let s = KBKnowledgeStore.store(withName: DBName.userStore.rawValue) {
            self._userStore = s
        } else {
            DispatchQueue.global(qos: .userInteractive).async { [self] in
                KBKVStore.initKVStoreWithRetries(dbName: DBName.userStore.rawValue) { result in
                    if case .success(let kvStore) = result {
                        self._userStore = kvStore
                    }
                }
            }
        }
        
        // Initialize asset store
        if let s = KBKnowledgeStore.store(withName: DBName.assetsStore.rawValue) {
            self._assetStore = s
        } else {
            DispatchQueue.global(qos: .userInteractive).async { [self] in
                KBKVStore.initKVStoreWithRetries(dbName: DBName.assetsStore.rawValue) { result in
                    if case .success(let kvStore) = result {
                        self._assetStore = kvStore
                    }
                }
            }
        }
        
        // Initialize knowledge graph
        if let s = KBKnowledgeStore.store(withName: DBName.knowledgeGraph.rawValue) {
            self._graph = s
        } else {
            DispatchQueue.global(qos: .userInteractive).async { [self] in
                KBKVStore.initKVStoreWithRetries(dbName: DBName.knowledgeGraph.rawValue) { result in
                    if case .success(let kvStore) = result {
                        /// Since we could initialize a handler to connect to the DB on this location
                        /// it should be safe to force initialize the KBKnowledgeStore at this point
                        self._graph = KBKnowledgeStore.store(kvStore.location)!
                    }
                }
            }
        }
        
        // Initialize background operation queues
        for type in BackgroundOperationQueue.OperationType.allCases {
            if let q = KBQueueStore.store(withName: type.identifier, type: .fifo) {
                self._queues[type] = q
            } else {
                DispatchQueue.global(qos: .userInteractive).async {
                    BackgroundOperationQueue.initWithRetries(type: type) { result in
                        if case .success(let q) = result {
                            self._queues[type] = q
                        }
                    }
                }
            }
        }
    }
}