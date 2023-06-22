import Foundation
import KnowledgeBase

extension KBKVStore {
    static func initDBHandlerWithRetries(dbName name: String,
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


class SHDBManager {
    public static var sharedInstance = SHDBManager()
    
    var _userStore: KBKVStore? = nil
    var _assetStore: KBKVStore? = nil
    
    var userStore: KBKVStore {
        if let s = _userStore {
            return s
        } else {
            fatalError("user store handler could not be initialized")
        }
    }
    var assetStore: KBKVStore {
        if let s = _assetStore {
            return s
        } else {
            fatalError("asset store handler could not be initialized")
        }
    }
    
    private var _graph: KBKnowledgeStore? = nil
    
    public var graph: KBKnowledgeStore {
        if let g = _graph {
            return g
        } else {
            fatalError("knowledge graph database handler could not be initialized")
        }
    }
    
    init() {
        if let s = KBKnowledgeStore.store(withName: "com.gf.safehill.LocalServer.users") {
            self._userStore = s
        } else {
            DispatchQueue.global(qos: .userInteractive).async { [self] in
                KBKVStore.initDBHandlerWithRetries(dbName: "com.gf.safehill.LocalServer.users") { result in
                    if case .success(let kvStore) = result {
                        self._userStore = kvStore
                    }
                }
            }
        }
        
        if let s = KBKnowledgeStore.store(withName: "com.gf.safehill.LocalServer.assets") {
            self._assetStore = s
        } else {
            DispatchQueue.global(qos: .userInteractive).async { [self] in
                KBKVStore.initDBHandlerWithRetries(dbName: "com.gf.safehill.LocalServer.assets") { result in
                    if case .success(let kvStore) = result {
                        self._assetStore = kvStore
                    }
                }
            }
        }
        
        if let s = KBKnowledgeStore.store(withName: "com.gf.safehill.KnowledgeGraph") {
            self._graph = s
        } else {
            DispatchQueue.global(qos: .userInteractive).async { [self] in
                KBKVStore.initDBHandlerWithRetries(dbName: "com.gf.safehill.KnowledgeGraph") { result in
                    if case .success(let kvStore) = result {
                        /// Since we could initialize a handler to connect to the DB on this location
                        /// it should be safe to force initialize the KBKnowledgeStore at this point
                        self._graph = KBKnowledgeStore.store(kvStore.location)!
                    }
                }
            }
        }
    }
}
