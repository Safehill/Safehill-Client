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


public class SHDBManager {
    
    private enum DBName: String {
        case userStore = "com.gf.safehill.LocalServer.users"
        case assetsStore = "com.gf.safehill.LocalServer.assets"
        case knowledgeGraph = "com.gf.safehill.KnowledgeGraph"
    }
    
    public static var sharedInstance = SHDBManager()
    
    private var _userStore: KBKVStore? = nil
    private var _assetStore: KBKVStore? = nil
    
    public var userStore: KBKVStore {
        if let s = _userStore {
            return s
        } else {
            fatalError("\(DBName.userStore.rawValue) handler could not be initialized")
        }
    }
    public var assetStore: KBKVStore {
        if let s = _assetStore {
            return s
        } else {
            fatalError("\(DBName.assetsStore.rawValue) handler could not be initialized")
        }
    }
    
    private var _graph: KBKnowledgeStore? = nil
    
    public var graph: KBKnowledgeStore {
        if let g = _graph {
            return g
        } else {
            fatalError("\(DBName.assetsStore.rawValue) handler could not be initialized")
        }
    }
    
    private init() {
        if let s = KBKnowledgeStore.store(withName: DBName.userStore.rawValue) {
            self._userStore = s
        } else {
            DispatchQueue.global(qos: .userInteractive).async { [self] in
                KBKVStore.initDBHandlerWithRetries(dbName: DBName.userStore.rawValue) { result in
                    if case .success(let kvStore) = result {
                        self._userStore = kvStore
                    }
                }
            }
        }
        
        if let s = KBKnowledgeStore.store(withName: DBName.assetsStore.rawValue) {
            self._assetStore = s
        } else {
            DispatchQueue.global(qos: .userInteractive).async { [self] in
                KBKVStore.initDBHandlerWithRetries(dbName: DBName.assetsStore.rawValue) { result in
                    if case .success(let kvStore) = result {
                        self._assetStore = kvStore
                    }
                }
            }
        }
        
        if let s = KBKnowledgeStore.store(withName: DBName.knowledgeGraph.rawValue) {
            self._graph = s
        } else {
            DispatchQueue.global(qos: .userInteractive).async { [self] in
                KBKVStore.initDBHandlerWithRetries(dbName: DBName.knowledgeGraph.rawValue) { result in
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
