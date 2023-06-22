import KnowledgeBase
import Foundation

public enum SHKGPredicates: String {
    case shares = "shares"
    case sharedWith = "sharedWith"
}

public class SHShareGraph {
    
    public static let sharedInstance = SHShareGraph()
    
    private var _store: KBKnowledgeStore? = nil
    
    public var store: KBKnowledgeStore {
        if let s = _store {
            return s
        } else {
            fatalError("knowledge graph database handler could not be initialized")
        }
    }
    
    init() {
        DispatchQueue.global(qos: .userInteractive).async { [self] in
            KBKVStore.initDBHandlerWithRetries(dbName: "com.gf.safehill.KnowledgeGraph") { result in
                if case .success(let kvStore) = result {
                    /// Since we could initialize a handler to connect to the DB on this location
                    /// it should be safe to force initialize the KBKnowledgeStore at this point
                    self._store = KBKnowledgeStore.store(kvStore.location)!
                }
                
            }
        }
    }
}
