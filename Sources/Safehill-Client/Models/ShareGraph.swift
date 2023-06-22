import KnowledgeBase


public enum SHKGPredicates: String {
    case shares = "shares"
    case sharedWith = "sharedWith"
}

public struct SHShareGraph {
    public let store: KBKnowledgeStore
    
    init() {
        do {
            let kvStore = try KBKVStore.initDBHandlerWithRetries(dbName: "com.gf.safehill.KnowledgeGraph")
            /// Since we could initialize a handler to connect to the DB on this location
            /// it should be safe to force initialize the KBKnowledgeStore at this point
            self.store = KBKnowledgeStore.store(kvStore.location)!
        } catch {
            fatalError("knowledge graph database handler could not be initialized")
        }
    }
}

public let SHDefaultShareGraph = SHShareGraph()
