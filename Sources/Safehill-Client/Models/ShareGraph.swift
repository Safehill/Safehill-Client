import KnowledgeBase


public enum SHKGPredicates: String {
    case shares = "shares"
    case sharedWith = "sharedWith"
}

public struct SHShareGraph {
    public let store: KBKnowledgeStore
    
    init() {
        do {
            self.store = try KBKnowledgeStore.initDBHandlerWithRetries(dbName: "com.gf.safehill.KnowledgeGraph") as! KBKnowledgeStore
        } catch {
            fatalError("knowledge graph database handler could not be initialized")
        }
    }
}

let shareGraph = SHShareGraph()
