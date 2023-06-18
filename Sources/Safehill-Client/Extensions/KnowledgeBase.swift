import Foundation
import KnowledgeBase

extension KBKVStore {
    static func initDBHandlerWithRetries(dbName name: String) throws -> KBKVStore {
        if let s = KBKVStore.store(withName: name) {
            return s
        }
        
        var store: KBKVStore? = nil
        let semaphore = DispatchSemaphore(value: 0)
        var timer: Timer? = nil
        
        DispatchQueue.global(qos: .userInitiated).async {
            timer = Timer.scheduledTimer(withTimeInterval: 1, repeats: true, block: { _ in
                if let s = KBKVStore.store(withName: name) {
                    store = s
                    semaphore.signal()
                    timer?.invalidate()
                    timer = nil
                }
            })
            timer!.fire()
        }
        
        let dispatchResult = semaphore.wait(timeout: .now() + .seconds(5))
        guard dispatchResult == .success else {
            throw KBError.databaseException("Failed to connect to database")
        }
        
        return store!
    }
}
