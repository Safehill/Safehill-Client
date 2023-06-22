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

//        
//        var store: KBKVStore? = nil
//        let semaphore = DispatchSemaphore(value: 0)
//        var timer: Timer? = nil
//        
//        DispatchQueue.main.async {
//            timer = Timer.scheduledTimer(withTimeInterval: 1, repeats: true, block: { _ in
//                if let s = KBKVStore.store(withName: name) {
//                    store = s
//                    semaphore.signal()
//                    timer?.invalidate()
//                    timer = nil
//                }
//            })
//        }
//        
//        let dispatchResult = semaphore.wait(timeout: .now() + .seconds(5))
//        guard dispatchResult == .success else {
//            throw KBError.databaseException("Failed to connect to database")
//        }
//        
//        return store!
    }
}
