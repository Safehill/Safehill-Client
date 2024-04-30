import Foundation
import KnowledgeBase
import os

protocol SHOutboundBackgroundOperation {
    
    var operationType: BackgroundOperationQueue.OperationType { get }
    var processingState: ProcessingState { get }
    var log: Logger { get }
    var limit: Int { get }
    
    func run(
        forQueueItemIdentifiers queueItemIdentifiers: [String],
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    )
        
    func runOnce(
        for item: KBQueueItem,
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    )
    
    func runOnce(
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    )
    
    func process(
        _ item: KBQueueItem,
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    )
}

extension SHOutboundBackgroundOperation {
    
    func runOnce(
        for item: KBQueueItem,
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        guard Safehill_Client.processingState(for: item.identifier) != self.processingState else {
            completionHandler(.failure(SHBackgroundOperationError.alreadyProcessed))
            return
        }
        
        let queue: KBQueueStore
        do {
            queue = try BackgroundOperationQueue.of(type: self.operationType)
        } catch {
            log.critical("failed to read from \(self.operationType.identifier) queue. \(error.localizedDescription)")
            completionHandler(.failure(error))
            return
        }
        
        setProcessingState(self.processingState, for: item.identifier)
        
        /// Check the item still exists in the queue
        /// Because it was retrieved earlier it might already have been processed by a competing process
        queue.retrieveItem(withIdentifier: item.identifier) { result in
            switch result {
            case .success(let queuedItem):
                guard let queuedItem else {
                    setProcessingState(nil, for: item.identifier)
                    completionHandler(.success(()))
                    return
                }
                
                self.log.info("\(self.operationType.identifier) item \(queuedItem.identifier) created at \(queuedItem.createdAt)")
                
                self.process(queuedItem, qos: qos) { result in
                    switch result {
                    case .success:
                        self.log.info("[√] \(self.operationType.identifier) task completed for item \(queuedItem.identifier)")
                    case .failure(let error):
                        self.log.critical("[x] \(self.operationType.identifier) task failed for item \(queuedItem.identifier): \(error.localizedDescription)")
                    }
                    
                    setProcessingState(nil, for: queuedItem.identifier)
                    completionHandler(result)
                }
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    func run(
        forQueueItemIdentifiers queueItemIdentifiers: [String],
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        let queue: KBQueueStore
        
        do {
            queue = try BackgroundOperationQueue.of(type: self.operationType)
        } catch {
            log.critical("failed to read from \(self.operationType.identifier) queue. \(error.localizedDescription)")
            completionHandler(.failure(error))
            return
        }
        
        var queueItems = [KBQueueItem]()
        var error: Error? = nil
        let group = DispatchGroup()
        group.enter()
        queue.retrieveItems(withIdentifiers: queueItemIdentifiers) {
            result in
            switch result {
            case .success(let items):
                queueItems = items
            case .failure(let err):
                error = err
            }
            group.leave()
        }
        
        group.notify(queue: .global(qos: qos)) {
            guard error == nil else {
                self.log.critical("failed to retrieve items from \(self.operationType.identifier) queue. \(error!.localizedDescription)")
                completionHandler(.failure(error!))
                return
            }
            
            let semaphore = DispatchSemaphore(value: 0)
            
            for item in queueItems {
                group.enter()
                self.runOnce(for: item, qos: qos) { _ in
                    semaphore.signal()
                }
                
                semaphore.wait()
            }
            
            completionHandler(.success(()))
        }
    }
    
    func runOnce(
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        let queue: KBQueueStore
        do {
            queue = try BackgroundOperationQueue.of(type: self.operationType)
        } catch {
            log.critical("failed to read from \(self.operationType.identifier) queue. \(error.localizedDescription)")
            completionHandler(.failure(error))
            return
        }
        
        var queueItems = [KBQueueItem]()
        var error: Error? = nil
        let group = DispatchGroup()
        group.enter()
        
        let interval = DateInterval(start: Date.distantPast, end: Date())
        queue.peekItems(
            createdWithin: interval,
            limit: self.limit > 0 ? self.limit : nil
        ) { result in
            switch result {
            case .success(let items):
                queueItems = items
            case .failure(let err):
                error = err
            }
            group.leave()
        }
        
        guard queueItems.isEmpty == false else {
            completionHandler(.success(()))
            return
        }
        
        group.notify(queue: .global(qos: qos)) {
            guard error == nil else {
                self.log.critical("failed to retrieve items from FETCH queue. \(error!.localizedDescription)")
                completionHandler(.failure(error!))
                return
            }
            
            var count = 0
            
            let semaphore = DispatchSemaphore(value: 0)
            
            for item in queueItems {
                count += 1
                self.runOnce(for: item, qos: qos) { _ in
                    semaphore.signal()
                }
                
                semaphore.wait()
            }
            
            completionHandler(.success(()))
            
            self.log.info("started \(count) \(self.operationType.identifier) operations")
        }
    }
    
    public func run(
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        self.runOnce(qos: qos, completionHandler: completionHandler)
    }
}
