import Foundation
import KnowledgeBase
import os

protocol SHOutboundBackgroundOperation {
    
    var operationType: BackgroundOperationQueue.OperationType { get }
    var processingState: ProcessingState { get }
    var log: Logger { get }
    var limit: Int { get }
    
    func process(
        _ item: KBQueueItem,
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    )
    
    func run(
        forQueueItemIdentifiers queueItemIdentifiers: [String],
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    )
    
    func content(ofQueueItem item: KBQueueItem) throws -> SHSerializableQueueItem
}


extension SHOutboundBackgroundOperation {
    
    public func content(ofQueueItem item: KBQueueItem) throws -> SHSerializableQueueItem {
        guard let data = item.content as? Data else {
            throw SHBackgroundOperationError.unexpectedData(item.content)
        }
        
        let unarchiver: NSKeyedUnarchiver
        if #available(macOS 10.13, *) {
            unarchiver = try NSKeyedUnarchiver(forReadingFrom: data)
        } else {
            unarchiver = NSKeyedUnarchiver(forReadingWith: data)
        }
        
        guard let deserialized = unarchiver.decodeObject(
            of: SHGenericShareableGroupableQueueItem.self,
            forKey: NSKeyedArchiveRootObjectKey
        ) else {
            throw SHBackgroundOperationError.unexpectedData(data)
        }
        
        return deserialized
    }
}

extension SHOutboundBackgroundOperation {
    
    private func runOnce(
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
    
    private func runSequentially(
        item index: Int,
        of queueItems: [KBQueueItem],
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        guard queueItems.count > index else {
            completionHandler(.success(()))
            return
        }
        
        let queueItem = queueItems[index]
        self.runOnce(for: queueItem, qos: qos) { _ in
            self.runSequentially(
                item: index+1,
                of: queueItems,
                qos: qos,
                completionHandler: completionHandler
            )
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
        
        queue.retrieveItems(withIdentifiers: queueItemIdentifiers) {
            result in
            switch result {
            case .success(let items):
                self.runSequentially(
                    item: 0,
                    of: items,
                    qos: qos,
                    completionHandler: completionHandler
                )
            case .failure(let error):
                self.log.critical("failed to retrieve items from \(self.operationType.identifier) queue. \(error.localizedDescription)")
                completionHandler(.failure(error))
            }
        }
    }
    
    private func runOnce(
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
        
        let interval = DateInterval(start: Date.distantPast, end: Date())
        queue.peekItems(
            createdWithin: interval,
            limit: self.limit > 0 ? self.limit : nil
        ) { result in
            switch result {
            case .success(let items):
                self.runSequentially(
                    item: 0,
                    of: items,
                    qos: qos,
                    completionHandler: completionHandler
                )
            case .failure(let error):
                self.log.critical("failed to retrieve items from queue \(self.operationType.identifier). \(error.localizedDescription)")
                completionHandler(.failure(error))
                return
            }
        }
    }
    
    public func run(
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        self.runOnce(qos: qos, completionHandler: completionHandler)
    }
}
