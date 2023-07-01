import KnowledgeBase

public struct SHAssetDownloadController {
    let user: SHLocalUser
    
    public func authorizeDownload(ofDescriptors descriptors: [any SHAssetDescriptor],
                                  completionHandler: @escaping (Result<Void, Error>) -> Void) {
        guard let authorizedQueue = try? BackgroundOperationQueue.of(type: .download) else {
            log.error("Unable to connect to local queue or database")
            completionHandler(.failure(SHBackgroundOperationError.fatalError("Unable to connect to local queue or database")))
            return
        }
        guard let unauthorizedQueue = try? BackgroundOperationQueue.of(type: .unauthorizedDownload) else {
            log.error("Unable to connect to local queue or database")
            completionHandler(.failure(SHBackgroundOperationError.fatalError("Unable to connect to local queue or database")))
            return
        }
        
        do {
            try self.dequeue(from: unauthorizedQueue, itemsWithIdentifiers: descriptors.map({ $0.globalIdentifier }))
            try self.enqueue(descriptors: descriptors, in: authorizedQueue)
            completionHandler(.success(()))
        } catch {
            completionHandler(.failure(error))
        }
    }
    
    func waitForDownloadAuthorization(forDescriptors descriptors: [any SHAssetDescriptor],
                                      completionHandler: @escaping (Result<Void, Error>) -> Void) {
        guard let unauthorizedQueue = try? BackgroundOperationQueue.of(type: .unauthorizedDownload) else {
            log.error("Unable to connect to local queue or database")
            completionHandler(.failure(SHBackgroundOperationError.fatalError("Unable to connect to local queue or database")))
            return
        }
        
        do {
            try self.enqueue(descriptors: descriptors, in: unauthorizedQueue)
            completionHandler(.success(()))
        } catch {
            completionHandler(.failure(error))
        }
    }
    
    func startDownloadOf(descriptors: [any SHAssetDescriptor],
                         completionHandler: @escaping (Result<Void, Error>) -> Void) {
        guard let authorizedQueue = try? BackgroundOperationQueue.of(type: .download) else {
            log.error("Unable to connect to local queue or database")
            completionHandler(.failure(SHBackgroundOperationError.fatalError("Unable to connect to local queue or database")))
            return
        }
        
        do {
            try self.enqueue(descriptors: descriptors, in: authorizedQueue)
            completionHandler(.success(()))
        } catch {
            completionHandler(.failure(error))
        }
    }
    
    private func enqueue(descriptors: [any SHAssetDescriptor], in queue: KBQueueStore) throws {
        var errors = [Error]()
        
        for descr in descriptors {
            let queueItemIdentifier = descr.globalIdentifier
            guard let existingItemIdentifiers = try? queue.keys(matching: KBGenericCondition(.equal, value: queueItemIdentifier)),
                  existingItemIdentifiers.isEmpty else {
                log.info("Not enqueuing item \(queueItemIdentifier) in queue \(queue.name) as a request with the same identifier hasn't been fulfilled yet")
                continue
            }
            
            let queueItem = SHDownloadRequestQueueItem(
                assetDescriptor: descr,
                receiverUserIdentifier: self.user.identifier
            )
            log.info("enqueuing item \(queueItemIdentifier) in queue \(queue.name)")
            do {
                try queueItem.enqueue(in: queue, with: queueItemIdentifier)
            } catch {
                log.error("error enqueueing in queue \(queue.name). \(error.localizedDescription)")
                errors.append(error)
                continue
            }
        }
        
        if errors.count > 0 {
            throw errors.first!
        }
    }
    
    private func dequeue(from queue: KBQueueStore, itemsWithIdentifiers identifiers: [String]) throws {
        
    }
}
