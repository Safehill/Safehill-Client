import Foundation
import KnowledgeBase


public struct SHAssetDownloadController {
    let user: SHLocalUser
    let delegate: SHAssetDownloaderDelegate
    
    public init(user: SHLocalUser, delegate: SHAssetDownloaderDelegate) {
        self.user = user
        self.delegate = delegate
    }
    
    public func unauthorizedDownloads(for userId: String) throws -> [GlobalIdentifier] {
        let userStore = try SHDBManager.sharedInstance.userStore()
        let key = "auth-" + userId
        
        guard let assetGIdList = try userStore.value(for: key) as? [String] else {
            throw SHBackgroundOperationError.missingUnauthorizedDownloadIndexForUserId(userId)
        }
        return assetGIdList
    }
    
    private func indexUnauthorizedDownloads(from descriptors: [any SHAssetDescriptor]) throws {
        let userStore = try SHDBManager.sharedInstance.userStore()
        
        for descr in descriptors {
            let key = "auth-" + descr.sharingInfo.sharedByUserIdentifier
            if var assetGIdList = try userStore.value(for: key) as? [String] {
                assetGIdList.append(descr.globalIdentifier)
                try userStore.set(value: Array(Set(assetGIdList)), for: key)
            } else {
                let assetGIdList = [descr.globalIdentifier]
                try userStore.set(value: assetGIdList, for: key)
            }
        }
    }
    
    private func removeUnauthorizedDownloadsFromIndex(for userId: String) throws {
        let userStore = try SHDBManager.sharedInstance.userStore()
        let key = "auth-" + userId
        let _ = try userStore.removeValues(forKeysMatching: KBGenericCondition(.equal, value: key))
    }
    
    public func authorizeDownloads(for userId: String,
                                   completionHandler: @escaping (Result<Void, Error>) -> Void) {
        guard let unauthorizedQueue = try? BackgroundOperationQueue.of(type: .unauthorizedDownload) else {
            log.error("Unable to connect to local queue or database")
            completionHandler(.failure(SHBackgroundOperationError.fatalError("Unable to connect to local queue or database")))
            return
        }
        
        do {
            let assetGIdList = try self.unauthorizedDownloads(for: userId)
            let descriptors = try self.dequeue(from: unauthorizedQueue,
                                               descriptorsForItemsWithIdentifiers: assetGIdList)
            try self.removeUnauthorizedDownloadsFromIndex(for: userId)
            
            self.delegate.handleAssetDescriptorResults(for: descriptors, users: [])
            
            self.startDownloadOf(descriptors: descriptors, completionHandler: completionHandler)
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
            try self.indexUnauthorizedDownloads(from: descriptors)
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
    
    private func dequeue(from queue: KBQueueStore, descriptorsForItemsWithIdentifiers identifiers: [String]) throws -> [any SHAssetDescriptor] {
        let group = DispatchGroup()
        var dequeuedDescriptors = [any SHAssetDescriptor]()
        var errors = [String: any Error]()
        
        for assetGId in identifiers {
            group.enter()
            queue.retrieveItem(withIdentifier: assetGId) { result in
                switch result {
                case .success(let item):
                    do {
                        guard let data = item?.content as? Data else {
                            throw SHBackgroundOperationError.unexpectedData(item?.content)
                        }
                        
                        let unarchiver: NSKeyedUnarchiver
                        if #available(macOS 10.13, *) {
                            unarchiver = try NSKeyedUnarchiver(forReadingFrom: data)
                        } else {
                            unarchiver = NSKeyedUnarchiver(forReadingWith: data)
                        }
                        
                        guard let downloadRequest = unarchiver.decodeObject(of: SHDownloadRequestQueueItem.self, forKey: NSKeyedArchiveRootObjectKey) else {
                            throw SHBackgroundOperationError.unexpectedData(data)
                        }
                        
                        dequeuedDescriptors.append(downloadRequest.assetDescriptor)
                        
                    } catch {
                        errors[assetGId] = error
                    }
                    
                case .failure(let error):
                    errors[assetGId] = error
                }
                
                queue.removeValue(for: assetGId) { (_: Swift.Result) in
                    group.leave()
                }
            }
        }
        
        let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds * identifiers.count))
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
        
        if errors.count > 0 {
            log.error("Error dequeueing from queue \(queue.name): \(errors)")
            for (_, error) in errors {
                throw error
            }
        }
        
        return dequeuedDescriptors
    }
}
