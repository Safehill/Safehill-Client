import Foundation
import KnowledgeBase


public struct SHAssetDownloadAuthorizationResponse {
    public let descriptors: [any SHAssetDescriptor]
    public let users: [any SHServerUser]
}

public struct SHAssetsDownloadManager {
    let user: SHLocalUserProtocol
    
    public init(user: SHLocalUserProtocol) {
        self.user = user
    }
    
    /// Invoked during local cleanup (when the local user is removed or a new login happens, for instance)
    public static func deepClean() throws {
        let userStore = try SHDBManager.sharedInstance.userStore()
        let _ = try userStore.removeValues(forKeysMatching: KBGenericCondition(.beginsWith, value: "auth-"))
        
        // Unauthorized queues are removed in LocalServer::runDataMigrations
        
        try DownloadBlacklist.shared.deepClean()
    }
    
    /// - Parameter userId: the user identifier
    /// - Returns: the asset identifiers that require authorization from the user
    public static func unauthorizedDownloads(for userId: String) throws -> [GlobalIdentifier] {
        let userStore = try SHDBManager.sharedInstance.userStore()
        let key = "auth-" + userId
        
        guard let assetGIdList = try userStore.value(for: key) as? [String] else {
            throw SHBackgroundOperationError.missingUnauthorizedDownloadIndexForUserId(userId)
        }
        return assetGIdList
    }
    
    /// Authorizing downloads from a user means:
    /// - Moving the items in the unauthorized queue to the authorized queue for that user
    /// - Removing items from the auth index (user store) corresponding to the user identifier
    /// - Parameters:
    ///   - userId: the user identifier
    ///   - completionHandler: the callback method
    public func authorizeDownloads(from userId: String,
                                   completionHandler: @escaping (Result<SHAssetDownloadAuthorizationResponse, Error>) -> Void) {
        SHAssetsDownloadManager.removeUsersFromBlacklist(with: [userId])
        
        guard let unauthorizedQueue = try? BackgroundOperationQueue.of(type: .unauthorizedDownload) else {
            log.error("Unable to connect to local queue or database")
            completionHandler(.failure(SHBackgroundOperationError.fatalError("Unable to connect to local queue or database")))
            return
        }
        
        do {
            let assetGIdList = try SHAssetsDownloadManager.unauthorizedDownloads(for: userId)
            guard assetGIdList.count > 0 else {
                let response = SHAssetDownloadAuthorizationResponse(
                    descriptors: [],
                    users: []
                )
                completionHandler(.success(response))
                return
            }
            
            let descriptors = try SHAssetsDownloadManager.dequeue(
                from: unauthorizedQueue,
                itemsWithIdentifiers: assetGIdList
            )
            
            let userStore = try SHDBManager.sharedInstance.userStore()
            let key = "auth-" + userId
            let _ = try userStore.removeValues(forKeysMatching: KBGenericCondition(.equal, value: key))
            
            self.startAuthorizedDownload(of: descriptors) { result in
                switch result {
                case .success(let users):
                    let response = SHAssetDownloadAuthorizationResponse(
                        descriptors: descriptors,
                        users: users
                    )
                    completionHandler(.success(response))
                case .failure(let error):
                    completionHandler(.failure(error))
                }
            }
        } catch {
            completionHandler(.failure(error))
        }
    }
    
    /// Enqueue items that are ready for download but not explicitly authorized in the unauthorized queue
    /// and update the auth index (user store) with a list of global identifiers waiting download, per user
    /// - Parameters:
    ///   - descriptors: the list of asset descriptors that are ready for download
    ///   - completionHandler: the callback method
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
    
    /// Enqueue downloads from authorized users in the authorized queue, so they'll be picked up for download
    /// - Parameters:
    ///   - descriptors: the list of asset descriptors for the assets that can be downloaded
    ///   - users: the cache of user objects, if one was already retrieved by the caller method
    ///   - completionHandler: the callback method
    func startDownload(of descriptors: [any SHAssetDescriptor],
                       completionHandler: @escaping (Result<Void, Error>) -> Void) {
        guard descriptors.count > 0 else {
            completionHandler(.success(()))
            return
        }
        guard let authorizedQueue = try? BackgroundOperationQueue.of(type: .download) else {
            log.error("Unable to connect to local queue or database")
            completionHandler(.failure(SHBackgroundOperationError.fatalError("Unable to connect to local queue or database")))
            return
        }
        
        do {
            try self.enqueue(descriptors: descriptors, in: authorizedQueue)
            do {
                try SHKGQuery.ingest(descriptors, receiverUserId: self.user.identifier)
            } catch {
                log.error("[KG] failed to ingest some descriptor into the graph")
            }
            completionHandler(.success(()))
        } catch {
            completionHandler(.failure(error))
        }
    }
    
    func startAuthorizedDownload(of descriptors: [any SHAssetDescriptor],
                                 completionHandler: @escaping (Result<[SHServerUser], Error>) -> Void) {
        var users = [SHServerUser]()
        var userIdentifiers = Set(descriptors.flatMap { $0.sharingInfo.sharedWithUserIdentifiersInGroup.keys })
        userIdentifiers.formUnion(Set(descriptors.compactMap { $0.sharingInfo.sharedByUserIdentifier }))
        
        do {
            users = try SHUsersController(localUser: self.user).getUsers(withIdentifiers: Array(userIdentifiers))
        } catch {
            log.error("Unable to fetch users mentioned in asset descriptors: \(error.localizedDescription)")
            completionHandler(.failure(error))
            return
        }
        
        self.startDownload(of: descriptors) { result in
            switch result {
            case .success():
                completionHandler(.success(users))
            case .failure(let failure):
                completionHandler(.failure(failure))
            }
        }
    }
    
    func stopDownload(ofAssetsWith globalIdentifiers: [GlobalIdentifier]) throws {
        try SHKGQuery.removeAssets(with: globalIdentifiers)
        try SHAssetsDownloadManager.dequeueEntries(for: globalIdentifiers)
    }
}

// - MARK: Helpers for enqueueing and dequeueing

private extension SHAssetsDownloadManager {
    
    private func enqueue(descriptors: [any SHAssetDescriptor], in queue: KBQueueStore) throws {
        var errors = [Error]()
        
        for descr in descriptors {
            let queueItemIdentifier = descr.globalIdentifier
            guard let existingItemIdentifiers = try? queue.keys(matching: KBGenericCondition(.equal, value: queueItemIdentifier)),
                  existingItemIdentifiers.isEmpty else {
                log.info("Not enqueueing item \(queueItemIdentifier) in queue \(queue.name) as a request with the same identifier hasn't been fulfilled yet")
                continue
            }
            
            let queueItem = SHDownloadRequestQueueItem(
                assetDescriptor: descr,
                receiverUserIdentifier: self.user.identifier
            )
            log.info("enqueueing item \(queueItemIdentifier) in queue \(queue.name)")
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
    
    private static func dequeue(from queue: KBQueueStore, itemsWithIdentifiers identifiers: [GlobalIdentifier]) throws -> [any SHAssetDescriptor] {
        let group = DispatchGroup()
        var dequeuedDescriptors = [any SHAssetDescriptor]()
        var errors = [String: any Error]()
        
        for assetGId in identifiers {
            group.enter()
            queue.retrieveItem(withIdentifier: assetGId) { result in
                switch result {
                case .success(let item):
                    do {
                        guard let item = item else {
                            /// If no such items exist in the queue, return
                            group.leave()
                            return
                        }
                        
                        guard let data = item.content as? Data else {
                            throw SHBackgroundOperationError.unexpectedData(item.content)
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
                
                queue.removeValue(for: assetGId) { (_: Result) in
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

// - MARK: Index additions

private extension SHAssetsDownloadManager {
    
    private func indexUnauthorizedDownloads(from descriptors: [any SHAssetDescriptor]) throws {
        let userStore = try SHDBManager.sharedInstance.userStore()
        let writeBatch = userStore.writeBatch()
        var updatedKVs = [String: [String]]()
        
        for descr in descriptors {
            let key = "auth-" + descr.sharingInfo.sharedByUserIdentifier
            var newAssetGIdList: [GlobalIdentifier]
            if let assetGIdList = try userStore.value(for: key) as? [String] {
                newAssetGIdList = updatedKVs[key] ?? assetGIdList
            } else {
                newAssetGIdList = updatedKVs[key] ?? []
            }
            newAssetGIdList.append(descr.globalIdentifier)
            writeBatch.set(value: Array(Set(newAssetGIdList)), for: key)
            updatedKVs[key] = newAssetGIdList
        }
        
        try writeBatch.write()
    }
}

// - MARK: Index and Queue Cleanup

internal extension SHAssetsDownloadManager {
    
    /// Asset identifiers and user identifiers passed to this methods are coming from the server.
    /// Everything that is in the local DB referencing users or assets not in these set is considered stale and should be removed.
    /// - Parameters:
    ///   - allSharedAssetIds: the full list of asset identifiers that are shared with this user
    ///   - allUserIds: all user ids that this user is connected to
    static func cleanEntriesNotIn(allSharedAssetIds: [GlobalIdentifier], allUserIds: [String]) throws {
        let userStore = try SHDBManager.sharedInstance.userStore()
        var condition = KBGenericCondition(value: false)
        
        /// Remove dangling users
        for userId in allUserIds {
            condition = condition.or(KBGenericCondition(.equal, value: "auth-\(userId)", negated: true))
        }
        condition = KBGenericCondition(.beginsWith, value: "auth-").and(condition)
        let _ = try userStore.removeValues(forKeysMatching: condition)
        
        /// Remove dangling assets (if unauthorized user stops sharing)
        let writeBatch = userStore.writeBatch()
        var removedAssetGIds = [GlobalIdentifier]()
        for (key, value) in try userStore.dictionaryRepresentation(forKeysMatching: KBGenericCondition(.beginsWith, value: "auth-")) {
            if let value = value as? [String] {
                let intersection = value.subtract(allSharedAssetIds)
                if intersection.isEmpty == false {
                    removedAssetGIds.append(contentsOf: intersection)
                    let newValue = value.filter({ allSharedAssetIds.contains($0) })
                    writeBatch.set(value: newValue, for: key)
                }
            } else {
                writeBatch.set(value: nil, for: key)
            }
        }
        try writeBatch.write()
        
        /// Remove queue item indentifiers in the download queues
        try SHAssetsDownloadManager.dequeueEntries(for: removedAssetGIds)
    }
    
    func cleanEntries(for assetIdentifiers: [GlobalIdentifier]) throws {
        guard assetIdentifiers.count > 0 else {
            return
        }
        
        try SHAssetsDownloadManager.dequeueEntries(for: assetIdentifiers)
        
        let userStore = try SHDBManager.sharedInstance.userStore()
        let key = "auth-" + self.user.identifier
        if var assetGIdList = try userStore.value(for: key) as? [String] {
            assetGIdList.removeAll(where: { assetIdentifiers.contains($0) })
            try userStore.set(value: assetGIdList, for: key)
        }
    }
    
    private static func dequeueEntries(for assetIdentifiers: [GlobalIdentifier]) throws {
        guard assetIdentifiers.count > 0 else {
            return
        }
        
        let queueTypes: [BackgroundOperationQueue.OperationType] = [.download, .unauthorizedDownload]
        for queueType in queueTypes {
            guard let queue = try? BackgroundOperationQueue.of(type: queueType) else {
                log.error("Unable to connect to local queue or database \(queueType.identifier)")
                throw SHBackgroundOperationError.fatalError("Unable to connect to local queue or database \(queueType.identifier)")
            }
            
            let _ = try SHAssetsDownloadManager.dequeue(from: queue, itemsWithIdentifiers: assetIdentifiers)
        }
    }
}
