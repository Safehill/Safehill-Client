import Foundation
import KnowledgeBase


public struct SHAssetDownloadController {
    let user: SHLocalUser
    let delegate: SHAssetDownloaderDelegate?
    
    public init(user: SHLocalUser, delegate: SHAssetDownloaderDelegate? = nil) {
        self.user = user
        self.delegate = delegate
    }
    
    /// Invoked during local cleanup (when the local user is removed or a new login happens, for instance)
    public func deepClean() throws {
        let userStore = try SHDBManager.sharedInstance.userStore()
        let _ = try userStore.removeValues(forKeysMatching: KBGenericCondition(.beginsWith, value: "auth-"))
        
        // Unauthorized queues are removed in LocalServer::runDataMigrations
        
        try DownloadBlacklist.shared.deepClean()
    }
    
    /// - Parameter userId: the user identifier
    /// - Returns: the asset identifiers that require authorization from the user
    public func unauthorizedDownloads(for userId: String) throws -> [GlobalIdentifier] {
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
                                   completionHandler: @escaping (Result<Void, Error>) -> Void) {
        self.removeUsersFromBlacklist(with: [userId])
        
        guard let unauthorizedQueue = try? BackgroundOperationQueue.of(type: .unauthorizedDownload) else {
            log.error("Unable to connect to local queue or database")
            completionHandler(.failure(SHBackgroundOperationError.fatalError("Unable to connect to local queue or database")))
            return
        }
        
        do {
            let assetGIdList = try self.unauthorizedDownloads(for: userId)
            guard assetGIdList.count > 0 else {
                completionHandler(.success(()))
                return
            }
            
            let descriptors = try self.dequeue(from: unauthorizedQueue,
                                               descriptorsForItemsWithIdentifiers: assetGIdList)
            
            let userStore = try SHDBManager.sharedInstance.userStore()
            let key = "auth-" + userId
            let _ = try userStore.removeValues(forKeysMatching: KBGenericCondition(.equal, value: key))
            
            self.startDownloadOf(descriptors: descriptors, completionHandler: completionHandler)
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
    func startDownloadOf(descriptors: [any SHAssetDescriptor],
                         from users: [SHServerUser]? = nil,
                         completionHandler: @escaping (Result<Void, Error>) -> Void) {
        guard let authorizedQueue = try? BackgroundOperationQueue.of(type: .download) else {
            log.error("Unable to connect to local queue or database")
            completionHandler(.failure(SHBackgroundOperationError.fatalError("Unable to connect to local queue or database")))
            return
        }
        
        var usersManifest = [SHServerUser]()
        if users == nil {
            var userIdentifiers = Set(descriptors.flatMap { $0.sharingInfo.sharedWithUserIdentifiersInGroup.keys })
            userIdentifiers.formUnion(Set(descriptors.compactMap { $0.sharingInfo.sharedByUserIdentifier }))
            
            do {
                usersManifest = try SHUsersController(localUser: self.user).getUsers(withIdentifiers: Array(userIdentifiers))
            } catch {
                log.error("Unable to fetch users mentioned in asset descriptors: \(error.localizedDescription)")
                completionHandler(.failure(error))
                return
            }
        } else {
            usersManifest = users!
        }
        
        do {
            try SHKGQuery.ingest(descriptors, receiverUserId: self.user.identifier)
        } catch {
            log.error("[KG] failed to ingest some descriptor into the graph")
        }
        self.delegate?.handleAssetDescriptorResults(for: descriptors, users: usersManifest, completionHandler: nil)
        
        do {
            try self.enqueue(descriptors: descriptors, in: authorizedQueue)
            completionHandler(.success(()))
        } catch {
            completionHandler(.failure(error))
        }
    }
}

// - MARK: Helpers for enqueueing and dequeueing

private extension SHAssetDownloadController {
    
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

// - MARK: Index additions

private extension SHAssetDownloadController {
    
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

internal extension SHAssetDownloadController {
    
    /// Asset identifiers and user identifiers passed to this methods are coming from the server.
    /// Everything that is in the local DB referencing users or assets not in these set is considered stale and should be removed.
    /// - Parameters:
    ///   - allSharedAssetIds: the full list of asset identifiers that are shared with this user
    ///   - allUserIds: all user ids that this user is connected to
    func cleanEntriesNotIn(allSharedAssetIds: [GlobalIdentifier], allUserIds: [String]) throws {
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
        try self.dequeueEntries(for: removedAssetGIds)
    }
    
    func cleanEntries(for assetIdentifiers: [GlobalIdentifier]) throws {
        guard assetIdentifiers.count > 0 else {
            return
        }
        
        try self.dequeueEntries(for: assetIdentifiers)
        
        let userStore = try SHDBManager.sharedInstance.userStore()
        let key = "auth-" + self.user.identifier
        if var assetGIdList = try userStore.value(for: key) as? [String] {
            assetGIdList.removeAll(where: { assetIdentifiers.contains($0) })
            try userStore.set(value: assetGIdList, for: key)
        }
    }
    
    private func dequeueEntries(for assetIdentifiers: [GlobalIdentifier]) throws {
        guard assetIdentifiers.count > 0 else {
            return
        }
        
        let queueTypes: [BackgroundOperationQueue.OperationType] = [.download, .unauthorizedDownload]
        for queueType in queueTypes {
            guard let queue = try? BackgroundOperationQueue.of(type: queueType) else {
                log.error("Unable to connect to local queue or database \(queueType.identifier)")
                throw SHBackgroundOperationError.fatalError("Unable to connect to local queue or database \(queueType.identifier)")
            }
            
            let _ = try self.dequeue(from: queue, descriptorsForItemsWithIdentifiers: assetIdentifiers)
        }
    }
}

// - MARK: User black/white listing

public extension SHAssetDownloadController {
    var blacklistedUsers: [String] {
        DownloadBlacklist.shared.blacklistedUsers
    }
    
    func blacklistUser(with userId: String) {
        DownloadBlacklist.shared.blacklist(userIdentifier: userId)
    }
    
    func removeUsersFromBlacklist(with userIds: [String]) {
        DownloadBlacklist.shared.removeFromBlacklist(userIdentifiers: userIds)
    }
}
