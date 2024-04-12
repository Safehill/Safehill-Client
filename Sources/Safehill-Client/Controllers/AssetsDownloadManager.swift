import Foundation
import KnowledgeBase
import Safehill_Crypto

public struct SHAssetDownloadAuthorizationResponse {
    public let descriptors: [any SHAssetDescriptor]
    public let users: [UserIdentifier: any SHServerUser]
}

public enum SHAssetDownloadError: Error {
    case assetIsBlacklisted(GlobalIdentifier)
}

public struct SHAssetsDownloadManager {
    let user: SHLocalUserProtocol
    
    public init(user: SHLocalUserProtocol) {
        self.user = user
    }
    
    /// Invoked during local cleanup (when the local user is removed or a new login happens, for instance)
    public static func deepClean() throws {
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            throw KBError.databaseNotReady
        }
        let _ = try userStore.removeValues(forKeysMatching: KBGenericCondition(.beginsWith, value: "auth-"))
        
        let queueTypes: [BackgroundOperationQueue.OperationType] = [.unauthorizedDownload]
        for queueType in queueTypes {
            guard let queue = try? BackgroundOperationQueue.of(type: queueType) else {
                log.error("Unable to connect to local queue or database \(queueType.identifier)")
                throw SHBackgroundOperationError.fatalError("Unable to connect to local queue or database \(queueType.identifier)")
            }
            
            let _ = try queue.removeAll()
        }
        
        // Unauthorized queues are removed in LocalServer::runDataMigrations
        Task(priority: .low) {
            do {
                try await SHDownloadBlacklist.shared.deepClean()
            } catch {
                log.error("failed to clean the download blacklist")
            }
        }
    }
    
    /// - Parameter userId: the user identifier
    /// - Returns: the asset identifiers that require authorization from the user
    public static func unauthorizedDownloads(for userId: String) throws -> [GlobalIdentifier] {
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            throw KBError.databaseNotReady
        }
        let key = "auth-" + userId
        
        guard let assetGIdList = try userStore.value(for: key) as? [String] else {
            throw SHBackgroundOperationError.missingUnauthorizedDownloadIndexForUserId(userId)
        }
        return assetGIdList
    }
    
    private func fetchAsset(
        withGlobalIdentifier globalIdentifier: GlobalIdentifier,
        quality: SHAssetQuality,
        descriptor: any SHAssetDescriptor,
        completionHandler: @escaping (Result<any SHDecryptedAsset, Error>) -> Void
    ) {
        let start = CFAbsoluteTimeGetCurrent()
        
        log.info("downloading assets with identifier \(globalIdentifier) version \(quality.rawValue)")
        user.serverProxy.getAssets(
            withGlobalIdentifiers: [globalIdentifier],
            versions: [quality]
        )
        { result in
            switch result {
            case .success(let assetsDict):
                guard assetsDict.count > 0,
                      let encryptedAsset = assetsDict[globalIdentifier] else {
                    completionHandler(.failure(SHHTTPError.ClientError.notFound))
                    return
                }
                let localAssetStore = SHLocalAssetStoreController(
                    user: self.user
                )
                localAssetStore.decryptedAsset(
                    encryptedAsset: encryptedAsset,
                    quality: quality,
                    descriptor: descriptor
                ) { result in
                    switch result {
                    case .failure(let error):
                        completionHandler(.failure(error))
                    case .success(let decryptedAsset):
                        completionHandler(.success(decryptedAsset))
                    }
                }
            case .failure(let err):
                log.critical("unable to download assets \(globalIdentifier) version \(quality.rawValue) from server: \(err)")
                completionHandler(.failure(err))
            }
            let end = CFAbsoluteTimeGetCurrent()
            log.debug("[PERF] \(CFAbsoluteTime(end - start)) for version \(quality.rawValue)")
        }
    }
    
    /// Authorizing downloads from a user means:
    /// - Moving the items in the unauthorized queue to the authorized queue for that user
    /// - Removing items from the auth index (user store) corresponding to the user identifier
    /// - Parameters:
    ///   - userId: the user identifier
    ///   - completionHandler: the callback method
    public func authorizeDownloads(
        from userId: UserIdentifier,
        completionHandler: @escaping (Result<SHAssetDownloadAuthorizationResponse, Error>) -> Void
    ) {
        guard self.user is SHAuthenticatedLocalUser else {
            completionHandler(.failure(SHLocalUserError.notAuthenticated))
            return
        }
        
        guard let unauthorizedQueue = try? BackgroundOperationQueue.of(type: .unauthorizedDownload) else {
            log.error("Unable to connect to local queue or database")
            completionHandler(.failure(SHBackgroundOperationError.fatalError("Unable to connect to local queue or database")))
            return
        }
        
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        Task(priority: .high) {
            await SHDownloadBlacklist.shared.removeFromBlacklist(userIdentifiers: [userId])
            
            do {
                var assetGIdList = [GlobalIdentifier]()
                do {
                    assetGIdList = try SHAssetsDownloadManager.unauthorizedDownloads(for: userId)
                } catch SHBackgroundOperationError.missingUnauthorizedDownloadIndexForUserId {}
                
                guard assetGIdList.count > 0 else {
                    try SHKGQuery.recordExplicitAuthorization(by: self.user.identifier, for: userId)
                    let response = SHAssetDownloadAuthorizationResponse(
                        descriptors: [],
                        users: [:]
                    )
                    completionHandler(.success(response))
                    return
                }
                
                let descriptors = try SHAssetsDownloadManager.dequeue(
                    from: unauthorizedQueue,
                    itemsWithIdentifiers: assetGIdList
                )
                let key = "auth-" + userId
                let _ = try userStore.removeValues(forKeysMatching: KBGenericCondition(.equal, value: key))
                
                try SHKGQuery.ingest(descriptors, receiverUserId: self.user.identifier)
                try SHKGQuery.recordExplicitAuthorization(by: self.user.identifier, for: userId)
                
                self.user.serverProxy.getUsers(inAssetDescriptors: descriptors) {
                    getUsersResult in
                    switch getUsersResult {
                    case .success(let users):
                        let usersById = users.reduce([UserIdentifier: any SHServerUser]()) { partialResult, serverUser in
                            var result = partialResult
                            result[serverUser.identifier] = serverUser
                            return result
                        }
                        let response = SHAssetDownloadAuthorizationResponse(
                            descriptors: descriptors,
                            users: usersById
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
            log.debug("[downloadManager] index BEFORE \(try! SHDBManager.sharedInstance.userStore!.keys(matching: KBGenericCondition(.beginsWith, value: "auth-")))")
            log.debug("[downloadManager] enqueueing descriptors for senders \(descriptors.map({ ($0.sharingInfo.sharedByUserIdentifier, $0.globalIdentifier) })) to unauthorized queue")
            let enqueuedGids = try self.enqueue(descriptors: descriptors, in: unauthorizedQueue)
            log.debug("[downloadManager] enqueued asset gids \(descriptors.map({ $0.globalIdentifier }))")
            try self.indexUnauthorizedDownloads(from: descriptors, filtering: enqueuedGids)
            log.debug("[downloadManager] index AFTER \(try! SHDBManager.sharedInstance.userStore!.keys(matching: KBGenericCondition(.beginsWith, value: "auth-")))")
            completionHandler(.success(()))
        } catch {
            completionHandler(.failure(error))
        }
    }
    
    /// Downloads the asset for the given descriptor, decrypts it, and returns the decrypted version, or an error.
    /// - Parameters:
    ///   - descriptor: the descriptor for the assets to download
    ///   - completionHandler: the callback
    func downloadAsset(
        for descriptor: any SHAssetDescriptor,
        completionHandler: @escaping (Result<any SHDecryptedAsset, Error>) -> Void
    ) {
        Task {
            
            log.info("[downloadManager] downloading assets with identifier \(descriptor.globalIdentifier)")
            
            let start = CFAbsoluteTimeGetCurrent()
            let globalIdentifier = descriptor.globalIdentifier
            
            // MARK: Start
            
            guard await SHDownloadBlacklist.shared.isBlacklisted(assetGlobalIdentifier: descriptor.globalIdentifier) == false else {
                
                do {
                    try SHKGQuery.removeAssets(with: [globalIdentifier])
                } catch {
                    log.warning("[downloadManager] Attempt to remove asset \(globalIdentifier) the knowledgeGraph because it's blacklisted FAILED. \(error.localizedDescription)")
                }
                
                completionHandler(.failure(SHAssetDownloadError.assetIsBlacklisted(globalIdentifier)))
                return
            }
            
            // MARK: Get Low Res asset
            
            self.fetchAsset(
                withGlobalIdentifier: globalIdentifier,
                quality: .lowResolution,
                descriptor: descriptor
            ) { result in
                
                switch result {
                case .success(let decryptedAsset):
                    completionHandler(.success(decryptedAsset))
                    
                    Task(priority: .low) {
                        await SHDownloadBlacklist.shared.removeFromBlacklist(assetGlobalIdentifier: globalIdentifier)
                    }
                case .failure(let error):
                    completionHandler(.failure(error))
                    
                    Task(priority: .low) {
                        if error is SHCypher.DecryptionError {
                            await SHDownloadBlacklist.shared.blacklist(globalIdentifier: globalIdentifier)
                        } else {
                            await SHDownloadBlacklist.shared.recordFailedAttempt(globalIdentifier: globalIdentifier)
                        }
                    }
                }
                
                let end = CFAbsoluteTimeGetCurrent()
                log.debug("[PERF] it took \(CFAbsoluteTime(end - start)) to download asset \(globalIdentifier)")
            }
        }
    }
}

// - MARK: Helpers for enqueueing and dequeueing

private extension SHAssetsDownloadManager {
    
    private func enqueue(descriptors: [any SHAssetDescriptor], in queue: KBQueueStore) throws -> [GlobalIdentifier] {
        var errors = [Error]()
        var enqueuedGIds = [GlobalIdentifier]()
        
        for descr in descriptors {
            let queueItemIdentifier = descr.globalIdentifier
            guard let existingItemIdentifiers = try? queue.keys(matching: KBGenericCondition(.equal, value: queueItemIdentifier)),
                  existingItemIdentifiers.isEmpty else {
                continue
            }
            
            enqueuedGIds.append(descr.globalIdentifier)
            
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
        
        if enqueuedGIds.isEmpty {
            throw SHBackgroundOperationError.alreadyProcessed
        }
        
        return enqueuedGIds
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
    
    private func indexUnauthorizedDownloads(
        from descriptors: [any SHAssetDescriptor],
        filtering assetGlobalIdentifiers: [GlobalIdentifier]
    ) throws {
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            throw KBError.databaseNotReady
        }
        let writeBatch = userStore.writeBatch()
        var updatedKVs = [String: [String]]()
        
        for descr in descriptors.filter({ assetGlobalIdentifiers.contains($0.globalIdentifier) }) {
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
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            throw KBError.databaseNotReady
        }
        var condition = KBGenericCondition(value: false)
        
        /// Remove dangling users
        for userId in allUserIds {
            condition = condition.and(KBGenericCondition(.equal, value: "auth-\(userId)", negated: true))
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
        
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            throw KBError.databaseNotReady
        }
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
        
        let queueTypes: [BackgroundOperationQueue.OperationType] = [.unauthorizedDownload]
        for queueType in queueTypes {
            guard let queue = try? BackgroundOperationQueue.of(type: queueType) else {
                log.error("Unable to connect to local queue or database \(queueType.identifier)")
                throw SHBackgroundOperationError.fatalError("Unable to connect to local queue or database \(queueType.identifier)")
            }
            
            let _ = try SHAssetsDownloadManager.dequeue(from: queue, itemsWithIdentifiers: assetIdentifiers)
        }
    }
}
