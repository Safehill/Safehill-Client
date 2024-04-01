import Foundation
import os
import KnowledgeBase


internal class SHEncryptAndShareOperation: SHEncryptionOperation {
    
    public override var log: Logger {
        Logger(subsystem: "com.gf.safehill", category: "BG-SHARE")
    }
    
    let delegatesQueue = DispatchQueue(label: "com.safehill.encryptAndShare.delegates")
    
    public override func clone() -> SHBackgroundOperationProtocol {
        SHEncryptAndShareOperation(
            user: self.user,
            assetsDelegates: self.assetDelegates,
            threadsDelegates: self.threadsDelegates,
            limitPerRun: self.limit,
            imageManager: self.imageManager
        )
    }
    
    public override func content(ofQueueItem item: KBQueueItem) throws -> SHSerializableQueueItem {
        guard let data = item.content as? Data else {
            throw KBError.unexpectedData(item.content)
        }
        
        let unarchiver: NSKeyedUnarchiver
        if #available(macOS 10.13, *) {
            unarchiver = try NSKeyedUnarchiver(forReadingFrom: data)
        } else {
            unarchiver = NSKeyedUnarchiver(forReadingWith: data)
        }
        
        guard let uploadRequest = unarchiver.decodeObject(of: SHEncryptionForSharingRequestQueueItem.self, forKey: NSKeyedArchiveRootObjectKey) else {
            throw KBError.unexpectedData(item)
        }
        
        return uploadRequest
    }
    
    public override func markAsFailed(
        item: KBQueueItem,
        encryptionRequest request: SHEncryptionRequestQueueItem,
        globalIdentifier: String?,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        do {
            try self.markAsFailed(encryptionRequest: request, queueItem: item)
            completionHandler(.success(()))
        } catch {
            completionHandler(.failure(error))
        }
        
    }
    
    public func markAsFailed(
        encryptionRequest request: SHEncryptionRequestQueueItem,
        queueItem: KBQueueItem
    ) throws {
        let localIdentifier = request.localIdentifier
        let versions = request.versions
        let groupId = request.groupId
        let eventOriginator = request.eventOriginator
        let users = request.sharedWith
        
        do { _ = try BackgroundOperationQueue.of(type: .share).dequeue(item: queueItem) }
        catch {
            log.error("asset \(localIdentifier) failed to share but dequeueing from SHARE queue failed. Sharing will be attempted again")
            throw error
        }
        
        /// Enquque to failed
        log.info("enqueueing share request for asset \(localIdentifier) versions \(versions) in the FAILED queue")
        
        let failedShare = SHFailedShareRequestQueueItem(
            localIdentifier: localIdentifier,
            versions: versions,
            groupId: groupId,
            eventOriginator: eventOriginator,
            sharedWith: users,
            isBackground: request.isBackground
        )

        do {
            let failedShareQueue = try BackgroundOperationQueue.of(type: .failedShare)
            let successfulShareQueue = try BackgroundOperationQueue.of(type: .successfulShare)
            try failedShare.enqueue(in: failedShareQueue)
            
            /// Remove items in the `ShareHistoryQueue` for the same identifier
            let _ = try? successfulShareQueue.removeValues(forKeysMatching: KBGenericCondition(.equal, value: failedShare.identifier))
        }
        catch {
            log.fault("asset \(localIdentifier) failed to share but will never be recorded as 'failed to share' because enqueueing to SHARE FAILED queue failed: \(error.localizedDescription)")
            throw error
        }
        
        guard request.isBackground == false else {
            /// Avoid other side-effects for background  `SHEncryptionRequestQueueItem`
            return
        }
        
        /// Notify the delegates
        for delegate in assetDelegates {
            if let delegate = delegate as? SHAssetSharerDelegate {
                delegate.didFailSharing(queueItemIdentifier: failedShare.identifier)
            }
        }
    }
    
    public override func markAsSuccessful(
        item: KBQueueItem,
        encryptionRequest request: SHEncryptionRequestQueueItem,
        globalIdentifier: String
    ) throws {
        let localIdentifier = request.localIdentifier
        let versions = request.versions
        let groupId = request.groupId
        let eventOriginator = request.eventOriginator
        let users = request.sharedWith
        
        /// Dequeque from ShareQueue
        log.info("dequeueing request for asset \(localIdentifier) from the SHARE queue")
        
        do { _ = try BackgroundOperationQueue.of(type: .share).dequeue(item: item) }
        catch {
            log.warning("asset \(localIdentifier) was uploaded but dequeuing from UPLOAD queue failed, so this operation will be attempted again")
        }
        
        let successfulShare = SHShareHistoryItem(
            localAssetId: localIdentifier,
            globalAssetId: globalIdentifier,
            versions: versions,
            groupId: groupId,
            eventOriginator: eventOriginator,
            sharedWith: users,
            isBackground: request.isBackground
        )
        
        do {
            /// Enquque to ShareHistoryQueue
            log.info("SHARING succeeded. Enqueueing sharing upload request in the SUCCESS queue (upload history) for asset \(localIdentifier)")
            let failedShareQueue = try BackgroundOperationQueue.of(type: .failedShare)
            let successfulShareQueue = try BackgroundOperationQueue.of(type: .successfulShare)
            try successfulShare.enqueue(in: successfulShareQueue)
            
            /// Remove items in the `FailedShareQueue` for the same identifier
            let _ = try? failedShareQueue.removeValues(forKeysMatching: KBGenericCondition(.equal, value: successfulShare.identifier))
        }
        catch {
            log.fault("asset \(localIdentifier) was shared but will never be recorded as shared because enqueueing to SUCCESS queue failed")
            throw error
        }
        
        guard request.isBackground == false else {
            /// Avoid other side-effects for background  `SHEncryptionRequestQueueItem`
            return
        }
        
        /// Notify the delegates
        for delegate in assetDelegates {
            if let delegate = delegate as? SHAssetSharerDelegate {
                delegate.didCompleteSharing(queueItemIdentifier: successfulShare.identifier)
            }
        }
    }
    
    private func storeSecrets(
        request: SHEncryptionForSharingRequestQueueItem,
        globalIdentifier: String,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        self.user.shareableEncryptedAsset(
            globalIdentifier: globalIdentifier,
            versions: request.versions,
            recipients: request.sharedWith,
            groupId: request.groupId
        ) { result in
            switch result {
            case .failure(let error):
                completionHandler(.failure(error))
            case .success(let shareableEncryptedAsset):
                self.serverProxy.shareAssetLocally(
                    shareableEncryptedAsset,
                    completionHandler: completionHandler
                )
            }
        }
    }
    
    private func share(
        globalIdentifier: String,
        via request: SHEncryptionForSharingRequestQueueItem,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        self.serverProxy.getLocalSharingInfo(
            forAssetIdentifier: globalIdentifier,
            for: request.sharedWith
        ) { result in
            switch result {
            case .success(let shareableEncryptedAsset):
                guard let shareableEncryptedAsset = shareableEncryptedAsset else {
                    let error = SHBackgroundOperationError.fatalError("Asset sharing information wasn't stored as expected during the encrypt step")
                    completionHandler(.failure(error))
                    return
                }
                
                self.serverProxy.share(shareableEncryptedAsset) { shareResult in
                    if case .failure(let err) = shareResult {
                        completionHandler(.failure(err))
                        return
                    }
                    completionHandler(.success(()))
                }
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    private func initializeGroupAndThread(
        shareRequest: SHEncryptionForSharingRequestQueueItem,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        var errorInitializingGroup: Error? = nil
        var errorInitializingThread: Error? = nil
        let dispatchGroup = DispatchGroup()
        
        var usersAndSelf = shareRequest.sharedWith
        usersAndSelf.append(self.user)
        
        self.log.debug("creating or updating group for request \(shareRequest.identifier)")
        dispatchGroup.enter()
        self.interactionsController.setupGroupEncryptionDetails(
            groupId: shareRequest.groupId,
            with: usersAndSelf
        ) { initializeGroupResult in
            switch initializeGroupResult {
            case .failure(let error):
                errorInitializingGroup = error
            default: break
            }
            dispatchGroup.leave()
        }
        
        self.log.debug("creating or updating thread for request \(shareRequest.identifier)")
        dispatchGroup.enter()
        self.interactionsController.setupThread(
            with: usersAndSelf
        ) {
            setupThreadResult in
            switch setupThreadResult {
            case .success(let serverThread):
                let threadsDelegates = self.threadsDelegates
                self.delegatesQueue.async {
                    threadsDelegates.forEach({ $0.didUpdateThreadsList([serverThread])} )
                }
            case .failure(let error):
                errorInitializingThread = error
            }
            dispatchGroup.leave()
        }
        
        dispatchGroup.notify(queue: .global()) {
            guard errorInitializingGroup == nil,
                  errorInitializingThread == nil else {
                self.log.error("failed to initialize thread or group. \((errorInitializingGroup ?? errorInitializingThread!).localizedDescription)")
                // Mark as failed on any other error
                completionHandler(.failure(errorInitializingGroup ?? errorInitializingThread!))
                return
            }
            
            completionHandler(.success(()))
        }
    }
    
    private func process(
        _ item: KBQueueItem,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        let shareRequest: SHEncryptionForSharingRequestQueueItem
        
        do {
            let content = try content(ofQueueItem: item)
            guard let content = content as? SHEncryptionForSharingRequestQueueItem else {
                ///
                /// Delegates can't be called as item content can't be read and it will be silently removed from the queue
                ///
                log.error("unexpected data found in SHARE queue. Dequeueing")
                throw SHBackgroundOperationError.unexpectedData(item.content)
            }
            shareRequest = content
        } catch {
            do { _ = try BackgroundOperationQueue.of(type: .share).dequeue(item: item) }
            catch {
                log.fault("dequeuing failed of unexpected data in SHARE queue. \(error.localizedDescription)")
            }
            completionHandler(.failure(error))
            return
        }
        
        let handleError = { (error: Error) in
            self.log.critical("Error in SHARE asset: \(error.localizedDescription)")
            do {
                try self.markAsFailed(
                    encryptionRequest: shareRequest,
                    queueItem: item
                )
            } catch {
                self.log.critical("failed to mark SHARE as failed. This will likely cause infinite loops")
                // TODO: Handle
            }
            completionHandler(.failure(error))
        }
        
        guard shareRequest.sharedWith.count > 0 else {
            handleError(SHBackgroundOperationError.fatalError("empty sharing information in SHEncryptionForSharingRequestQueueItem object. SHEncryptAndShareOperation can only operate on sharing operations, which require user identifiers"))
            return
        }
        
        log.info("sharing it with users \(shareRequest.sharedWith.map { $0.identifier })")
        
        if shareRequest.isBackground == false {
            for delegate in assetDelegates {
                if let delegate = delegate as? SHAssetSharerDelegate {
                    delegate.didStartSharing(queueItemIdentifier: shareRequest.identifier)
                }
            }
        }
        
        let globalIdentifier: GlobalIdentifier

        do {
            ///
            /// At this point the global identifier should be calculated by the `SHLocalFetchOperation`,
            /// serialized and deserialized as part of the `SHApplePhotoAsset` object.
            ///
            globalIdentifier = try shareRequest.asset.retrieveOrGenerateGlobalIdentifier()
        } catch {
            handleError(error)
            return
        }
        
        ///
        /// Store sharing information in the local server proxy
        ///
        log.info("storing encryption secrets for asset \(globalIdentifier) for OTHER users in local server proxy")
        self.storeSecrets(request: shareRequest, globalIdentifier: globalIdentifier) {
            result in
            
            switch result {
            case .failure(let error):
                handleError(error)
                return
            case .success:
                self.log.info("successfully stored asset \(globalIdentifier) sharing information in local server proxy")
                
                ///
                /// Share using Safehill Server API
                ///
#if DEBUG
                guard ErrorSimulator.percentageShareFailures == 0
                        || arc4random() % (100 / ErrorSimulator.percentageShareFailures) != 0 else {
                    self.log.debug("simulating SHARE failure")
                    let error = SHBackgroundOperationError.fatalError("share failed")
                    handleError(error)
                    return
                }
#endif
                
                self.share(
                    globalIdentifier: globalIdentifier,
                    via: shareRequest
                ) { result in
                    if case .failure(let error) = result {
                        handleError(error)
                        return
                    }
                    
                    let handleSuccess = { (globalIdentifier: GlobalIdentifier) in
                        do {
                            try self.markAsSuccessful(
                                item: item,
                                encryptionRequest: shareRequest,
                                globalIdentifier: globalIdentifier
                            )
                        } catch {
                            self.log.critical("failed to mark SHARE as successful. This will likely cause infinite loops")
                            handleError(error)
                        }
                        completionHandler(.success(()))
                    }
                    
                    if shareRequest.isBackground == false {
                        self.initializeGroupAndThread(shareRequest: shareRequest) { result in
                            switch result {
                            case .failure(let error):
                                self.log.error("failed to initialize thread or group. \(error.localizedDescription)")
                                // Mark as failed on any other error
                                handleError(error)
                            case .success:
                                do {
                                    // Ingest into the graph
                                    try SHKGQuery.ingestShare(
                                        of: globalIdentifier,
                                        from: self.user.identifier,
                                        to: shareRequest.sharedWith.map({ $0.identifier })
                                    )
                                    handleSuccess(globalIdentifier)
                                } catch {
                                    handleError(error)
                                }
                            }
                        }
                    } else {
                        handleSuccess(globalIdentifier)
                    }
                }
            }
        }
    }
    
    public override func run(
        forQueueItemIdentifiers queueItemIdentifiers: [String],
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        let shareQueue: KBQueueStore
        
        do {
            shareQueue = try BackgroundOperationQueue.of(type: .share)
        } catch {
            log.critical("failed to read from SHARE queue. \(error.localizedDescription)")
            completionHandler(.failure(error))
            return
        }
        
        var queueItems = [KBQueueItem]()
        var error: Error? = nil
        let group = DispatchGroup()
        group.enter()
        shareQueue.retrieveItems(withIdentifiers: queueItemIdentifiers) {
            result in
            switch result {
            case .success(let items):
                queueItems = items
            case .failure(let err):
                error = err
            }
            group.leave()
        }
        
        group.notify(queue: .global()) {
            guard error == nil else {
                self.log.critical("failed to retrieve items from SHARE queue. \(error!.localizedDescription)")
                completionHandler(.failure(error!))
                return
            }
            
            for item in queueItems {
                group.enter()
                self.runOnce(for: item) { _ in
                    group.leave()
                }
            }
            
            group.notify(queue: .global()) {
                completionHandler(.success(()))
            }
        }
    }
    
    override func runOnce(
        for item: KBQueueItem,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        guard processingState(for: item.identifier) != .sharing else {
            completionHandler(.failure(SHBackgroundOperationError.alreadyProcessed))
            return
        }
        
        let shareQueue: KBQueueStore
        do {
            shareQueue = try BackgroundOperationQueue.of(type: .share)
        } catch {
            log.critical("failed to read from SHARE queue. \(error.localizedDescription)")
            completionHandler(.failure(error))
            return
        }
        
        setProcessingState(.sharing, for: item.identifier)
        
        /// Check the item still exists in the queue
        /// Because it was retrieved earlier it might already have been processed by a competing process
        shareQueue.retrieveItem(withIdentifier: item.identifier) { result in
            switch result {
            case .success(let queuedItem):
                guard let queuedItem else {
                    setProcessingState(nil, for: item.identifier)
                    completionHandler(.success(()))
                    return
                }
                
                self.log.info("sharing item \(queuedItem.identifier) created at \(queuedItem.createdAt)")
                
                self.process(queuedItem) { result in
                    if case .success = result {
                        self.log.info("[√] share task completed for item \(queuedItem.identifier)")
                    } else {
                        self.log.error("[x] share task failed for item \(queuedItem.identifier)")
                    }
                    
                    setProcessingState(nil, for: queuedItem.identifier)
                    completionHandler(result)
                }
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    public override func runOnce(
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        let shareQueue: KBQueueStore
        do {
            shareQueue = try BackgroundOperationQueue.of(type: .share)
        } catch {
            log.critical("failed to read from SHARE queue. \(error.localizedDescription)")
            completionHandler(.failure(error))
            return
        }
        
        var queueItems = [KBQueueItem]()
        var error: Error? = nil
        let group = DispatchGroup()
        group.enter()
        
        let interval = DateInterval(start: Date.distantPast, end: Date())
        shareQueue.peekItems(
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
        
        group.notify(queue: .global()) {
            guard error == nil else {
                self.log.critical("failed to retrieve items from SHARE queue. \(error!.localizedDescription)")
                completionHandler(.failure(error!))
                return
            }
            
            var count = 0
            
            for item in queueItems {
                count += 1
                
                group.enter()
                self.runOnce(for: item) { _ in
                    group.leave()
                }
            }
            
            group.notify(queue: .global()) {
                completionHandler(.success(()))
            }
            
            self.log.info("started \(count) SHARE operations")
        }
    }
}

internal class SHAssetEncryptAndShareQueueProcessor : SHBackgroundOperationProcessor<SHEncryptAndShareOperation> {
    /// Singleton (with private initializer)
    public static var shared = SHAssetEncryptAndShareQueueProcessor(
        delayedStartInSeconds: 5,
        dispatchIntervalInSeconds: 2
    )
    
    private override init(delayedStartInSeconds: Int = 0,
                          dispatchIntervalInSeconds: Int? = nil) {
        super.init(delayedStartInSeconds: delayedStartInSeconds, dispatchIntervalInSeconds: dispatchIntervalInSeconds)
    }
}
