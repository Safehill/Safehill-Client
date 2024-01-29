import Foundation
import os
import KnowledgeBase


open class SHEncryptAndShareOperation: SHEncryptionOperation {
    
    public override var log: Logger {
        Logger(subsystem: "com.gf.safehill", category: "BG-SHARE")
    }
    
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
        globalIdentifier: String?
    ) throws
    {
        try self.markAsFailed(encryptionRequest: request, queueItem: item)
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
        globalIdentifier: String
    ) throws {
        let asset = request.asset
        
        let shareableEncryptedAsset = try asset.shareableEncryptedAsset(
            globalIdentifier: globalIdentifier,
            versions: request.versions,
            sender: self.user,
            recipients: request.sharedWith,
            groupId: request.groupId
        )
        
        var error: Error? = nil
        let semaphore = DispatchSemaphore(value: 0)
        
        serverProxy.shareAssetLocally(shareableEncryptedAsset) { result in
            if case .failure(let err) = result {
                error = err
            }
            semaphore.signal()
        }
        
        let dispatchResult = semaphore.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds))
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
        guard error == nil else {
            throw error!
        }
    }
    
    private func share(
        globalIdentifier: String,
        via request: SHEncryptionForSharingRequestQueueItem
    ) throws {
        var error: Error? = nil
        let group = DispatchGroup()
        group.enter()
        self.serverProxy.getLocalSharingInfo(
            forAssetIdentifier: globalIdentifier,
            for: request.sharedWith
        ) { result in
            switch result {
            case .success(let shareableEncryptedAsset):
                guard let shareableEncryptedAsset = shareableEncryptedAsset else {
                    error = SHBackgroundOperationError.fatalError("Asset sharing information wasn't stored as expected during the encrypt step")
                    group.leave()
                    return
                }
                self.serverProxy.share(shareableEncryptedAsset) { shareResult in
                    if case .failure(let err) = shareResult {
                        error = err
                    }
                    group.leave()
                }
            case .failure(let err):
                error = err
                group.leave()
            }
        }
        
        let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds))
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
        guard error == nil else {
            throw error!
        }
    }
    
    private func process(_ item: KBQueueItem) {
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
            return
        }

        do {
            ///
            /// At this point the global identifier should be calculated by the `SHLocalFetchOperation`,
            /// serialized and deserialized as part of the `SHApplePhotoAsset` object.
            ///
            let globalIdentifier = try shareRequest.asset.retrieveOrGenerateGlobalIdentifier()
            
            guard shareRequest.sharedWith.count > 0 else {
                log.error("empty sharing information in SHEncryptionForSharingRequestQueueItem object. SHEncryptAndShareOperation can only operate on sharing operations, which require user identifiers")
                fatalError("sharingWith emtpy. No sharing info")
            }
            
            log.info("sharing it with users \(shareRequest.sharedWith.map { $0.identifier })")
            
            if shareRequest.isBackground == false {
                for delegate in assetDelegates {
                    if let delegate = delegate as? SHAssetSharerDelegate {
                        delegate.didStartSharing(queueItemIdentifier: shareRequest.identifier)
                    }
                }
            }
            
            ///
            /// Store sharing information in the local server proxy
            ///
            do {
                log.info("storing encryption secrets for asset \(globalIdentifier) for OTHER users in local server proxy")
                
                try self.storeSecrets(request: shareRequest, globalIdentifier: globalIdentifier)
                
                log.info("successfully stored asset \(globalIdentifier) sharing information in local server proxy")
            } catch {
                log.error("failed to locally share encrypted item \(item.identifier) with users \(shareRequest.sharedWith.map { $0.identifier }): \(error.localizedDescription)")
                throw SHBackgroundOperationError.fatalError("failed to store secrets")
            }
            
            ///
            /// Share using Safehill Server API
            ///
#if DEBUG
            guard ErrorSimulator.percentageShareFailures == 0
                    || arc4random() % (100 / ErrorSimulator.percentageShareFailures) != 0 else {
                log.debug("simulating SHARE failure")
                throw SHBackgroundOperationError.fatalError("share failed")
            }
#endif
            
            do {
                try self.share(globalIdentifier: globalIdentifier, via: shareRequest)
            } catch {
                log.error("failed to share with users \(shareRequest.sharedWith.map { $0.identifier })")
                throw SHBackgroundOperationError.fatalError("share failed")
            }
            
            if shareRequest.isBackground == false {
                var errorInitializingGroup: Error? = nil
                var errorInitializingThread: Error? = nil
                let dispatchGroup = DispatchGroup()
                
                let interactionsController = SHUserInteractionController(
                    user: self.user,
                    protocolSalt: self.user.encryptionProtocolSalt!
                )
                dispatchGroup.enter()
                interactionsController.setupGroupEncryptionDetails(
                    groupId: shareRequest.groupId,
                    with: shareRequest.sharedWith,
                    completionHandler: { initializeGroupResult in
                        switch initializeGroupResult {
                        case .failure(let error):
                            errorInitializingGroup = error
                        default: break
                        }
                        dispatchGroup.leave()
                    }
                )
                
                dispatchGroup.enter()
                interactionsController.setupThread(
                    with: shareRequest.sharedWith
                ) { 
                    setupThreadResult in
                    switch setupThreadResult {
                    case .success(let serverThread):
                        self.threadsDelegates.forEach({ $0.threadsWereUpdated([serverThread])} )
                    case .failure(let error):
                        errorInitializingThread = error
                    default: break
                    }
                    dispatchGroup.leave()
                }
                
                let dispatchResult = dispatchGroup.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds))
                guard dispatchResult == .success else {
                    // Retry (by not dequeueing) on timeout
                    throw SHBackgroundOperationError.timedOut
                }
                guard errorInitializingGroup == nil,
                      errorInitializingThread == nil else {
                    // Mark as failed on any other error
                    throw errorInitializingGroup ?? errorInitializingThread!
                }
                
                // Ingest into the graph
                try SHKGQuery.ingestShare(
                    of: globalIdentifier,
                    from: self.user.identifier,
                    to: shareRequest.sharedWith.map({ $0.identifier })
                )
            }
            
            do {
                try self.markAsSuccessful(
                    item: item,
                    encryptionRequest: shareRequest,
                    globalIdentifier: globalIdentifier
                )
            } catch {
                log.critical("failed to mark SHARE as successful. This will likely cause infinite loops")
                throw error
            }
        } catch {
            log.critical("Error in SHARE asset: \(error.localizedDescription)")
            do {
                try self.markAsFailed(
                    encryptionRequest: shareRequest,
                    queueItem: item
                )
            } catch {
                log.critical("failed to mark SHARE as failed. This will likely cause infinite loops")
                // TODO: Handle
            }
        }
    }
    
    public override func run(forQueueItemIdentifiers queueItemIdentifiers: [String]) throws {
        let shareQueue = try BackgroundOperationQueue.of(type: .share)
        
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
        
        let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds))
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
        guard error == nil else {
            throw error!
        }
        
        for item in queueItems {
            guard processingState(for: item.identifier) != .sharing else {
                continue
            }
            
            log.info("sharing item \(item.identifier) created at \(item.createdAt)")
            
            setProcessingState(.sharing, for: item.identifier)
            
            self.process(item)
            log.info("[√] share task completed for item \(item.identifier)")
            
            setProcessingState(nil, for: item.identifier)
        }
    }
    
    public override func runOnce(maxItems: Int? = nil) throws {
        var count = 0
        let queue = try BackgroundOperationQueue.of(type: .share)
        
        while let item = try queue.peek() {
            guard maxItems == nil || count < maxItems! else {
                break
            }
            guard processingState(for: item.identifier) != .sharing else {
                continue
            }
            
            log.info("sharing item \(item.identifier) created at \(item.createdAt)")
            
            setProcessingState(.sharing, for: item.identifier)
            
            self.process(item)
            log.info("[√] share task completed for item \(item.identifier)")
            
            setProcessingState(nil, for: item.identifier)
            
            count += 1
        }
    }
    
    public override func main() {
        guard !self.isCancelled else {
            state = .finished
            return
        }
        
        state = .executing
        
        let items: [KBQueueItem]
        
        do {
            items = try BackgroundOperationQueue.of(type: .share).peekNext(self.limit)
        } catch {
            log.error("failed to fetch items from the ENCRYPT queue")
            state = .finished
            return
        }
        
        for item in items {
            guard processingState(for: item.identifier) != .sharing else {
                break
            }
            
            log.info("sharing item \(item.identifier) created at \(item.createdAt)")
            
            setProcessingState(.sharing, for: item.identifier)
            
            DispatchQueue.global().async { [self] in
                guard !isCancelled else {
                    log.info("share task cancelled. Finishing")
                    setProcessingState(nil, for: item.identifier)
                    return
                }
                
                self.process(item)
                log.info("[√] share task completed for item \(item.identifier)")
                
                setProcessingState(nil, for: item.identifier)
            }
            
            guard !self.isCancelled else {
                log.info("share task cancelled. Finishing")
                break
            }
        }
        
        state = .finished
    }
}

public class SHAssetEncryptAndShareQueueProcessor : SHBackgroundOperationProcessor<SHEncryptAndShareOperation> {
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
