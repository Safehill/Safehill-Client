import Foundation
import os
import KnowledgeBase

open class SHUploadOperation: SHAbstractBackgroundOperation, SHBackgroundQueueProcessorOperationProtocol {
    
    public let log = Logger(subsystem: "com.gf.safehill", category: "BG-UPLOAD")
    
    public let limit: Int
    public let user: SHLocalUser
    public var delegates: [SHOutboundAssetOperationDelegate]
    
    public init(user: SHLocalUser,
                delegates: [SHOutboundAssetOperationDelegate],
                limitPerRun limit: Int) {
        self.user = user
        self.limit = limit
        self.delegates = delegates
    }
    
    public var serverProxy: SHServerProxy {
        SHServerProxy(user: self.user)
    }
    
    public func clone() -> SHBackgroundOperationProtocol {
        SHUploadOperation(user: self.user,
                          delegates: self.delegates,
                          limitPerRun: self.limit)
    }
    
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
        
        guard let uploadRequest = unarchiver.decodeObject(of: SHUploadRequestQueueItem.self, forKey: NSKeyedArchiveRootObjectKey) else {
            throw SHBackgroundOperationError.unexpectedData(data)
        }
        
        return uploadRequest
    }
    
    private func markLocalAssetAsFailed(globalIdentifier: String) throws {
        let group = DispatchGroup()
        for quality in SHAssetQuality.all {
            group.enter()
            self.serverProxy.localServer.markAsset(with: globalIdentifier, quality: quality, as: .failed) { result in
                if case .failure(let err) = result {
                    self.log.info("failed to mark local asset \(globalIdentifier) as failed: \(err.localizedDescription)")
                }
                group.leave()
            }
        }
        
        let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds))
        
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
    }
    
    public func markAsFailed(item: KBQueueItem,
                             localIdentifier: String,
                             globalIdentifier: String,
                             groupId: String,
                             eventOriginator: SHServerUser,
                             sharedWith: [SHServerUser],
                             error: Error) throws {
        ///
        /// Dequeque from UploadQueue
        ///
        log.info("dequeueing request for asset \(localIdentifier) from the UPLOAD queue")
        
        do { _ = try UploadQueue.dequeue(item: item) }
        catch {
            log.error("asset \(localIdentifier) failed to upload but dequeuing from UPLOAD queue failed, so this operation will be attempted again.")
            throw error
        }
        
#if DEBUG
        log.debug("items in the UPLOAD queue after dequeueing \((try? UploadQueue.peekNext(100))?.count ?? 0)")
#endif
        
        ///
        /// Enquque to FailedUpload queue
        ///
        log.info("enqueueing upload request for asset \(localIdentifier) in the FAILED queue")
        let failedUploadQueueItem = SHFailedUploadRequestQueueItem(localIdentifier: localIdentifier,
                                                                   groupId: groupId,
                                                                   eventOriginator: eventOriginator,
                                                                   sharedWith: sharedWith)
        
        do {
            try self.markLocalAssetAsFailed(globalIdentifier: globalIdentifier)
            try failedUploadQueueItem.enqueue(in: FailedUploadQueue, with: localIdentifier)
        }
        catch {
            log.fault("asset \(localIdentifier) failed to upload but will never be recorded as failed because enqueueing to FAILED queue failed: \(error.localizedDescription)")
            throw error
        }
        
        // Notify the delegates
        for delegate in delegates {
            if let delegate = delegate as? SHAssetUploaderDelegate {
                delegate.didFailUpload(
                    itemWithLocalIdentifier: localIdentifier,
                    globalIdentifier: globalIdentifier,
                    groupId: groupId,
                    sharedWith: sharedWith,
                    error: error
                )
            }
        }
    }
    
    private func shouldShareItToo(_ sharedWith: [SHServerUser]) -> Bool {
        return sharedWith.count > 0 || (sharedWith.count == 1 && sharedWith.first!.identifier == self.user.identifier)
    }
    
    public func markAsSuccessful(
        item: KBQueueItem,
        uploadRequest: SHUploadRequestQueueItem
    ) throws {
        let localIdentifier = uploadRequest.assetId
        let globalIdentifier = uploadRequest.globalAssetId
        let groupId = uploadRequest.groupId
        let eventOriginator = uploadRequest.eventOriginator
        let sharedWith = uploadRequest.sharedWith
        
        ///
        /// Dequeue from Upload queue
        ///
        log.info("dequeueing item \(item.identifier) from the UPLOAD queue")
        
        do { _ = try UploadQueue.dequeue(item: item) }
        catch {
            log.warning("item \(item.identifier) was completed but dequeuing from UPLOAD queue failed. This task will be attempted again")
        }
#if DEBUG
        log.debug("items in the UPLOAD queue after dequeueing \((try? UploadQueue.peekNext(100))?.count ?? 0)")
#endif
        
        ///
        /// Enquque to success history
        ///
        log.info("UPLOAD succeeded. Enqueueing upload request in the SUCCESS queue (upload history) for asset \(globalIdentifier)")
        
        let succesfulUploadQueueItem = SHUploadHistoryItem(localIdentifier: localIdentifier,
                                                           groupId: groupId,
                                                           eventOriginator: eventOriginator,
                                                           sharedWith: [self.user])
        
        do { try succesfulUploadQueueItem.enqueue(in: UploadHistoryQueue, with: localIdentifier) }
        catch {
            log.fault("asset \(localIdentifier) was upload but will never be recorded as uploaded because enqueueing to SUCCESS queue failed")
            throw error
        }
        
        do { try uploadRequest.enqueue(in: HiResUploadQueue, with: localIdentifier) }
        catch {
            log.fault("asset \(localIdentifier) was upload but the hi resolution will not be uploaded because enqueueing to HIRESUPLOAD queue failed")
            throw error
        }
        
        ///
        /// Start the sharing part if needed
        ///
        if shouldShareItToo(sharedWith) {
            ///
            /// Enquque to FETCH queue (fetch is needed for encrypting for sharing)
            ///
            log.info("enqueueing upload request in the FETCH+SHARE queue for asset \(localIdentifier)")

            let fetchRequest = SHLocalFetchRequestQueueItem(
                localIdentifier: localIdentifier,
                groupId: groupId,
                eventOriginator: eventOriginator,
                sharedWith: sharedWith,
                shouldUpload: false
            )

            do { try fetchRequest.enqueue(in: FetchQueue, with: localIdentifier) }
            catch {
                log.fault("asset \(localIdentifier) was uploaded but will never be shared because enqueueing to FETCH queue failed")
                throw error
            }
        }
        
        ///
        /// Notify the delegates
        ///
        for delegate in delegates {
            if let delegate = delegate as? SHAssetUploaderDelegate {
                delegate.didCompleteUpload(
                    itemWithLocalIdentifier: localIdentifier,
                    globalIdentifier: globalIdentifier,
                    groupId: groupId
                )
            }
        }
    }
    
    ///
    /// Best attempt to remove the same item from the any other queue in the same pipeline
    ///
    private func tryRemoveExistingQueueItems(with localIdentifier: String) {
        for queue in [UploadHistoryQueue, FailedUploadQueue] {
            let condition = KBGenericCondition(.equal, value: localIdentifier)
            let _ = try? queue.removeValues(forKeysMatching: condition)
        }
    }
    
    private func process(_ item: KBQueueItem) throws {
        
        let uploadRequest: SHUploadRequestQueueItem
        
        do {
            let content = try content(ofQueueItem: item)
            guard let content = content as? SHUploadRequestQueueItem else {
                log.error("unexpected data found in UPLOAD queue. Dequeueing")
                // Delegates can't be called as item content can't be read and it will be silently removed from the queue
                throw SHBackgroundOperationError.unexpectedData(item.content)
            }
            uploadRequest = content
        } catch {
            do { _ = try UploadQueue.dequeue(item: item) }
            catch {
                log.warning("dequeuing failed of unexpected data in UPLOAD queue. This task will be attempted again.")
            }
            throw error
        }
        
        let globalIdentifier = uploadRequest.globalAssetId
        let localIdentifier = uploadRequest.assetId
        
        do {
            self.tryRemoveExistingQueueItems(with: localIdentifier)
            
            for delegate in delegates {
                if let delegate = delegate as? SHAssetUploaderDelegate {
                    delegate.didStartUpload(
                        itemWithLocalIdentifier: localIdentifier,
                        globalIdentifier: globalIdentifier,
                        groupId: uploadRequest.groupId
                    )
                }
            }
            
            /// Upload low and mid resolution.
            /// Once successful, enqueue hi resolution upload in the HiResUploadQueue
            let versions: [SHAssetQuality] = [
                .lowResolution,
                .midResolution
            ]
            
            log.info("retrieving encrypted asset from local server proxy: \(globalIdentifier)")
            let encryptedAsset: any SHEncryptedAsset
            do {
                encryptedAsset = try SHLocalAssetStoreController(user: self.user)
                    .encryptedAsset(
                        with: globalIdentifier,
                        versions: versions
                    )
            } catch {
                log.error("failed to retrieve local server asset for localIdentifier \(localIdentifier): \(error.localizedDescription).")
                throw SHBackgroundOperationError.fatalError("failed to retrieved encrypted asset from local server")
            }
            
            guard globalIdentifier == encryptedAsset.globalIdentifier else {
                throw SHBackgroundOperationError.globalIdentifierDisagreement(localIdentifier)
            }
            
#if DEBUG
            guard kSHSimulateBackgroundOperationFailures == false || arc4random() % 20 != 0 else {
                log.debug("simulating CREATE ASSET failure")
                throw SHBackgroundOperationError.fatalError("failed to create server asset")
            }
#endif
            let serverAsset: SHServerAsset
            do {
                serverAsset = try SHAssetStoreController(user: self.user)
                    .upload(
                        asset: encryptedAsset,
                        with: uploadRequest.groupId,
                        filterVersions: versions
                    )
            } catch {
                log.error("failed to upload asset for item with localIdentifier \(localIdentifier). Dequeueing item, as to let the user control the retry. error=\(error.localizedDescription)")
                throw SHBackgroundOperationError.fatalError("failed to create server asset or upload asset to the CDN")
            }
            
            guard globalIdentifier == serverAsset.globalIdentifier else {
                throw SHBackgroundOperationError.globalIdentifierDisagreement(localIdentifier)
            }
        } catch {
            do {
                try self.markAsFailed(item: item,
                                      localIdentifier: localIdentifier,
                                      globalIdentifier: globalIdentifier,
                                      groupId: uploadRequest.groupId,
                                      eventOriginator: uploadRequest.eventOriginator,
                                      sharedWith: uploadRequest.sharedWith,
                                      error: error)
            } catch {
                log.critical("failed to mark UPLOAD as failed. This will likely cause infinite loops")
                // TODO: Handle
            }
            
            throw error
        }

        ///
        /// Upload is completed.
        /// Create an item in the history queue for this upload, and remove the one in the upload queue
        ///
        do {
            try self.markAsSuccessful(
                item: item,
                uploadRequest: uploadRequest
            )
        } catch {
            log.critical("failed to mark UPLOAD as successful. This will likely cause infinite loops")
            // TODO: Handle
        }
    }
    
    public func runOnce() throws {
        while let item = try UploadQueue.peek() {
            guard processingState(for: item.identifier) != .uploading else {
                break
            }
            
            log.info("uploading item \(item.identifier) created at \(item.createdAt)")
            
            setProcessingState(.uploading, for: item.identifier)
            
            do {
                try self.process(item)
                log.info("[√] upload task completed for item \(item.identifier)")
            } catch {
                log.error("[x] upload task failed for item \(item.identifier): \(error.localizedDescription)")
            }
            
            setProcessingState(nil, for: item.identifier)
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
            items = try UploadQueue.peekNext(self.limit)
        } catch {
            log.error("failed to fetch items from the UPLOAD queue")
            state = .finished
            return
        }
        
        for item in items {
            guard processingState(for: item.identifier) != .uploading else {
                break
            }
            
            log.info("uploading item \(item.identifier) created at \(item.createdAt)")
            
            setProcessingState(.uploading, for: item.identifier)
            
            DispatchQueue.global(qos: .background).async { [self] in
                guard !isCancelled else {
                    log.info("upload task cancelled. Finishing")
                    setProcessingState(nil, for: item.identifier)
                    return
                }
                do {
                    try self.process(item)
                    log.info("[√] upload task completed for item \(item.identifier)")
                } catch {
                    log.error("[x] upload task failed for item \(item.identifier): \(error.localizedDescription)")
                }
                
                setProcessingState(nil, for: item.identifier)
            }
            
            guard !isCancelled else {
                log.info("upload task cancelled. Finishing")
                break
            }
        }
        
        state = .finished
    }
}

public class SHAssetsUploaderQueueProcessor : SHBackgroundOperationProcessor<SHUploadOperation> {
    /// Singleton (with private initializer)
    public static var shared = SHAssetsUploaderQueueProcessor(
        delayedStartInSeconds: 4,
        dispatchIntervalInSeconds: 2
    )
    
    private override init(delayedStartInSeconds: Int = 0,
                          dispatchIntervalInSeconds: Int? = nil) {
        super.init(delayedStartInSeconds: delayedStartInSeconds,
                   dispatchIntervalInSeconds: dispatchIntervalInSeconds)
    }
}
