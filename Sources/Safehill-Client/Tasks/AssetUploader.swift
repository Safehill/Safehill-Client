import Foundation
import os
import KnowledgeBase

open class SHUploadOperation: SHAbstractBackgroundOperation, SHBackgroundQueueProcessorOperationProtocol {
    
    public let log = Logger(subsystem: "com.gf.safehill", category: "BG-UPLOAD")
    
    public let limit: Int
    public let user: SHLocalUser
    public var delegates: [SHOutboundAssetOperationDelegate]
    
    var queue: KBQueueStore {
        UploadQueue
    }
    
    public init(user: SHLocalUser,
                delegates: [SHOutboundAssetOperationDelegate],
                limitPerRun limit: Int) {
        self.user = user
        self.limit = limit
        self.delegates = delegates
    }
    
    private var serverProxy: SHServerProxy {
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
    
    private func markLocalAssetAsFailed(globalIdentifier: String, versions: [SHAssetQuality]) throws {
        let group = DispatchGroup()
        for quality in versions {
            group.enter()
            self.serverProxy.localServer.markAsset(with: globalIdentifier, quality: quality, as: .failed) { result in
                if case .failure(let err) = result {
                    if case SHAssetStoreError.noEntries = err {
                        self.log.error("No entries found when trying to update local asset upload state for \(globalIdentifier)::\(quality.rawValue)")
                    }
                    self.log.info("failed to mark local asset \(globalIdentifier) as failed in local server: \(err.localizedDescription)")
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
                             uploadRequest request: SHUploadRequestQueueItem,
                             error: Error) throws {
        let localIdentifier = request.localIdentifier
        let globalIdentifier = request.globalAssetId
        let versions = request.versions
        let groupId = request.groupId
        let eventOriginator = request.eventOriginator
        let sharedWith = request.sharedWith
        
        /// Dequeque from UploadQueue
        log.info("dequeueing request for asset \(localIdentifier) from the UPLOAD queue")
        
        do { _ = try self.queue.dequeue(item: item) }
        catch {
            log.error("asset \(localIdentifier) failed to upload but dequeuing from UPLOAD queue failed, so this operation will be attempted again.")
            throw error
        }
        
#if DEBUG
        log.debug("items in the UPLOAD queue after dequeueing \((try? self.queue.peekNext(100))?.count ?? 0)")
#endif
        
        guard request.isBackground == false else {
            /// Avoid other side-effects for background  `SHUploadRequestQueueItem`
            return
        }
        
        /// Enquque to FailedUpload queue
        log.info("enqueueing upload request for asset \(localIdentifier) in the FAILED queue")
        let queueItemIdentifier = SHUploadPipeline.uploadQueueItemKey(
            groupId: groupId,
            assetLocalIdentifier: localIdentifier
        )
        let failedUploadQueueItem = SHFailedUploadRequestQueueItem(
            localIdentifier: localIdentifier,
            versions: versions,
            groupId: groupId,
            eventOriginator: eventOriginator,
            sharedWith: sharedWith
        )
        
        do {
            try self.markLocalAssetAsFailed(globalIdentifier: globalIdentifier, versions: versions ?? SHAssetQuality.all)
            try failedUploadQueueItem.enqueue(in: FailedUploadQueue, with: queueItemIdentifier)
            
            /// Remove items in the `UploadHistoryQueue` and `FailedUploadQueue` for the same asset
            /// This ensures that the queue stays clean and both are in sync (self-healing)
            let baseCondition = KBGenericCondition(.beginsWith, value: localIdentifier)
            let _ = try FailedUploadQueue.removeValues(forKeysMatching: baseCondition.and(KBGenericCondition(.equal, value: queueItemIdentifier, negated: true)))
            let _ = try UploadHistoryQueue.removeValues(forKeysMatching: baseCondition)
        }
        catch {
            log.fault("asset \(localIdentifier) failed to upload but will never be recorded as failed because enqueueing to FAILED queue failed: \(error.localizedDescription)")
            throw error
        }
        
        /// Notify the delegates
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
    
    public func markAsSuccessful(
        item: KBQueueItem,
        uploadRequest request: SHUploadRequestQueueItem
    ) throws {
        let localIdentifier = request.localIdentifier
        let globalIdentifier = request.globalAssetId
        let versions = request.versions
        let groupId = request.groupId
        let eventOriginator = request.eventOriginator
        let sharedWith = request.sharedWith
        let isBackground = request.isBackground
        
        /// Dequeue from Upload queue
        log.info("dequeueing item \(item.identifier) from the UPLOAD queue")
        
        do { _ = try self.queue.dequeue(item: item) }
        catch {
            log.warning("item \(item.identifier) was completed but dequeuing from UPLOAD queue failed. This task will be attempted again")
            throw error
        }
#if DEBUG
        log.debug("items in the UPLOAD queue after dequeueing \((try? self.queue.peekNext(100))?.count ?? 0)")
#endif
        
        if isBackground == false {
            /// Enqueue to success history
            log.info("UPLOAD succeeded. Enqueueing upload request in the SUCCESS queue (upload history) for asset \(globalIdentifier)")
            
            let uploadedQueueItemIdentifier = SHUploadPipeline.uploadQueueItemKey(
                groupId: groupId,
                assetLocalIdentifier: localIdentifier
            )
            let succesfulUploadQueueItem = SHUploadHistoryItem(
                localIdentifier: localIdentifier,
                versions: versions,
                groupId: groupId,
                eventOriginator: eventOriginator,
                sharedWith: [self.user]
            )
            
            do {
                try succesfulUploadQueueItem.enqueue(in: UploadHistoryQueue, with: uploadedQueueItemIdentifier)
                
                /// Remove items in the `UploadHistoryQueue` and `FailedUploadQueue` for the same asset
                /// This is necessary as we don't want duplicates when uploading an asset multiple times (for whatever reason)
                let baseCondition = KBGenericCondition(.beginsWith, value: localIdentifier)
                let _ = try UploadHistoryQueue.removeValues(forKeysMatching: baseCondition.and(KBGenericCondition(.equal, value: uploadedQueueItemIdentifier, negated: true)))
                let _ = try FailedUploadQueue.removeValues(forKeysMatching: baseCondition)
            }
            catch {
                log.fault("asset \(localIdentifier) was upload but will never be recorded as uploaded because enqueueing to SUCCESS queue failed")
                throw error
            }
        }
        
        ///
        /// Start the sharing part if needed
        ///
        if request.isSharingWithOtherUsers {
            ///
            /// Enquque to FETCH queue for encrypting for sharing (note: `shouldUpload=false`)
            ///
            log.info("enqueueing upload request in the FETCH+SHARE queue for asset \(localIdentifier) versions \(versions ?? []) isBackground=\(isBackground)")

            let shareFetchQueueItemIdentifier = SHUploadPipeline.shareQueueItemKey(
                groupId: groupId,
                assetId: localIdentifier,
                users: sharedWith
            )
            let fetchRequest = SHLocalFetchRequestQueueItem(
                localIdentifier: localIdentifier,
                versions: versions,
                groupId: groupId,
                eventOriginator: eventOriginator,
                sharedWith: sharedWith,
                shouldUpload: false,
                isBackground: isBackground
            )
            do { try fetchRequest.enqueue(in: FetchQueue, with: shareFetchQueueItemIdentifier) }
            catch {
                log.fault("asset \(localIdentifier) was uploaded but will never be shared because enqueueing to FETCH queue failed")
                throw error
            }
            
            if request.versions?.contains(.hiResolution) == false,
               isBackground == false {
                ///
                /// Enquque to FETCH queue cause for sharing we only upload the `.midResolution` version so far.
                /// `.hiResolution` will be uploaded via this operation (note: `versions=[.hiResolution]`, `isBackground=true` and `shouldUpload=true`).
                /// Avoid unintentional recursion by not having a background request calling another background request.
                ///
                /// NOTE: This is only necessary when the user shares assets, because in that case `.lowResolution` and `.midResolution` are uploaded first, and `.hiResolution` later
                /// When assets are only backed up, there's no `.midResolution` used as a surrogate.
                ///
                let fetchQueueItem = SHLocalFetchRequestQueueItem(
                    localIdentifier: request.localIdentifier,
                    versions: [.hiResolution],
                    groupId: request.groupId,
                    eventOriginator: request.eventOriginator,
                    sharedWith: request.sharedWith,
                    shouldUpload: true,
                    isBackground: true
                )
                do {
                    let hiVersionQueueItemIdentifier = SHUploadPipeline.hiResUploadQueueItemKey(groupId: groupId, assetLocalIdentifier: request.localIdentifier)
                    try fetchQueueItem.enqueue(in: FetchQueue, with: hiVersionQueueItemIdentifier)
                    log.info("enqueueing asset \(localIdentifier) HI RESOLUTION for upload")
                }
                catch {
                    log.fault("asset \(localIdentifier) was upload but the hi resolution will not be uploaded because enqueueing to FETCH queue failed")
                    throw error
                }
            }
        }
        
        guard isBackground == false else {
            /// Avoid other side-effects for background  `SHUploadRequestQueueItem`
            return
        }
        
        /// Notify the delegates
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
    
    func process(_ item: KBQueueItem) throws {
        
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
            do { _ = try self.queue.dequeue(item: item) }
            catch {
                log.warning("dequeuing failed of unexpected data in UPLOAD queue. This task will be attempted again.")
            }
            throw error
        }
        
        let globalIdentifier = uploadRequest.globalAssetId
        let localIdentifier = uploadRequest.localIdentifier
        
        do {
            if uploadRequest.isBackground == false {
                for delegate in delegates {
                    if let delegate = delegate as? SHAssetUploaderDelegate {
                        delegate.didStartUpload(
                            itemWithLocalIdentifier: localIdentifier,
                            globalIdentifier: globalIdentifier,
                            groupId: uploadRequest.groupId
                        )
                    }
                }
            }
            
            let versions = uploadRequest.versions ?? SHUploadPipeline.defaultVersions(for: uploadRequest)
            
            log.info("retrieving encrypted asset from local server proxy: \(globalIdentifier) versions=\(versions)")
            let encryptedAsset: any SHEncryptedAsset
            do {
                encryptedAsset = try SHLocalAssetStoreController(user: self.user)
                    .encryptedAsset(
                        with: globalIdentifier,
                        versions: versions,
                        cacheHiResolution: false
                    )
            } catch {
                log.error("failed to retrieve local server asset for localIdentifier \(localIdentifier): \(error.localizedDescription).")
                throw SHBackgroundOperationError.missingAssetInLocalServer(globalIdentifier)
            }
            
            guard globalIdentifier == encryptedAsset.globalIdentifier else {
                throw SHBackgroundOperationError.globalIdentifierDisagreement(localIdentifier)
            }
            
#if DEBUG
            guard ErrorSimulator.percentageUploadFailures == 0
                  || arc4random() % (100 / ErrorSimulator.percentageUploadFailures) != 0 else {
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
                                      uploadRequest: uploadRequest,
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
        while let item = try self.queue.peek() {
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
            items = try self.queue.peekNext(self.limit)
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
