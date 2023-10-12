import Foundation
import os
import KnowledgeBase

protocol SHUploadStepBackgroundOperation {
    var log: Logger { get }
    var serverProxy: SHServerProxy { get }
    
    func markLocalAssetAsFailed(globalIdentifier: String, versions: [SHAssetQuality]) throws
}

extension SHUploadStepBackgroundOperation {
    
    func markLocalAssetAsFailed(globalIdentifier: String, versions: [SHAssetQuality]) throws {
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
}

open class SHUploadOperation: SHAbstractBackgroundOperation, SHUploadStepBackgroundOperation, SHBackgroundQueueProcessorOperationProtocol {
    
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
    
    internal var serverProxy: SHServerProxy {
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
        
        do {
            let queue = try BackgroundOperationQueue.of(type: .upload)
            _ = try queue.dequeue(item: item)
        }
        catch {
            log.error("asset \(localIdentifier) failed to upload but dequeuing from UPLOAD queue failed, so this operation will be attempted again.")
            throw error
        }
        
        let failedUploadQueueItem = SHFailedUploadRequestQueueItem(
            localIdentifier: localIdentifier,
            versions: versions,
            groupId: groupId,
            eventOriginator: eventOriginator,
            sharedWith: sharedWith,
            isBackground: request.isBackground
        )
          
        do {
            /// Enquque to FailedUpload queue
            log.info("enqueueing upload request for asset \(localIdentifier) versions \(versions) in the FAILED queue")
            
            let failedUploadQueue = try BackgroundOperationQueue.of(type: .failedUpload)
            let successfulUploadQueue = try BackgroundOperationQueue.of(type: .successfulUpload)
            
            try self.markLocalAssetAsFailed(globalIdentifier: globalIdentifier, versions: versions)
            try failedUploadQueueItem.enqueue(in: failedUploadQueue)
            
            /// Remove items in the `UploadHistoryQueue` for the same request identifier
            let _ = try? successfulUploadQueue.removeValues(forKeysMatching: KBGenericCondition(.equal, value: failedUploadQueueItem.identifier))
        }
        catch {
            log.fault("asset \(localIdentifier) failed to upload but will never be recorded as failed because enqueueing to FAILED queue failed: \(error.localizedDescription)")
            throw error
        }
        
        guard request.isBackground == false else {
            /// Avoid other side-effects for background  `SHUploadRequestQueueItem`
            return
        }
        
        /// Notify the delegates
        for delegate in delegates {
            if let delegate = delegate as? SHAssetUploaderDelegate {
                delegate.didFailUpload(queueItemIdentifier: failedUploadQueueItem.identifier, error: error)
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
        
        do {
            let queue = try BackgroundOperationQueue.of(type: .upload)
            _ = try queue.dequeue(item: item)
        } catch {
            log.warning("item \(item.identifier) was completed but dequeuing from UPLOAD queue failed. This task will be attempted again")
            throw error
        }

        let succesfulUploadQueueItem = SHUploadHistoryItem(
            localAssetId: localIdentifier,
            globalAssetId: globalIdentifier,
            versions: versions,
            groupId: groupId,
            eventOriginator: eventOriginator,
            sharedWith: [],
            isBackground: isBackground
        )
        
        do {
            let failedUploadQueue = try BackgroundOperationQueue.of(type: .failedUpload)
            let successfulUploadQueue = try BackgroundOperationQueue.of(type: .successfulUpload)
            
            /// Enqueue to success history
            log.info("UPLOAD succeeded. Enqueueing upload request in the SUCCESS queue (upload history) for asset \(globalIdentifier)")
            try succesfulUploadQueueItem.enqueue(in: successfulUploadQueue)
            
            /// Remove items in the `FailedUploadQueue` for the same identifier
            let _ = try? failedUploadQueue.removeValues(forKeysMatching: KBGenericCondition(.equal, value: succesfulUploadQueueItem.identifier))
        } catch {
            log.fault("asset \(localIdentifier) was upload but will never be recorded as uploaded because enqueueing to SUCCESS queue failed")
            throw error
        }
        
        if isBackground == false {
            /// Notify the delegates
            for delegate in delegates {
                if let delegate = delegate as? SHAssetUploaderDelegate {
                    delegate.didCompleteUpload(queueItemIdentifier: succesfulUploadQueueItem.identifier)
                }
            }
        }
        
        ///
        /// Start the sharing part if needed
        ///
        if request.isSharingWithOtherUsers {
            let fetchQueue = try BackgroundOperationQueue.of(type: .fetch)
            
            do {
                ///
                /// Enquque to FETCH queue for encrypting for sharing (note: `shouldUpload=false`)
                ///
                log.info("enqueueing upload request in the FETCH+SHARE queue for asset \(localIdentifier) versions \(versions) isBackground=\(isBackground)")

                let fetchRequest = SHLocalFetchRequestQueueItem(
                    localIdentifier: localIdentifier,
                    versions: versions,
                    groupId: groupId,
                    eventOriginator: eventOriginator,
                    sharedWith: sharedWith,
                    shouldUpload: false,
                    isBackground: isBackground
                )
                try fetchRequest.enqueue(in: fetchQueue)
            } catch {
                log.fault("asset \(localIdentifier) was uploaded but will never be shared because enqueueing to FETCH queue failed")
                throw error
            }
            
            if request.versions.contains(.hiResolution) == false,
               isBackground == false {
                ///
                /// Enquque to FETCH queue cause for sharing we only upload the `.midResolution` version so far.
                /// `.hiResolution` will be uploaded via this operation (note: `versions=[.hiResolution]`, `isBackground=true` and `shouldUpload=true`).
                /// Avoid unintentional recursion by not having a background request calling another background request.
                ///
                /// NOTE: This is only necessary when the user shares assets, because in that case `.lowResolution` and `.midResolution` are uploaded first, and `.hiResolution` later
                /// When assets are only backed up, there's no `.midResolution` used as a surrogate.
                ///
                do {
                    let hiResFetchQueueItem = SHLocalFetchRequestQueueItem(
                        localIdentifier: request.localIdentifier,
                        versions: [.hiResolution],
                        groupId: request.groupId,
                        eventOriginator: request.eventOriginator,
                        sharedWith: request.sharedWith,
                        shouldUpload: true,
                        isBackground: true
                    )
                    try hiResFetchQueueItem.enqueue(in: fetchQueue)
                    log.info("enqueueing asset \(localIdentifier) HI RESOLUTION for upload")
                }
                catch {
                    log.fault("asset \(localIdentifier) was upload but the hi resolution will not be uploaded because enqueueing to FETCH queue failed")
                    throw error
                }
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
            do {
                let queue = try BackgroundOperationQueue.of(type: .upload)
                _ = try queue.dequeue(item: item)
            }
            catch {
                log.warning("dequeuing failed of unexpected data in UPLOAD queue. This task will be attempted again.")
            }
            throw error
        }
        
        if uploadRequest.isBackground == false {
            for delegate in delegates {
                if let delegate = delegate as? SHAssetUploaderDelegate {
                    delegate.didStartUpload(queueItemIdentifier: uploadRequest.identifier)
                }
            }
        }
        
        let globalIdentifier = uploadRequest.globalAssetId
        let localIdentifier = uploadRequest.localIdentifier
        let versions = uploadRequest.versions
        
        do {
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
    
    public func run(forQueueItemIdentifiers queueItemIdentifiers: [String]) throws {
        let uploadQueue = try BackgroundOperationQueue.of(type: .upload)
        
        var queueItems = [KBQueueItem]()
        var error: Error? = nil
        let group = DispatchGroup()
        group.enter()
        uploadQueue.retrieveItems(withIdentifiers: queueItemIdentifiers) {
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
            guard processingState(for: item.identifier) != .uploading else {
                continue
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
    
    public func runOnce(maxItems: Int? = nil) throws {
        var count = 0
        let queue = try BackgroundOperationQueue.of(type: .upload)
        
        while let item = try queue.peek() {
            guard maxItems == nil || count < maxItems! else {
                break
            }
            guard processingState(for: item.identifier) != .uploading else {
                continue
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
            let queue = try BackgroundOperationQueue.of(type: .upload)
            items = try queue.peekNext(self.limit)
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
            
            DispatchQueue.global().async { [self] in
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
