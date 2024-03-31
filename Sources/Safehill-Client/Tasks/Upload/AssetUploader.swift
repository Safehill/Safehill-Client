import Foundation
import os
import KnowledgeBase


internal class SHUploadOperation: SHAbstractBackgroundOperation, SHUploadStepBackgroundOperation, SHBackgroundQueueProcessorOperationProtocol {
    
    let log = Logger(subsystem: "com.gf.safehill", category: "BG-UPLOAD")
    
    let limit: Int
    let user: SHAuthenticatedLocalUser
    let localAssetStoreController: SHLocalAssetStoreController
    let delegates: [SHOutboundAssetOperationDelegate]
    
    var serverProxy: SHServerProxy {
        return self.user.serverProxy
    }
    
    public init(user: SHAuthenticatedLocalUser,
                localAssetStoreController: SHLocalAssetStoreController,
                delegates: [SHOutboundAssetOperationDelegate],
                limitPerRun limit: Int) {
        self.user = user
        self.localAssetStoreController = localAssetStoreController
        self.limit = limit
        self.delegates = delegates
    }
    
    public func clone() -> SHBackgroundOperationProtocol {
        SHUploadOperation(user: self.user,
                          localAssetStoreController: self.localAssetStoreController,
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
    
    public func markAsFailed(
        item: KBQueueItem,
        uploadRequest request: SHUploadRequestQueueItem,
        error: Error,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        let localIdentifier = request.localIdentifier
        let globalIdentifier = request.globalAssetId
        let versions = request.versions
        let groupId = request.groupId
        let eventOriginator = request.eventOriginator
        let users = request.sharedWith
        
        /// Dequeque from UploadQueue
        log.info("dequeueing request for asset \(localIdentifier) from the UPLOAD queue")
        
        do {
            let uploadQueue = try BackgroundOperationQueue.of(type: .upload)
            _ = try uploadQueue.dequeue(item: item)
        }
        catch {
            log.error("asset \(localIdentifier) failed to upload but dequeuing from UPLOAD queue failed, so this operation will be attempted again.")
            completionHandler(.failure(error))
            return
        }
        
        self.markAsFailed(
            localIdentifier: localIdentifier,
            versions: versions,
            groupId: groupId,
            eventOriginator: eventOriginator,
            sharedWith: users,
            isBackground: request.isBackground
        )
        
        self.markLocalAssetAsFailed(globalIdentifier: globalIdentifier, versions: versions) {
            guard request.isBackground == false else {
                /// Avoid other side-effects for background  `SHUploadRequestQueueItem`
                completionHandler(.success(()))
                return
            }
            
            /// Notify the delegates
            for delegate in self.delegates {
                if let delegate = delegate as? SHAssetUploaderDelegate {
                    delegate.didFailUpload(queueItemIdentifier: request.identifier, error: error)
                }
                if users.count > 0 {
                    if let delegate = delegate as? SHAssetSharerDelegate {
                        delegate.didFailSharing(queueItemIdentifier: request.identifier)
                    }
                }
            }
            
            completionHandler(.success(()))
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
            let uploadQueue = try BackgroundOperationQueue.of(type: .upload)
            _ = try uploadQueue.dequeue(item: item)
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
                    globalIdentifier: globalIdentifier,
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
                        globalIdentifier: globalIdentifier,
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
    
    func process(
        _ item: KBQueueItem,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
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
                let uploadQueue = try BackgroundOperationQueue.of(type: .upload)
                _ = try uploadQueue.dequeue(item: item)
            }
            catch {
                log.warning("dequeuing failed of unexpected data in UPLOAD queue. This task will be attempted again.")
            }
            completionHandler(.failure(error))
            return
        }
        
        let handleError = { (error: Error) in
            self.log.error("FAIL in UPLOAD: \(error.localizedDescription)")
            
            self.markAsFailed(
                item: item,
                uploadRequest: uploadRequest,
                error: error
            ) { _ in
                completionHandler(.failure(error))
            }
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
        
        log.info("retrieving encrypted asset from local server proxy: \(globalIdentifier) versions=\(versions)")
        self.localAssetStoreController.encryptedAsset(
            with: globalIdentifier,
            versions: versions,
            cacheHiResolution: false,
            qos: .background
        ) { result in
            
            switch result {
            case .failure(let error):
                self.log.error("failed to retrieve local server asset for localIdentifier \(localIdentifier): \(error.localizedDescription).")
                handleError(error)
            case .success(let encryptedAsset):
                guard globalIdentifier == encryptedAsset.globalIdentifier else {
                    let error = SHBackgroundOperationError.globalIdentifierDisagreement(localIdentifier)
                    handleError(error)
                    return
                }
                
#if DEBUG
                guard ErrorSimulator.percentageUploadFailures == 0
                      || arc4random() % (100 / ErrorSimulator.percentageUploadFailures) != 0 else {
                    self.log.debug("simulating CREATE ASSET failure")
                    let error = SHBackgroundOperationError.fatalError("failed to create server asset")
                    handleError(error)
                    return
                }
#endif
                let serverAsset: SHServerAsset
                do {
                    serverAsset = try SHAssetStoreController(user: self.user)
                        .upload(
                            asset: encryptedAsset,
                            with: uploadRequest.groupId,
                            filterVersions: versions,
                            force: true
                        )
                } catch {
                    let error = SHBackgroundOperationError.fatalError("failed to create server asset or upload asset to the CDN")
                    handleError(error)
                    return
                }
                
                guard globalIdentifier == serverAsset.globalIdentifier else {
                    let error = SHBackgroundOperationError.globalIdentifierDisagreement(localIdentifier)
                    handleError(error)
                    return
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
                    self.log.critical("failed to mark UPLOAD as successful. This will likely cause infinite loops")
                    handleError(error)
                }
                completionHandler(.success(()))
            }
        }
    }
    
    public func run(
        forQueueItemIdentifiers queueItemIdentifiers: [String],
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        let uploadQueue: KBQueueStore
        
        do {
            uploadQueue = try BackgroundOperationQueue.of(type: .upload)
        } catch {
            log.critical("failed to read from UPLOAD queue. \(error.localizedDescription)")
            completionHandler(.failure(error))
            return
        }
        
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
        
        group.notify(queue: .global()) {
            guard error == nil else {
                self.log.critical("failed to retrieve items from UPLOAD queue. \(error!.localizedDescription)")
                completionHandler(.failure(error!))
                return
            }
            
            /// 
            /// Don't start more than 5 every 500ms
            ///
            for itemChunk in queueItems.chunked(into: 5) {
                for item in itemChunk {
                    group.enter()
                    self.runOnce(for: item) { _ in
                        group.leave()
                    }
                }
                
                let second: Double = 1000000
                usleep(useconds_t(0.5 * second)) // 500ms
            }
            
            group.notify(queue: .global()) {
                completionHandler(.success(()))
            }
        }
    }
    
    func runOnce(
        for item: KBQueueItem,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        guard processingState(for: item.identifier) != .uploading else {
            completionHandler(.failure(SHBackgroundOperationError.alreadyProcessed))
            return
        }
        
        log.info("uploading item \(item.identifier) created at \(item.createdAt)")
        setProcessingState(.uploading, for: item.identifier)
        
        self.process(item) { result in
            if case .success = result {
                self.log.info("[√] upload task completed for item \(item.identifier)")
            } else {
                self.log.error("[x] upload task failed for item \(item.identifier)")
            }
            
            setProcessingState(nil, for: item.identifier)
            completionHandler(result)
        }
    }
    
    public override func runOnce(
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        let dispatchGroup = DispatchGroup()
        let uploadQueue: KBQueueStore
        do {
            uploadQueue = try BackgroundOperationQueue.of(type: .upload)
        } catch {
            log.critical("failed to read from UPLOAD queue. \(error.localizedDescription)")
            completionHandler(.failure(error))
            return
        }
        
        var count = 0
        
        do {
            while let item = try uploadQueue.peek() {
                guard self.limit == 0 || count < self.limit else {
                    break
                }
                
                dispatchGroup.enter()
                self.runOnce(for: item) { _ in
                    dispatchGroup.leave()
                }
                
                count += 1
                
                guard !self.isCancelled else {
                    self.log.info("upload task cancelled. Finishing")
                    break
                }
            }
        } catch {
            log.error("failed to fetch items from the UPLOAD queue: \(error.localizedDescription)")
        }
        
        log.info("started \(count) UPLOAD operations")
        
        dispatchGroup.notify(queue: .global()) {
            completionHandler(.success(()))
        }
    }
}

internal class SHAssetsUploaderQueueProcessor : SHBackgroundOperationProcessor<SHUploadOperation> {
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
