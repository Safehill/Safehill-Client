import Foundation
import Safehill_Crypto
import KnowledgeBase
import Photos
import os

internal class SHLocalFetchOperation: SHAbstractBackgroundOperation, SHBackgroundQueueProcessorOperationProtocol {
    
    public var log: Logger {
        Logger(subsystem: "com.gf.safehill", category: "BG-FETCH")
    }
    
    public let limit: Int
    public var delegates: [SHOutboundAssetOperationDelegate]
    var imageManager: PHCachingImageManager
    let photoIndexer: SHPhotosIndexer
    
    public init(delegates: [SHOutboundAssetOperationDelegate],
                limitPerRun limit: Int,
                imageManager: PHCachingImageManager? = nil,
                photoIndexer: SHPhotosIndexer? = nil) {
        self.limit = limit
        self.delegates = delegates
        self.imageManager = imageManager ?? PHCachingImageManager()
        self.photoIndexer = photoIndexer ?? SHPhotosIndexer()
    }
    
    public func clone() -> SHBackgroundOperationProtocol {
        SHLocalFetchOperation(
            delegates: self.delegates,
            limitPerRun: self.limit,
            imageManager: self.imageManager,
            photoIndexer: self.photoIndexer
        )
    }
    
    public func content(ofQueueItem item: KBQueueItem) throws -> SHSerializableQueueItem {
        guard let data = item.content as? Data else {
            throw KBError.unexpectedData(item.content)
        }
        
        let unarchiver: NSKeyedUnarchiver
        if #available(macOS 10.13, *) {
            unarchiver = try NSKeyedUnarchiver(forReadingFrom: data)
        } else {
            unarchiver = NSKeyedUnarchiver(forReadingWith: data)
        }
        
        guard let fetchRequest = unarchiver.decodeObject(of: SHLocalFetchRequestQueueItem.self, forKey: NSKeyedArchiveRootObjectKey) else {
            throw KBError.unexpectedData(item)
        }
        
        return fetchRequest
    }
    
    private func retrieveAsset(
        fetchRequest request: SHLocalFetchRequestQueueItem,
        completionHandler: @escaping (Result<SHApplePhotoAsset, Error>) -> Void
    ) {
        let localIdentifier = request.localIdentifier
        
        if let value = try? photoIndexer.index?.value(for: localIdentifier) as? SHApplePhotoAsset {
            completionHandler(.success(value))
            return
        }
        
        photoIndexer.fetchAsset(withLocalIdentifier: localIdentifier) { result in
            switch result {
            case .failure(let error):
                completionHandler(.failure(error))
            case .success(let maybePHAsset):
                guard let phAsset = maybePHAsset else {
                    let error = SHBackgroundOperationError.fatalError("No asset with local identifier \(localIdentifier)")
                    completionHandler(.failure(error))
                    return
                }
                
                let photoAsset = SHApplePhotoAsset(
                    for: phAsset,
                    usingCachingImageManager: self.imageManager
                )
                completionHandler(.success(photoAsset))
            }
        }
    }
    
    public func markAsFailed(
        fetchRequest request: SHLocalFetchRequestQueueItem,
        queueItem: KBQueueItem
    ) throws {
        let localIdentifier = request.localIdentifier
        let versions = request.versions
        let groupId = request.groupId
        let eventOriginator = request.eventOriginator
        let users = request.sharedWith
        
        // Dequeue from FETCH queue
        log.info("dequeueing request for asset \(localIdentifier) from the FETCH queue")
        let fetchQueue = try BackgroundOperationQueue.of(type: .fetch)
        let failedUploadQueue = try BackgroundOperationQueue.of(type: .failedUpload)
        
        do { _ = try fetchQueue.dequeue(item: queueItem) }
        catch {
            log.error("asset \(localIdentifier) failed to encrypt but dequeuing from FETCH queue failed, so this operation will be attempted again.")
            throw error
        }
        
        guard request.isBackground == false else {
            /// Avoid other side-effects for background  `SHLocalFetchRequestQueueItem`
            return
        }
        
        let failedUploadQueueItem = SHFailedUploadRequestQueueItem(
            localIdentifier: localIdentifier,
            versions: versions,
            groupId: groupId,
            eventOriginator: eventOriginator,
            sharedWith: users,
            isBackground: request.isBackground
        )
        
        do {
            /// Enquque to failed
            log.info("enqueueing fetch request for asset \(localIdentifier) versions \(versions) in the FAILED queue")
            try failedUploadQueueItem.enqueue(in: failedUploadQueue)
        } catch {
            /// Be forgiving for failed Fetch operations
            log.fault("asset \(localIdentifier) failed to fetch but will never be recorded as failed because enqueueing to FAILED queue failed: \(error.localizedDescription)")
        }
        
        /// Notify the delegates
        for delegate in delegates {
            if let delegate = delegate as? SHAssetFetcherDelegate {
                if request.shouldUpload == true {
                    delegate.didFailFetchingForUpload(queueItemIdentifier: failedUploadQueueItem.identifier)
                } else {
                    delegate.didFailFetchingForSharing(queueItemIdentifier: failedUploadQueueItem.identifier)
                }
            }
        }
    }
    
    public func markAsSuccessful(
        photoAsset: SHApplePhotoAsset,
        fetchRequest request: SHLocalFetchRequestQueueItem,
        queueItem: KBQueueItem
    ) throws
    {
        let localIdentifier = photoAsset.phAsset.localIdentifier
        let versions = request.versions
        let groupId = request.groupId
        let eventOriginator = request.eventOriginator
        let users = request.sharedWith
        let shouldUpload = request.shouldUpload
        let isBackground = request.isBackground
        
        let fetchQueue = try BackgroundOperationQueue.of(type: .fetch)
        let encryptionQueue = try BackgroundOperationQueue.of(type: .encryption)
        let shareQueue = try BackgroundOperationQueue.of(type: .share)
        
        ///
        /// Enqueue in the next queue
        /// - Encryption queue for items to upload
        /// - Share queue for items to share
        ///
        if shouldUpload {
            do {
                log.info("enqueueing encryption request in the ENCRYPT queue for asset \(localIdentifier) versions \(versions) isBackground=\(isBackground)")
                
                let encryptionRequest = SHEncryptionRequestQueueItem(
                    asset: photoAsset,
                    versions: versions,
                    groupId: groupId,
                    eventOriginator: eventOriginator,
                    sharedWith: users,
                    isBackground: isBackground
                )
                try encryptionRequest.enqueue(in: encryptionQueue)
            } catch {
                log.fault("asset \(localIdentifier) was encrypted but will never be uploaded because enqueueing to UPLOAD queue failed")
                throw error
            }
            
            if request.isBackground == false {
                /// Notify the delegates
                for delegate in delegates {
                    if let delegate = delegate as? SHAssetFetcherDelegate {
                        if request.shouldUpload == true {
                            delegate.didCompleteFetchingForUpload(queueItemIdentifier: request.identifier)
                        } else {
                            delegate.didCompleteFetchingForSharing(queueItemIdentifier: request.identifier)
                        }
                    }
                }
            }
        } else {
            do {
                log.info("enqueueing encryption request in the SHARE queue for asset \(localIdentifier) versions \(versions) isBackground=\(isBackground)")

                let encryptionForSharingRequest = SHEncryptionForSharingRequestQueueItem(
                    asset: photoAsset,
                    versions: versions,
                    groupId: groupId,
                    eventOriginator: eventOriginator,
                    sharedWith: users,
                    isBackground: isBackground
                )
                try encryptionForSharingRequest.enqueue(in: shareQueue)
            } catch {
                log.fault("asset \(localIdentifier) was encrypted but will never be shared because enqueueing to SHARE queue failed")
                throw error
            }
        }
        
        ///
        /// Dequeue from FetchQueue
        ///
        log.info("dequeueing request for asset \(localIdentifier) from the FETCH queue")
        
        do { _ = try fetchQueue.dequeue(item: queueItem) }
        catch {
            log.warning("asset \(localIdentifier) was fetched but dequeuing failed, so this operation will be attempted again.")
            throw error
        }
    }
    
    private func process(
        _ item: KBQueueItem,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        let fetchRequest: SHLocalFetchRequestQueueItem
        
        do {
            let content = try content(ofQueueItem: item)
            guard let content = content as? SHLocalFetchRequestQueueItem else {
                log.error("unexpected data found in FETCH queue. Dequeueing")
                // Delegates can't be called as item content can't be read and it will be silently removed from the queue
                completionHandler(.failure(SHBackgroundOperationError.unexpectedData(item.content)))
                return
            }
            fetchRequest = content
        } catch {
            do { _ = try BackgroundOperationQueue.of(type: .fetch).dequeue(item: item) }
            catch {
                log.fault("dequeuing failed of unexpected data in FETCH queue. \(error.localizedDescription)")
            }
            completionHandler(.failure(error))
            return
        }
        
        let handleError = { (error: Error) in
            self.log.critical("Error in FETCH asset: \(error.localizedDescription)")
            do {
                try self.markAsFailed(fetchRequest: fetchRequest,
                                      queueItem: item)
            } catch {
                self.log.critical("failed to mark FETCH as failed. This will likely cause infinite loops. \(error.localizedDescription)")
                // TODO: Handle
            }
            completionHandler(.failure(error))
        }
        
        do {
            if fetchRequest.isBackground == false {
                ///
                /// Background requests should have no side effects, so they shouldn't remove items in the SUCCESS or FAILED queues created by non-background requests.
                /// All other requests when triggered (by adding them to the FetchQueue) should remove previous side effects in the following queues:
                /// - `FailedUploadQueue` (all items with same local identifier)
                /// - `FailedShareQueue` (all items with same local identifier, group and users (there can be many with same local identifier and group, when asset is shared with different users at different times)
                ///
                
                let failedUploadQueue = try BackgroundOperationQueue.of(type: .failedUpload)
                let failedShareQueue = try BackgroundOperationQueue.of(type: .failedShare)
                
                let _ = try? failedUploadQueue.removeValues(forKeysMatching: KBGenericCondition(.beginsWith, value: SHQueueOperation.queueIdentifier(for: fetchRequest.localIdentifier)))
                let _ = try? failedShareQueue.removeValues(forKeysMatching: KBGenericCondition(.equal, value: fetchRequest.identifier))
                
                ///
                /// Notify the delegates the upload or sharing operation was kicked off
                ///
                for delegate in delegates {
                    if let delegate = delegate as? SHAssetFetcherDelegate {
                        if fetchRequest.shouldUpload == true {
                            delegate.didStartFetchingForUpload(queueItemIdentifier: fetchRequest.identifier)
                        } else {
                            delegate.didStartFetchingForSharing(queueItemIdentifier: fetchRequest.identifier)
                        }
                    }
                }
            }
        } catch {
            handleError(error)
        }
            
        self.retrieveAsset(fetchRequest: fetchRequest) { result in
            switch result {
            case .success(let photoAsset):
                do {
                    if let gid = fetchRequest.globalIdentifier {
                        ///
                        /// Some `SHLocalFetchRequestQueueItem` have a global identifier set
                        /// for instance when these items are enqueued from an `SHUploadOperation`,
                        /// or when it's a share for an asset that is already on the server
                        ///
                        try photoAsset.setGlobalIdentifier(gid)
                    } else {
                        ///
                        /// Calculate the global identifier so it can be serialized and stored in the `SHApplePhotoAsset`
                        /// along with the queue item being enqueued in `markAsSuccessful`
                        ///
                        let _ = try photoAsset.retrieveOrGenerateGlobalIdentifier()
                    }
                    
                    do {
                        try self.markAsSuccessful(
                            photoAsset: photoAsset,
                            fetchRequest: fetchRequest,
                            queueItem: item
                        )
                        completionHandler(.success(()))
                    } catch {
                        self.log.critical("failed to mark FETCH as successful. This will likely cause infinite loops")
                        handleError(error)
                    }
                } catch {
                    handleError(error)
                }
            case .failure(let failure):
                handleError(failure)
            }
        }
    }
    
    public func run(
        forQueueItemIdentifiers queueItemIdentifiers: [String],
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        let fetchQueue: KBQueueStore
        
        do {
            fetchQueue = try BackgroundOperationQueue.of(type: .fetch)
        } catch {
            log.critical("failed to read from FETCH queue. \(error.localizedDescription)")
            completionHandler(.failure(error))
            return
        }
        
        var queueItems = [KBQueueItem]()
        var error: Error? = nil
        let group = DispatchGroup()
        group.enter()
        fetchQueue.retrieveItems(withIdentifiers: queueItemIdentifiers) {
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
                self.log.critical("failed to retrieve items from FETCH queue. \(error!.localizedDescription)")
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
    
    func runOnce(
        for item: KBQueueItem,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        guard processingState(for: item.identifier) != .fetching else {
            completionHandler(.failure(SHBackgroundOperationError.alreadyProcessed))
            return
        }
        
        log.info("fetching item \(item.identifier) created at \(item.createdAt)")
        setProcessingState(.fetching, for: item.identifier)
        
        self.process(item) { result in
            if case .success = result {
                self.log.info("[√] fetch task completed for item \(item.identifier)")
            } else {
                self.log.error("[x] fetch task failed for item \(item.identifier)")
            }
            
            setProcessingState(nil, for: item.identifier)
            completionHandler(result)
        }
    }
    
    public override func runOnce(
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        let dispatchGroup = DispatchGroup()
        let fetchQueue: KBQueueStore
        do {
            fetchQueue = try BackgroundOperationQueue.of(type: .fetch)
        } catch {
            log.critical("failed to read from FETCH queue. \(error.localizedDescription)")
            completionHandler(.failure(error))
            return
        }
        
        var count = 0
        
        do {
            while let item = try fetchQueue.peek() {
                guard self.limit == 0 || count < self.limit else {
                    break
                }
                
                dispatchGroup.enter()
                self.runOnce(for: item) { _ in
                    dispatchGroup.leave()
                }
                
                count += 1
                
                guard !self.isCancelled else {
                    self.log.info("fetch task cancelled. Finishing")
                    break
                }
            }
        } catch {
            log.error("failed to fetch items from the FETCH queue: \(error.localizedDescription)")
        }
        
        log.info("started \(count) FETCH operations")
        
        dispatchGroup.notify(queue: .global()) {
            completionHandler(.success(()))
        }
    }
}

internal class SHAssetsFetcherQueueProcessor : SHBackgroundOperationProcessor<SHLocalFetchOperation> {
    /// Singleton (with private initializer)
    public static var shared = SHAssetsFetcherQueueProcessor(
        delayedStartInSeconds: 1,
        dispatchIntervalInSeconds: 3
    )
    
    private override init(delayedStartInSeconds: Int = 0,
                          dispatchIntervalInSeconds: Int? = nil) {
        super.init(delayedStartInSeconds: delayedStartInSeconds, dispatchIntervalInSeconds: dispatchIntervalInSeconds)
    }
}
