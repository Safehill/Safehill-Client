import Foundation
import Safehill_Crypto
import KnowledgeBase
import Photos
import os

open class SHLocalFetchOperation: SHAbstractBackgroundOperation, SHBackgroundQueueProcessorOperationProtocol {
    
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
        self.photoIndexer.fetchCameraRollAssets(withFilters: []) { _ in }
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
    
    private func retrieveAsset(fetchRequest request: SHLocalFetchRequestQueueItem) throws -> SHApplePhotoAsset {
        let localIdentifier = request.localIdentifier
        let versions = request.versions
        
        var photoAsset: SHApplePhotoAsset? = nil
        var error: Error? = nil
        let group = DispatchGroup()
        
        group.enter()
        photoIndexer.fetchCameraRollAsset(withLocalIdentifier: localIdentifier) { result in
            switch result {
            case .failure(let err):
                error = err
                group.leave()
                return
            case .success(let maybePHAsset):
                guard let phAsset = maybePHAsset else {
                    error = SHBackgroundOperationError.fatalError("No asset with local identifier \(localIdentifier)")
                    group.leave()
                    return
                }
                
                ///
                /// Fetch the higest-needed resolution asset based on the versions,
                /// using the same imageManager used to display the asset (so that it's likely that it was already cached by Photos).
                /// Doing this here avoids fetching large assets from the Apple Photos library in the SHEncryptOperation,
                /// as cachedImage on the SHApplePhotoAsset will be set here, and will be resized to smaller images
                /// when generating the SHAssetQuality versions.
                ///
                
                let highestSize: CGSize
                if versions.contains(.hiResolution) {
                    highestSize = kSHSizeForQuality(quality: .hiResolution)
                } else if versions.contains(.midResolution) {
                    highestSize = kSHSizeForQuality(quality: .midResolution)
                } else {
                    highestSize = kSHSizeForQuality(quality: .lowResolution)
                }
                 
                let options = PHImageRequestOptions()
                options.isNetworkAccessAllowed = true
                options.deliveryMode = .highQualityFormat
                self.imageManager.startCachingImages(for: [phAsset],
                                                     targetSize: highestSize,
                                                     contentMode: .default,
                                                     options: options)
                
                self.log.info("asking imageManager \(self.imageManager) to cache image size \(highestSize.debugDescription) for asset \(phAsset.localIdentifier)")
                
                photoAsset = SHApplePhotoAsset(
                    for: phAsset,
                    cachedImage: nil,
                    usingCachingImageManager: self.imageManager
                )
                
                group.leave()
            }
        }
        
        let dispatchResult = group.wait(timeout: .now() + .seconds(30))
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
        guard error == nil else {
            throw error!
        }
        
        return photoAsset!
    }
    
    public func markAsFailed(fetchRequest request: SHLocalFetchRequestQueueItem) throws
    {
        let localIdentifier = request.localIdentifier
        let versions = request.versions
        let groupId = request.groupId
        let eventOriginator = request.eventOriginator
        let users = request.sharedWith
        
        // Dequeue from FETCH queue
        log.info("dequeueing request for asset \(localIdentifier) from the FETCH queue")
        let fetchQueue = try BackgroundOperationQueue.of(type: .fetch)
        let failedUploadQueue = try BackgroundOperationQueue.of(type: .failedUpload)
        
        do { _ = try fetchQueue.dequeue() }
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
        fetchRequest request: SHLocalFetchRequestQueueItem
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
        
        do { _ = try fetchQueue.dequeue() }
        catch {
            log.warning("asset \(localIdentifier) was fetched but dequeuing failed, so this operation will be attempted again.")
            throw error
        }
    }
    
    private func process(_ item: KBQueueItem) throws {
        let fetchQueue = try BackgroundOperationQueue.of(type: .fetch)
        let failedUploadQueue = try BackgroundOperationQueue.of(type: .failedUpload)
        let failedShareQueue = try BackgroundOperationQueue.of(type: .failedShare)
        
        let fetchRequest: SHLocalFetchRequestQueueItem
        
        do {
            let content = try content(ofQueueItem: item)
            guard let content = content as? SHLocalFetchRequestQueueItem else {
                log.error("unexpected data found in FETCH queue. Dequeueing")
                // Delegates can't be called as item content can't be read and it will be silently removed from the queue
                throw SHBackgroundOperationError.unexpectedData(item.content)
            }
            fetchRequest = content
        } catch {
            do { _ = try fetchQueue.dequeue(item: item) }
            catch {
                log.warning("dequeuing failed of unexpected data in FETCH queue. This task will be attempted again.")
            }
            throw error
        }
        
        if fetchRequest.isBackground == false {
            ///
            /// Background requests have no side effects, so they shouldn't remove items in the SUCCESS or FAILED queues created by non-background requests.
            /// All other requests when triggered (by adding them to the FetchQueue) should remove previous side effects in the following queues:
            /// - `FailedUploadQueue` (all items with same local identifier)
            /// - `FailedShareQueue` (all items with same local identifier, group and users (there can be many with same local identifier and group, when asset is shared with different users at different times)
            ///
            let _ = try? failedUploadQueue.removeValues(forKeysMatching: KBGenericCondition(.beginsWith, value: SHQueueOperation.queueIdentifier(for: fetchRequest.localIdentifier)))
            let _ = try? failedShareQueue.removeValues(forKeysMatching: KBGenericCondition(.equal, value: fetchRequest.identifier))
            
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
        
        guard let photoAsset = try? self.retrieveAsset(fetchRequest: fetchRequest) else {
            log.error("failed to fetch data for item \(item.identifier). Dequeueing item, as it's unlikely to succeed again.")
            do {
                try self.markAsFailed(fetchRequest: fetchRequest)
            } catch {
                log.critical("failed to mark FETCH as failed. This will likely cause infinite loops")
                // TODO: Handle
            }
            
            throw SHBackgroundOperationError.fatalError("failed to retrieve asset from Apple library")
        }
        
        do {
            try self.markAsSuccessful(
                photoAsset: photoAsset,
                fetchRequest: fetchRequest
            )
        } catch {
            log.critical("failed to mark FETCH as successful. This will likely cause infinite loops")
            // TODO: Handle
        }
    }
    
    public func runOnce(maxItems: Int? = nil) throws {
        var count = 0
        let fetchQueue = try BackgroundOperationQueue.of(type: .fetch)
        
        while let item = try fetchQueue.peek() {
            guard maxItems == nil || count < maxItems! else {
                break
            }
            guard processingState(for: item.identifier) != .fetching else {
                continue
            }
            
            log.info("fetching item \(item.identifier) created at \(item.createdAt)")
            setProcessingState(.fetching, for: item.identifier)
            
            do {
                try self.process(item)
                log.info("[√] fetch task completed for item \(item.identifier)")
            } catch {
                log.error("[x] fetch task failed for item \(item.identifier): \(error.localizedDescription)")
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
            let fetchQueue = try BackgroundOperationQueue.of(type: .fetch)
            items = try fetchQueue.peekNext(self.limit)
        } catch {
            log.error("failed to fetch items from the FETCH queue")
            state = .finished
            return
        }
        
        for item in items {
            guard processingState(for: item.identifier) != .fetching else {
                break
            }
            
            log.info("fetching item \(item.identifier) created at \(item.createdAt)")
            setProcessingState(.fetching, for: item.identifier)
            
            DispatchQueue.global().async { [self] in
                guard !isCancelled else {
                    log.info("fetch task cancelled. Finishing")
                    setProcessingState(nil, for: item.identifier)
                    return
                }
                do {
                    try self.process(item)
                    log.info("[√] fetch task completed for item \(item.identifier)")
                } catch {
                    log.error("[x] fetch task failed for item \(item.identifier): \(error.localizedDescription)")
                }
                
                setProcessingState(nil, for: item.identifier)
            }
                
            guard !self.isCancelled else {
                log.info("fetch task cancelled. Finishing")
                break
            }
        }
        
        state = .finished
    }
}

public class SHAssetsFetcherQueueProcessor : SHBackgroundOperationProcessor<SHLocalFetchOperation> {
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
