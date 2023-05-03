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
    
    public init(delegates: [SHOutboundAssetOperationDelegate],
                limitPerRun limit: Int,
                imageManager: PHCachingImageManager? = nil) {
        self.limit = limit
        self.delegates = delegates
        self.imageManager = imageManager ?? PHCachingImageManager()
    }
    
    public func clone() -> SHBackgroundOperationProtocol {
        SHLocalFetchOperation(
            delegates: self.delegates,
            limitPerRun: self.limit,
            imageManager: self.imageManager
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
        
        let photoIndexer = SHPhotosIndexer()
        let assetIdFilter = SHPhotosFilter.withLocalIdentifiers([localIdentifier])
        var photoAsset: SHApplePhotoAsset? = nil
        var error: Error? = nil
        let group = DispatchGroup()
        
        group.enter()
        photoIndexer.fetchCameraRollAssets(withFilters: [assetIdFilter]) {
            result in
            if case .failure(let err) = result {
                error = err
                group.leave()
                return
            }
            
            guard let phAsset = photoIndexer.indexedAssets.first else {
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
            if let versions = versions {
                if versions.contains(.hiResolution) {
                    highestSize = kSHSizeForQuality(quality: .hiResolution)
                } else if versions.contains(.midResolution) {
                    highestSize = kSHSizeForQuality(quality: .midResolution)
                } else {
                    highestSize = kSHSizeForQuality(quality: .lowResolution)
                }
            } else {
                highestSize = kSHHiResPictureSize
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
        
        do { _ = try FetchQueue.dequeue() }
        catch {
            log.error("asset \(localIdentifier) failed to encrypt but dequeuing from FETCH queue failed, so this operation will be attempted again.")
            throw error
        }
        
#if DEBUG
        log.debug("items in the FETCH queue after dequeueing \((try? FetchQueue.peekItems(createdWithin: DateInterval(start: .distantPast, end: Date())))?.count ?? 0)")
#endif
        
        guard request.isBackground == false else {
            /// Avoid other side-effects for background  `SHLocalFetchRequestQueueItem`
            return
        }
        
        /// Enquque to failed
        log.info("enqueueing fetch request for asset \(localIdentifier) in the FAILED queue")
        
        let queueItemIdentifier = SHUploadPipeline.uploadQueueItemKey(
            groupId: groupId,
            assetLocalIdentifier: localIdentifier,
            versions: versions
        )
        let failedUploadQueueItem = SHFailedUploadRequestQueueItem(
            localIdentifier: localIdentifier,
            versions: versions,
            groupId: groupId,
            eventOriginator: eventOriginator,
            sharedWith: users
        )
        
        do {
            try failedUploadQueueItem.enqueue(in: FailedUploadQueue, with: queueItemIdentifier)
        } catch {
            /// Be forgiving for failed Fetch operations
            log.fault("asset \(localIdentifier) failed to fetch but will never be recorded as failed because enqueueing to FAILED queue failed: \(error.localizedDescription)")
        }
        
        /// Notify the delegates
        for delegate in delegates {
            if let delegate = delegate as? SHAssetFetcherDelegate {
                delegate.didFailFetching(
                    itemWithLocalIdentifier: localIdentifier,
                    groupId: groupId,
                    sharedWith: users
                )
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
        
        ///
        /// Enqueue in the next queue
        /// - Encryption queue for items to upload
        /// - Share queue for items to share
        ///
        if shouldUpload {
            log.info("enqueueing encryption request in the ENCRYPT queue for asset \(localIdentifier) versions \(versions ?? []) isBackground=\(isBackground)")
            
            let queueItemIdentifier = SHUploadPipeline.uploadQueueItemKey(
                groupId: groupId,
                assetLocalIdentifier: localIdentifier,
                versions: versions
            )
            let encryptionRequest = SHEncryptionRequestQueueItem(
                asset: photoAsset,
                versions: versions,
                groupId: groupId,
                eventOriginator: eventOriginator,
                sharedWith: users,
                isBackground: isBackground
            )
            
            do {
                try encryptionRequest.enqueue(in: EncryptionQueue, with: queueItemIdentifier)
            } catch {
                log.fault("asset \(localIdentifier) was encrypted but will never be uploaded because enqueueing to UPLOAD queue failed")
                throw error
            }
        } else {
            let encryptionForSharingRequest = SHEncryptionForSharingRequestQueueItem(
                asset: photoAsset,
                versions: versions,
                groupId: groupId,
                eventOriginator: eventOriginator,
                sharedWith: users,
                isBackground: isBackground
            )
            log.info("enqueueing encryption request in the SHARE queue for asset \(localIdentifier) versions \(versions ?? []) isBackground=\(isBackground)")
            
            let queueItemIdentifier = SHUploadPipeline.shareQueueItemKey(
                groupId: groupId,
                assetLocalIdentifier: localIdentifier,
                versions: versions,
                users: users
            )
            do {
                try encryptionForSharingRequest.enqueue(in: ShareQueue, with: queueItemIdentifier)
            }
            catch {
                log.fault("asset \(localIdentifier) was encrypted but will never be shared because enqueueing to SHARE queue failed")
                throw error
            }
        }
        
        ///
        /// Dequeue from FetchQueue
        ///
        log.info("dequeueing request for asset \(localIdentifier) from the FETCH queue")
        
        do { _ = try FetchQueue.dequeue() }
        catch {
            log.warning("asset \(localIdentifier) was fetched but dequeuing failed, so this operation will be attempted again.")
            throw error
        }
        
#if DEBUG
        log.debug("items in the FETCH queue after dequeueing \((try? FetchQueue.peekNext(100))?.count ?? 0)")
#endif
        
        guard request.isBackground == false else {
            /// Avoid other side-effects for background  `SHLocalFetchRequestQueueItem`
            return
        }
        /// Notify the delegates
        for delegate in delegates {
            if let delegate = delegate as? SHAssetFetcherDelegate {
                delegate.didCompleteFetching(
                    itemWithLocalIdentifier: localIdentifier,
                    groupId: groupId,
                    sharedWith: users
                )
            }
        }
    }
    
    private func process(_ item: KBQueueItem) throws {
        
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
            do { _ = try FetchQueue.dequeue(item: item) }
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
            let _ = try? FailedUploadQueue.removeValues(forKeysMatching: KBGenericCondition(.beginsWith, value: SHQueueOperation.queueIdentifier(for: fetchRequest.localIdentifier)))
            let shareQueueItemIdentifier = SHUploadPipeline.shareQueueItemKey(
                groupId: fetchRequest.groupId,
                assetLocalIdentifier: fetchRequest.localIdentifier,
                versions: fetchRequest.versions,
                users: fetchRequest.sharedWith
            )
            let _ = try? FailedShareQueue.removeValues(forKeysMatching: KBGenericCondition(.equal, value: shareQueueItemIdentifier))
            
            for delegate in delegates {
                if let delegate = delegate as? SHAssetFetcherDelegate {
                    delegate.didStartFetching(
                        itemWithLocalIdentifier: fetchRequest.localIdentifier,
                        groupId: fetchRequest.groupId,
                        sharedWith: fetchRequest.sharedWith
                    )
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
    
    public func runOnce() throws {
        while let item = try FetchQueue.peek() {
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
            items = try FetchQueue.peekNext(self.limit)
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
