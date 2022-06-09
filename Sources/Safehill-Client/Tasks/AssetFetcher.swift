import Foundation
import Safehill_Crypto
import KnowledgeBase
import Photos
import os

open class SHLocalFetchOperation: SHAbstractBackgroundOperation, SHBackgroundOperationProtocol {
    
    public var log: Logger {
        Logger(subsystem: "com.safehill.enkey", category: "BG-FETCH")
    }
    
    public let limit: Int?
    public var delegates: [SHOutboundAssetOperationDelegate]
    var imageManager: PHCachingImageManager
    
    public init(delegates: [SHOutboundAssetOperationDelegate],
                limitPerRun limit: Int? = nil,
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
    
    private func retrieveAsset(withLocalIdentifier localIdentifier: String,
                               groupId: String,
                               sharedWith users: [SHServerUser]) throws -> KBPhotoAsset {
        let dispatch = KBTimedDispatch()
        let photoIndexer = KBPhotosIndexer()
        var kbPhotoAsset: KBPhotoAsset? = nil
        
        photoIndexer.fetchCameraRollAssets(withFilters: [KBPhotosFilter.withLocalIdentifiers([localIdentifier])]) { result in
            if case .failure(let err) = result {
                dispatch.interrupt(err)
                return
            }
            
            let options = PHImageRequestOptions()
            options.isSynchronous = true
            
            guard let phAsset = photoIndexer.indexedAssets.first else {
                dispatch.interrupt(SHBackgroundOperationError.fatalError("No asset with local identifier \(localIdentifier)"))
                return
            }
            
            ///
            /// Fetch the hi-resolution asset if not in the cache, using the same imageManager used to display the asset
            /// Doing this here avoids fetching large amounts of data in the SHEncryptOperation,
            /// as cachedData on the KBPhotoAsset will be set here
            ///
            var cachedData: Data? = nil
            if let d = SHLocalPHAssetHighQualityDataCache.data(forAssetId: phAsset.localIdentifier) {
                cachedData = d
            } else {
                var error: Error? = nil
                phAsset.data(
                    usingImageManager: self.imageManager,
                    synchronousFetch: true
                ) { result in
                    switch result {
                    case .success(let d):
                        cachedData = d
                    case .failure(let err):
                        error = err
                    }
                }
                
                if let error = error {
                    dispatch.interrupt(error)
                    return
                }
            }
            
            self.log.info("caching hi res data in a KBPhotoAsset for later consumption")
            #if DEBUG
            let bcf = ByteCountFormatter()
            bcf.allowedUnits = [.useMB] // optional: restricts the units to MB only
            bcf.countStyle = .file
            self.log.debug("hiRes bytes (\(bcf.string(fromByteCount: Int64(cachedData!.count))))")
            #endif
            
            kbPhotoAsset = KBPhotoAsset(
                for: phAsset,
                cachedData: cachedData,
                usingCachingImageManager: self.imageManager
            )
            dispatch.semaphore.signal()
        }
        
        try dispatch.wait()
        return kbPhotoAsset!
    }
    
    public func markAsFailed(
        localIdentifier: String,
        groupId: String,
        sharedWith users: [SHServerUser]) throws
    {
        // Enquque to failed
        log.info("enqueueing upload request for asset \(localIdentifier) in the FAILED queue")
        
        let failedUploadQueueItem = SHFailedUploadRequestQueueItem(localIdentifier: localIdentifier, groupId: groupId, sharedWith: users)
        
        do { try failedUploadQueueItem.enqueue(in: FailedUploadQueue, with: localIdentifier) }
        catch {
            log.fault("asset \(localIdentifier) failed to upload but will never be recorded as failed because enqueueing to FAILED queue failed: \(error.localizedDescription)")
            throw error
        }
        
        // Dequeue from encryption queue
        log.info("dequeueing upload request for asset \(localIdentifier) from the ENCRYPT queue")
        
        do { _ = try EncryptionQueue.dequeue() }
        catch {
            log.error("asset \(localIdentifier) failed to encrypt but dequeuing from ENCRYPT queue failed, so this operation will be attempted again.")
            throw error
        }
        
#if DEBUG
        log.debug("items in the ENCRYPT queue after dequeueing \((try? EncryptionQueue.peekItems(createdWithin: DateInterval(start: .distantPast, end: Date())))?.count ?? 0)")
#endif
        
        // Notify the delegates
        for delegate in delegates {
            if let delegate = delegate as? SHAssetFetcherDelegate {
                delegate.didFailFetching(
                    itemWithLocalIdentifier: localIdentifier,
                    groupId: groupId
                )
            }
        }
    }
    
    public func markAsSuccessful(
        kbPhotoAsset: KBPhotoAsset,
        groupId: String,
        sharedWith users: [SHServerUser],
        shouldUpload: Bool) throws
    {
        let localIdentifier = kbPhotoAsset.phAsset.localIdentifier
        
        if shouldUpload {
            let encryptionRequest = SHEncryptionRequestQueueItem(asset: kbPhotoAsset, groupId: groupId, sharedWith: users)
            log.info("enqueueing encryption request in the ENCRYPTING queue for asset \(localIdentifier)")
            
            do { try encryptionRequest.enqueue(in: EncryptionQueue, with: localIdentifier) }
            catch {
                log.fault("asset \(localIdentifier) was encrypted but will never be uploaded because enqueueing to UPLOAD queue failed")
                throw error
            }
        } else {
            let encryptionForSharingRequest = SHEncryptionForSharingRequestQueueItem(asset: kbPhotoAsset, groupId: groupId, sharedWith: users)
            log.info("enqueueing encryption request in the SHARE queue for asset \(localIdentifier)")
            
            do { try encryptionForSharingRequest.enqueue(in: ShareQueue, with: localIdentifier) }
            catch {
                log.fault("asset \(localIdentifier) was encrypted but will never be shared because enqueueing to SHARE queue failed")
                throw error
            }
        }
        
        // Dequeue from FetchQueue
        log.info("dequeueing upload request for asset \(localIdentifier) from the FETCH queue")
        
        do { _ = try FetchQueue.dequeue() }
        catch {
            log.warning("asset \(localIdentifier) was fetched but dequeuing failed, so this operation will be attempted again.")
            throw error
        }
        
#if DEBUG
        log.debug("items in the FETCH queue after dequeueing \((try? FetchQueue.peekItems(createdWithin: DateInterval(start: .distantPast, end: Date())))?.count ?? 0)")
#endif
        
        // Notify the delegates
        for delegate in delegates {
            if let delegate = delegate as? SHAssetFetcherDelegate {
                delegate.didCompleteFetching(
                    itemWithLocalIdentifier: localIdentifier,
                    groupId: groupId
                )
            }
        }
    }
    
    public override func main() {
        guard !self.isCancelled else {
            state = .finished
            return
        }
        
        state = .executing
        
        do {
            // Retrieve assets in the queue
            
            var count = 1
            
            while let item = try FetchQueue.peek() {
                if let limit = limit {
                    guard count < limit else {
                        break
                    }
                }
                
                log.info("fetching item \(count), with identifier \(item.identifier) created at \(item.createdAt)")
                
                guard let fetchRequest = try? content(ofQueueItem: item) as? SHLocalFetchRequestQueueItem else {
                    log.error("unexpected data found in FETCH queue. Dequeueing")
                    
                    do { _ = try FetchQueue.dequeue() }
                    catch {
                        log.fault("dequeuing failed of unexpected data in FETCH queue. ATTENTION: this operation will be attempted again.")
                        throw error
                    }
                    
                    throw KBError.unexpectedData(item.content)
                }
                
                for delegate in delegates {
                    if let delegate = delegate as? SHAssetFetcherDelegate {
                        delegate.didStartFetching(
                            itemWithLocalIdentifier: item.identifier,
                            groupId: fetchRequest.groupId
                        )
                    }
                }
                
                guard let kbPhotoAsset = try? self.retrieveAsset(
                    withLocalIdentifier: fetchRequest.assetId,
                    groupId: fetchRequest.groupId,
                    sharedWith: fetchRequest.sharedWith
                ) else {
                    log.error("failed to fetch data for item \(count), with identifier \(item.identifier). Dequeueing item, as it's unlikely to succeed again.")
                    do {
                        try self.markAsFailed(
                            localIdentifier: fetchRequest.assetId,
                            groupId: fetchRequest.groupId,
                            sharedWith: fetchRequest.sharedWith
                        )
                    } catch {
                        log.critical("failed to mark FETCH as failed. This will likely cause infinite loops")
                        // TODO: Handle
                    }
                    
                    return
                }
                
                log.info("[√] fetch task completed for item \(item.identifier)")
                
                do {
                    try self.markAsSuccessful(
                        kbPhotoAsset: kbPhotoAsset,
                        groupId: fetchRequest.groupId,
                        sharedWith: fetchRequest.sharedWith,
                        shouldUpload: fetchRequest.shouldUpload
                    )
                } catch {
                    log.critical("failed to mark FETCH as successful. This will likely cause infinite loops")
                    // TODO: Handle
                }
                
                count += 1
                
                guard !self.isCancelled else {
                    log.info("fetch task cancelled. Finishing")
                    state = .finished
                    break
                }
            }
        } catch {
            log.error("error executing fetch task: \(error.localizedDescription)")
        }
        
        state = .finished
    }
}

public class SHAssetsFetcherQueueProcessor : SHOperationQueueProcessor<SHLocalFetchOperation> {
    /// Singleton (with private initializer)
    public static var shared = SHAssetsFetcherQueueProcessor(
        delayedStartInSeconds: 2,
        dispatchIntervalInSeconds: 2
    )
    
    private override init(delayedStartInSeconds: Int = 0,
                          dispatchIntervalInSeconds: Int? = nil) {
        super.init(delayedStartInSeconds: delayedStartInSeconds, dispatchIntervalInSeconds: dispatchIntervalInSeconds)
    }
}
