import Foundation
import os
import KnowledgeBase


open class SHEncryptAndShareOperation: SHEncryptionOperation {
    
    public override var log: Logger {
        Logger(subsystem: "com.safehill.enkey", category: "BG-SHARE")
    }
    
    public override func clone() -> SHBackgroundOperationProtocol {
        SHEncryptAndShareOperation(
            user: self.user,
            delegates: self.delegates,
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
        localIdentifier: String,
        groupId: String,
        sharedWith users: [SHServerUser]) throws
    {
        try self.markAsFailed(
            localIdentifier: localIdentifier,
            globalIdentifier: "",
            groupId: groupId,
            sharedWith: users
        )
    }
    
    public func markAsFailed(
        localIdentifier: String,
        globalIdentifier: String,
        groupId: String,
        sharedWith users: [SHServerUser]) throws
    {
        // Enquque to failed
        log.info("enqueueing share request for asset \(localIdentifier) in the FAILED queue")
        
        let failedShare = SHFailedShareRequestQueueItem(localIdentifier: localIdentifier, groupId: groupId, sharedWith: users)
        
        do { try failedShare.enqueue(in: FailedShareQueue, with: localIdentifier) }
        catch {
            log.fault("asset \(localIdentifier) failed to upload but will never be recorded as failed because enqueueing to FAILED queue failed: \(error.localizedDescription)")
            throw error
        }
        
        do { _ = try ShareQueue.dequeue() }
        catch {
            log.error("asset \(localIdentifier) failed to share but dequeueing from SHARE queue failed. Sharing will be attempted again")
            throw error
        }
        
        // Notify the delegates
        for delegate in delegates {
            if let delegate = delegate as? SHAssetSharerDelegate {
                delegate.didFailSharing(
                    itemWithLocalIdentifier: localIdentifier,
                    globalIdentifier: globalIdentifier,
                    groupId: groupId,
                    with: users
                )
            }
        }
    }
    
    public override func markAsSuccessful(
        localIdentifier: String,
        globalIdentifier: String,
        groupId: String,
        sharedWith users: [SHServerUser]
    ) throws {
        // Enquque to success history
        log.info("SHARING succeeded. Enqueueing sharing upload request in the SUCCESS queue (upload history) for asset \(localIdentifier)")
        
        let succesfulUploadQueueItem = SHShareHistoryItem(localIdentifier: localIdentifier, groupId: groupId, sharedWith: users)
        
        do { try succesfulUploadQueueItem.enqueue(in: ShareHistoryQueue, with: localIdentifier) }
        catch {
            log.fault("asset \(localIdentifier) was shared but will never be recorded as shared because enqueueing to SUCCESS queue failed")
            throw error
        }
        
        // Dequeque from ShareQueue
        log.info("dequeueing upload request for asset \(localIdentifier) from the SHARE queue")
        
        do { _ = try ShareQueue.dequeue() }
        catch {
            log.warning("asset \(localIdentifier) was uploaded but dequeuing from UPLOAD queue failed, so this operation will be attempted again")
            throw error
        }
        
        // Notify the delegates
        for delegate in delegates {
            if let delegate = delegate as? SHAssetSharerDelegate {
                delegate.didCompleteSharing(
                    itemWithLocalIdentifier: localIdentifier,
                    globalIdentifier: globalIdentifier,
                    groupId: groupId,
                    with: users
                )
            }
        }
    }
    
    private func storeSecrets(
        request: SHEncryptionForSharingRequestQueueItem,
        encryptedAsset: SHEncryptedAsset
    ) throws {
        let dispatch = KBTimedDispatch()
        
        log.info("storing asset \(encryptedAsset.localIdentifier ?? encryptedAsset.globalIdentifier) sharing information in local server proxy")
        
        var shareableEncryptedVersions = [SHShareableEncryptedAssetVersion]()
        for otherUser in request.sharedWith {
            for quality in [SHAssetQuality.lowResolution, SHAssetQuality.hiResolution] {
                let encryptedVersion = encryptedAsset.encryptedVersions.first(where: { $0.quality == quality })!
                let shareableEncryptedVersion = SHGenericShareableEncryptedAssetVersion(
                    quality: quality,
                    userPublicIdentifier: otherUser.identifier,
                    encryptedSecret: encryptedVersion.encryptedSecret
                )
                shareableEncryptedVersions.append(shareableEncryptedVersion)
            }
        }
        
        let shareableEncryptedAsset = SHGenericShareableEncryptedAsset(
            globalIdentifier: encryptedAsset.globalIdentifier,
            sharedVersions: shareableEncryptedVersions
        )
        
        serverProxy.shareAssetLocally(shareableEncryptedAsset) { result in
            switch result {
            case .success():
                dispatch.semaphore.signal()
            case .failure(let err):
                dispatch.interrupt(err)
            }
        }
        
        try dispatch.wait()
    }
    
    private func share(
        encryptedAsset: SHEncryptedAsset,
        via request: SHEncryptionForSharingRequestQueueItem
    ) throws {
        let dispatch = KBTimedDispatch()
        
        self.serverProxy.getLocalSharingInfo(
            forAssetIdentifier: encryptedAsset.globalIdentifier,
            for: request.sharedWith
        ) { result in
            switch result {
            case .success(let shareableEncryptedAsset):
                guard let shareableEncryptedAsset = shareableEncryptedAsset else {
                    dispatch.interrupt(SHBackgroundOperationError.fatalError("Asset sharing information wasn't stored as expected during the encrypt step"))
                    return
                }
                self.serverProxy.share(shareableEncryptedAsset) { shareResult in
                    switch shareResult {
                    case .success():
                        dispatch.semaphore.signal()
                    case .failure(let err):
                        dispatch.interrupt(err)
                    }
                }
            case .failure(let err):
                dispatch.interrupt(err)
            }
        }
        
        try dispatch.wait()
    }
    
    public override func main() {
        guard !self.isCancelled else {
            state = .finished
            return
        }
        
        state = .executing
        
        do {
            ///
            /// Retrieve assets in the queue
            ///
            var count = 1
            
            while let item = try ShareQueue.peek() {
                if let limit = limit {
                    guard count < limit else {
                        break
                    }
                }
                
                log.info("encrypting and sharing item \(count), with identifier \(item.identifier) created at \(item.createdAt)")
                
                guard let shareRequest = try? content(ofQueueItem: item) as? SHEncryptionForSharingRequestQueueItem else {
                    log.error("unexpected data found in SHARE queue. Dequeueing")
                    
                    do { _ = try ShareQueue.dequeue() }
                    catch {
                        log.fault("dequeuing failed of unexpected data in SHARE queue. ATTENTION: this operation will be attempted again.")
                        throw error
                    }
                    
                    throw KBError.unexpectedData(item.content)
                }
                
                guard shareRequest.sharedWith.count > 0 else {
                    log.error("empty sharing information in SHEncryptionForSharingRequestQueueItem object. SHEncryptAndShareOperation can only operate on sharing operations, which require user identifiers")
                    throw KBError.unexpectedData(item.content)
                }
                
                log.info("sharing it with users \(shareRequest.sharedWith.map { $0.identifier })")
                
                let asset = shareRequest.asset
                
                for delegate in delegates {
                    if let delegate = delegate as? SHAssetSharerDelegate {
                        delegate.didStartSharing(
                            itemWithLocalIdentifier: item.identifier,
                            groupId: shareRequest.groupId,
                            with: shareRequest.sharedWith
                        )
                    }
                }
                
                ///
                /// Start encryption for the users it's shared with
                ///
                
                guard let encryptedAsset = try? self.generateEncryptedAsset(
                    for: asset,
                    item: item,
                    request: shareRequest
                ) else {
                    continue
                }
                
                ///
                /// Store sharing information in the local server proxy
                ///
                
                do {
                    try self.storeSecrets(request: shareRequest, encryptedAsset: encryptedAsset)
                } catch {
                    log.error("failed to locally share encrypted item \(count) with users \(shareRequest.sharedWith.map { $0.identifier }): \(error.localizedDescription)")
                    
                    try self.markAsFailed(
                        localIdentifier: item.identifier,
                        groupId: shareRequest.groupId,
                        sharedWith: shareRequest.sharedWith
                    )
                    
                    continue
                }
                
                log.info("successfully stored asset \(encryptedAsset.globalIdentifier) sharing information in local server proxy")
                
                ///
                /// Share using Safehill Server API
                ///
                
                do {
                    try self.share(encryptedAsset: encryptedAsset, via: shareRequest)
                } catch SHBackgroundOperationError.fatalError(let errorMsg) {
                    log.error("failed to share with users \(shareRequest.sharedWith.map { $0.identifier }): \(errorMsg)")
                    
                    try self.markAsFailed(
                        localIdentifier: item.identifier,
                        globalIdentifier: encryptedAsset.globalIdentifier,
                        groupId: shareRequest.groupId,
                        sharedWith: shareRequest.sharedWith
                    )
                    
                    continue
                    
                } catch {
                    log.error("failed to share with users \(shareRequest.sharedWith.map { $0.identifier }): \(error.localizedDescription)")
                    
                    try self.markAsFailed(
                        localIdentifier: item.identifier,
                        globalIdentifier: encryptedAsset.globalIdentifier,
                        groupId: shareRequest.groupId,
                        sharedWith: shareRequest.sharedWith
                    )
                    
                    continue
                }
                
                ///
                /// Finish
                ///
                
                log.info("[√] share task completed for item \(item.identifier)")
                
                try self.markAsSuccessful(
                    localIdentifier: item.identifier,
                    globalIdentifier: encryptedAsset.globalIdentifier,
                    groupId: shareRequest.groupId,
                    sharedWith: shareRequest.sharedWith
                )
                
                count += 1
                
                guard !self.isCancelled else {
                    log.info("share task cancelled. Finishing")
                    state = .finished
                    break
                }
            }
        } catch KBError.unexpectedData(_) {
            log.error("error executing share task. Unexpected data in the queue, dequeueing.")
            _ = try? ShareQueue.dequeue()
        } catch {
            log.error("error executing share task: \(error.localizedDescription)")
        }
        
        state = .finished
    }
}

public class SHAssetEncryptAndShareQueueProcessor : SHOperationQueueProcessor<SHEncryptAndShareOperation> {
    /// Singleton (with private initializer)
    public static var shared = SHAssetEncryptAndShareQueueProcessor(
        delayedStartInSeconds: 2,
        dispatchIntervalInSeconds: 2
    )
    
    private override init(delayedStartInSeconds: Int = 0,
                          dispatchIntervalInSeconds: Int? = nil) {
        super.init(delayedStartInSeconds: delayedStartInSeconds, dispatchIntervalInSeconds: dispatchIntervalInSeconds)
    }
}
