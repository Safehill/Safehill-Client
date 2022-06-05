import Foundation
import os
import KnowledgeBase


open class SHUploadOperation: SHAbstractBackgroundOperation, SHBackgroundOperationProtocol {
    
    public let log = Logger(subsystem: "com.safehill.enkey", category: "BG-UPLOAD")
    
    public let limit: Int?
    public let user: SHLocalUser
    public var delegates: [SHOutboundAssetOperationDelegate]
    
    public init(user: SHLocalUser,
                delegates: [SHOutboundAssetOperationDelegate],
                limitPerRun limit: Int? = nil) {
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
            throw KBError.unexpectedData(item.content)
        }
        
        let unarchiver: NSKeyedUnarchiver
        if #available(macOS 10.13, *) {
            unarchiver = try NSKeyedUnarchiver(forReadingFrom: data)
        } else {
            unarchiver = NSKeyedUnarchiver(forReadingWith: data)
        }
        
        guard let uploadRequest = unarchiver.decodeObject(of: SHUploadRequestQueueItem.self, forKey: NSKeyedArchiveRootObjectKey) else {
            throw KBError.unexpectedData(item)
        }
        
        return uploadRequest
    }
    
    private func getLocalAsset(with globalIdentifier: String) throws -> SHEncryptedAsset {
        
        let dispatch = KBTimedDispatch()
        var asset: SHEncryptedAsset? = nil
        
        // Because the encrypter requested the uploader to upload this asset
        // we can assume that the asset at this point is stored encrypted in the on-disk cache
        // but not on the server.
        // Calling `getAssets` will retrieve the encrypted asset from the on-disk cache.
        
        self.serverProxy.getAssets(withGlobalIdentifiers: [globalIdentifier],
                                   versions: nil /* all versions */) { result in
            switch result {
            case .success(let dict):
                guard let a = dict[globalIdentifier] else {
                    dispatch.interrupt(KBError.unexpectedData(dict))
                    return
                }
                asset = a
                dispatch.semaphore.signal()
            case .failure(let error):
                dispatch.interrupt(error)
            }
        }
        
        try dispatch.wait()
        return asset!
    }
    
    private func createServerAsset(_ asset: SHEncryptedAsset) throws -> SHServerAsset {
        let dispatch = KBTimedDispatch()
        
        var serverAsset: SHServerAsset? = nil
        
        serverProxy.create(asset: asset) { result in
            switch result {
            case .success(let obj):
                serverAsset = obj
                dispatch.semaphore.signal()
            case .failure(let error):
                dispatch.interrupt(error)
            }
        }
        
        try dispatch.wait()
        return serverAsset!
    }
    
    private func upload(serverAsset: SHServerAsset,
                        asset: SHEncryptedAsset) throws {
        
        let dispatch = KBTimedDispatch(timeoutInMilliseconds: SHUploadTimeoutInMilliseconds)
        
        serverProxy.upload(serverAsset: serverAsset, asset: asset) { result in
            switch result {
            case .failure(let err):
                dispatch.interrupt(err)
            case .success():
                dispatch.semaphore.signal()
            }
        }
        
        try dispatch.wait()
    }
    
    public func markAsFailed(localIdentifier: String,
                             globalIdentifier: String,
                             groupId: String,
                             sharedWith: [SHServerUser]) throws {
        // Enquque to failed
        log.info("enqueueing upload request for asset \(localIdentifier) in the FAILED queue")
        let failedUploadQueueItem = SHFailedUploadRequestQueueItem(assetId: localIdentifier,
                                                                   groupId: groupId,
                                                                   sharedWith: sharedWith)
        
        do { try failedUploadQueueItem.enqueue(in: FailedUploadQueue, with: localIdentifier) }
        catch {
            log.fault("asset \(localIdentifier) failed to upload but will never be recorded as failed because enqueueing to FAILED queue failed: \(error.localizedDescription)")
            throw error
        }
        
        // Dequeque from UploadQueue
        log.info("dequeueing upload request for asset \(localIdentifier) from the UPLOAD queue")
        
        do { _ = try UploadQueue.dequeue() }
        catch {
            log.error("asset \(localIdentifier) failed to upload but dequeuing from UPLOAD queue failed, so this operation will be attempted again.")
            throw error
        }
        
#if DEBUG
        log.debug("items in the UPLOAD queue after dequeueing \((try? UploadQueue.peekItems(createdWithin: DateInterval(start: .distantPast, end: Date())))?.count ?? 0)")
#endif
        
        // Notify the delegates
        for delegate in delegates {
            if let delegate = delegate as? SHAssetUploaderDelegate {
                delegate.didFailUpload(
                    itemWithLocalIdentifier: localIdentifier,
                    globalIdentifier: globalIdentifier,
                    groupId: groupId
                )
            }
        }
    }
    
    public func markAsSuccessful(
        localIdentifier: String,
        globalIdentifier: String,
        groupId: String,
        sharedWith: [SHServerUser]
    ) throws {
        
        if sharedWith.count > 0 || (sharedWith.count == 1 && sharedWith.first!.identifier == self.user.identifier) {
            log.info("enqueueing upload request in the SHARE queue for asset \(localIdentifier)")

//            let uploadRequest = SHShareRequestQueueItem(
//                localAssetId: localIdentifier,
//                globalAssetId: globalIdentifier,
//                groupId: groupId,
//                sharedWith: sharedWith
//            )
//
//            do { try uploadRequest.enqueue(in: ShareQueue, with: globalIdentifier) }
//            catch {
//                log.fault("asset \(localIdentifier) was encrypted but will never be shared because enqueueing to SHARE queue failed")
//                throw error
//            }
            // TODO: Implement this so that asset fetching from local library is a new operation (move from AUC), and so that after retrieving the asset an item of type SHEncryptionForSharingRequestQueueItem can be enqueued in the ShareQueue
            log.critical("NOT IMPLEMENTED!")
        }
        
        // Enquque to success history
        log.info("UPLOAD succeeded. Enqueueing upload request in the SUCCESS queue (upload history) for asset \(localIdentifier)")
        
        let succesfulUploadQueueItem = SHUploadHistoryItem(assetId: localIdentifier, groupId: groupId, sharedWith: [self.user])
        
        do { try succesfulUploadQueueItem.enqueue(in: UploadHistoryQueue, with: localIdentifier) }
        catch {
            log.fault("asset \(localIdentifier) was upload but will never be recorded as uploaded because enqueueing to SUCCESS queue failed")
            throw error
        }
        
        // Dequeque from UploadQueue
        log.info("dequeueing upload request for asset \(localIdentifier) from the UPLOAD queue")
        
        do { _ = try UploadQueue.dequeue() }
        catch {
            log.warning("asset \(localIdentifier) was uploaded but dequeuing from UPLOAD queue failed, so this operation will be attempted again")
            throw error
        }

#if DEBUG
        log.debug("items in the UPLOAD queue after dequeueing \((try? EncryptionQueue.peekItems(createdWithin: DateInterval(start: .distantPast, end: Date())))?.count ?? 0)")
#endif
        
        // Notify the delegates
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
    
    public override func main() {
        guard !self.isCancelled else {
            state = .finished
            return
        }
        
        state = .executing
        
        do {
            // Retrieve assets in the queue
            
            var count = 1
            
            while let item = try UploadQueue.peek() {
                if let limit = limit {
                    guard count < limit else {
                        break
                    }
                }
                log.info("uploading item \(count), with identifier \(item.identifier) created at \(item.createdAt)")
                
                guard let uploadRequest = try content(ofQueueItem: item) as? SHUploadRequestQueueItem else {
                    throw KBError.unexpectedData(item.content)
                }
                
                let globalIdentifier = uploadRequest.globalAssetId
                let localIdentifier = uploadRequest.localAssetId
                
                for delegate in delegates {
                    if let delegate = delegate as? SHAssetUploaderDelegate {
                        delegate.didStartUpload(
                            itemWithLocalIdentifier: localIdentifier,
                            globalIdentifier: globalIdentifier,
                            groupId: uploadRequest.groupId
                        )
                    }
                }
                
                var encryptedAsset: SHEncryptedAsset? = nil
                
                do {
                    log.info("retrieving encrypted asset from local server proxy: \(globalIdentifier)")
                    encryptedAsset = try self.getLocalAsset(with: globalIdentifier)
                } catch {
                    log.error("failed to retrieve stored data for item \(count), with identifier \(item.identifier). Dequeueing item, as it's unlikely to succeed again.")
                    do {
                        try self.markAsFailed(localIdentifier: localIdentifier,
                                              globalIdentifier: globalIdentifier,
                                              groupId: uploadRequest.groupId,
                                              sharedWith: uploadRequest.sharedWith)
                    } catch {
                        // TODO: Report
                    }
                    
                    continue
                }
                
                var serverAsset: SHServerAsset? = nil
                do {
                    log.info("requesting to create asset on the server: \(String(describing: encryptedAsset?.globalIdentifier))")
                    serverAsset = try self.createServerAsset(encryptedAsset!)
                } catch {
                    log.error("failed to create server asset for item \(count), with identifier \(item.identifier). Dequeueing item, as it's unlikely to succeed again. error=\(error.localizedDescription)")
                    do {
                        try self.markAsFailed(localIdentifier: localIdentifier,
                                              globalIdentifier: globalIdentifier,
                                              groupId: uploadRequest.groupId,
                                              sharedWith: uploadRequest.sharedWith)
                    } catch {
                        // TODO: Report
                    }
                    continue
                }
                
                do {
                    log.info("uploading asset to the CDN: \(String(describing: serverAsset?.globalIdentifier))")
                    try self.upload(serverAsset: serverAsset!,
                                    asset: encryptedAsset!)
                }
                catch {
                    log.error("failed to upload data for item \(count), with identifier \(item.identifier). error=\(error.localizedDescription)")
                    
                    do {
                        try self.markAsFailed(localIdentifier: localIdentifier,
                                              globalIdentifier: globalIdentifier,
                                              groupId: uploadRequest.groupId,
                                              sharedWith: uploadRequest.sharedWith)
                    } catch {
                        // TODO: Report
                    }
                    
                    log.info("removing asset \(globalIdentifier) from server")
                    let dispatch = KBTimedDispatch()
                    
                    serverProxy.deleteAssets(withGlobalIdentifiers: [encryptedAsset!.globalIdentifier]) { [weak self] result in
                        if case .failure(let err) = result {
                            self?.log.info("failed to remove asset \(globalIdentifier) from server: \(err.localizedDescription)")
                        }
                        dispatch.semaphore.signal()
                    }
                    
                    try dispatch.wait()
                    continue
                }

                //
                // Upload is completed so we can create an item in the history queue for this upload
                //
                do {
                    try self.markAsSuccessful(
                        localIdentifier: localIdentifier,
                        globalIdentifier: globalIdentifier,
                        groupId: uploadRequest.groupId,
                        sharedWith: uploadRequest.sharedWith
                    )
                } catch {
                    // TODO: report
                    continue
                }
                
                log.info("[√] upload task completed for item \(item.identifier)")
                
                count += 1
                
                guard !self.isCancelled else {
                    log.info("upload task cancelled. Finishing")
                    state = .finished
                    break
                }
            }
        } catch {
            log.error("error executing upload task: \(error.localizedDescription)")
        }
        
        state = .finished
    }
}

public class SHAssetsUploaderQueueProcessor : SHOperationQueueProcessor<SHUploadOperation> {
    /// Singleton (with private initializer)
    public static var shared = SHAssetsUploaderQueueProcessor(
        delayedStartInSeconds: 2,
        dispatchIntervalInSeconds: 2
    )
    
    private override init(delayedStartInSeconds: Int = 0,
                          dispatchIntervalInSeconds: Int? = nil) {
        super.init(delayedStartInSeconds: delayedStartInSeconds, dispatchIntervalInSeconds: dispatchIntervalInSeconds)
    }
}
