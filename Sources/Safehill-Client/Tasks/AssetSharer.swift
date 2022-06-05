//import Foundation
//import os
//import KnowledgeBase
//
//open class SHShareOperation: SHAbstractBackgroundOperation, SHBackgroundOperationProtocol, SHBackgroundUploadOperationProtocol {
//    
//    public let log = Logger(subsystem: "com.safehill.enkey", category: "BG-SHARE")
//    
//    public let limit: Int?
//    public let user: SHLocalUser
//    public var delegates: [SHAssetSharerDelegate]
//    
//    public init(user: SHLocalUser,
//                delegates: [SHAssetSharerDelegate],
//                limitPerRun limit: Int? = nil) {
//        self.user = user
//        self.limit = limit
//        self.delegates = delegates
//    }
//    
//    public var serverProxy: SHServerProxy {
//        SHServerProxy(user: self.user)
//    }
//    
//    public func clone() -> SHBackgroundOperationProtocol {
//        SHShareOperation(
//            user: self.user,
//            delegates: self.delegates,
//            limitPerRun: self.limit
//        )
//    }
//    
//    public func content(ofQueueItem item: KBQueueItem) throws -> SHGroupableUploadQueueItem {
//        guard let data = item.content as? Data else {
//            throw KBError.unexpectedData(item.content)
//        }
//        
//        let unarchiver: NSKeyedUnarchiver
//        if #available(macOS 10.13, *) {
//            unarchiver = try NSKeyedUnarchiver(forReadingFrom: data)
//        } else {
//            unarchiver = NSKeyedUnarchiver(forReadingWith: data)
//        }
//        
//        guard let uploadRequest = unarchiver.decodeObject(of: SHShareRequestQueueItem.self, forKey: NSKeyedArchiveRootObjectKey) else {
//            throw KBError.unexpectedData(item)
//        }
//        
//        return uploadRequest
//    }
//    
//    public func markAsFailed(
//        localIdentifier: String,
//        globalIdentifier: String,
//        groupId: String,
//        sharedWith users: [SHServerUser],
//        requeue: Bool = false) throws
//    {
//        // Enquque to failed
//        log.info("enqueueing share request for asset \(localIdentifier) in the FAILED queue")
//        
//        let failedUploadQueueItem = SHFailedUploadRequestQueueItem(assetId: localIdentifier, groupId: groupId, sharedWith: users)
//        
//        do { try failedUploadQueueItem.enqueue(in: FailedUploadQueue, with: localIdentifier) }
//        catch {
//            log.fault("asset \(localIdentifier) failed to upload but will never be recorded as failed because enqueueing to FAILED queue failed: \(error.localizedDescription)")
//            throw error
//        }
//        
//        // Requeue from encryption queue
//        log.info("\(requeue ? "requeueing" : "dequeueing") share request for asset \(localIdentifier) from the SHARE queue")
//        
//        do {
//            let item = try ShareQueue.dequeue()!
//            if requeue {
//                try ShareQueue.enqueue(item.content, withIdentifier: item.identifier)
//            }
//        }
//        catch {
//            if requeue {
//                log.critical("asset \(localIdentifier) failed to share but requeuing from SHARE queue failed. This asset sharing information may be lost, or the queue can be held by this item if dequeueing is failing")
//            } else {
//                log.critical("asset \(localIdentifier) failed to share but dequeueing from SHARE queue failed. Sharing will be attempted again")
//            }
//            throw error
//        }
//        
//        // Notify the delegates
//        for delegate in delegates {
//            delegate.didFailSharing(
//                itemWithLocalIdentifier: localIdentifier,
//                globalIdentifier: globalIdentifier,
//                groupId: groupId
//            )
//        }
//    }
//    
//    public func markAsSuccessful(
//        localIdentifier: String,
//        globalIdentifier: String,
//        groupId: String,
//        sharedWith: [SHServerUser]
//    ) throws {
//        // Enquque to success history
//        log.info("SHARING succeeded. Enqueueing sharing upload request in the SUCCESS queue (upload history) for asset \(localIdentifier)")
//        
//        let succesfulUploadQueueItem = SHUploadHistoryItem(assetId: localIdentifier, groupId: groupId, sharedWith: sharedWith)
//        
//        do { try succesfulUploadQueueItem.enqueue(in: UploadHistoryQueue, with: localIdentifier) }
//        catch {
//            log.fault("asset \(localIdentifier) was shared but will never be recorded as shared because enqueueing to SUCCESS queue failed")
//            throw error
//        }
//        
//        // Dequeque from ShareQueue
//        log.info("dequeueing upload request for asset \(localIdentifier) from the SHARE queue")
//        
//        do { _ = try ShareQueue.dequeue() }
//        catch {
//            log.warning("asset \(localIdentifier) was uploaded but dequeuing from UPLOAD queue failed, so this operation will be attempted again")
//            throw error
//        }
//        
//        // Notify the delegates
//        for delegate in delegates {
//            delegate.didCompleteSharing(
//                itemWithLocalIdentifier: localIdentifier,
//                globalIdentifier: globalIdentifier,
//                groupId: groupId
//            )
//        }
//    }
//    
//    public override func main() {
//        guard !self.isCancelled else {
//            state = .finished
//            return
//        }
//        
//        state = .executing
//        
//        do {
//            // Retrieve assets in the queue
//            
//            var count = 1
//            
//            while let item = try ShareQueue.peek() {
//                if let limit = limit {
//                    guard count < limit else {
//                        break
//                    }
//                }
//                log.info("sharing item \(count), with identifier \(item.identifier) created at \(item.createdAt)")
//                
//                guard let uploadRequest = try content(ofQueueItem: item) as? SHShareRequestQueueItem else {
//                    throw KBError.unexpectedData(item.content)
//                }
//                
//                log.info("sharing it with users \(uploadRequest.sharedWith.map { $0.identifier })")
//                
//                let globalIdentifier = uploadRequest.globalAssetId
//                let localIdentifier = uploadRequest.localAssetId
//                let newGroupId = UUID().uuidString
//                
//                for delegate in delegates {
//                    delegate.didStartSharing(
//                        itemWithLocalIdentifier: localIdentifier,
//                        groupId: uploadRequest.groupId,
//                        newGroupId: newGroupId
//                    )
//                }
//                
//                let dispatch = KBTimedDispatch()
//                
//                dispatch.group.enter()
//                self.serverProxy.getLocalSharingInfo(
//                    forAssetIdentifier: globalIdentifier,
//                    for: uploadRequest.sharedWith
//                ) { result in
//                    switch result {
//                    case .success(let shareableEncryptedAsset):
//                        guard let shareableEncryptedAsset = shareableEncryptedAsset else {
//                            dispatch.interrupt(SHAssetFetchError.fatalError("Asset sharing information wasn't stored as expected during the encrypt step"))
//                            return
//                        }
//                        self.serverProxy.share(shareableEncryptedAsset) { shareResult in
//                            switch shareResult {
//                            case .success():
//                                dispatch.group.leave()
//                            case .failure(let err):
//                                dispatch.interrupt(err)
//                            }
//                        }
//                    case .failure(let err):
//                        dispatch.interrupt(err)
//                    }
//                }
//                
//                do {
//                    try dispatch.wait()
//                } catch SHAssetFetchError.fatalError(let errorMsg) {
//                    log.error("failed to share with users \(uploadRequest.sharedWith.map { $0.identifier }): \(errorMsg)")
//                    
//                    do {
//                        try self.markAsFailed(localIdentifier: item.identifier,
//                                              globalIdentifier: globalIdentifier,
//                                              groupId: newGroupId,
//                                              sharedWith: uploadRequest.sharedWith,
//                                              requeue: false)
//                    } catch {
//                        // TODO: Report
//                    }
//                    
//                    continue
//                    
//                } catch {
//                    log.error("failed to share with users \(uploadRequest.sharedWith.map { $0.identifier }): \(error.localizedDescription)")
//                    
//                    do {
//                        try self.markAsFailed(localIdentifier: item.identifier,
//                                              globalIdentifier: globalIdentifier,
//                                              groupId: newGroupId,
//                                              sharedWith: uploadRequest.sharedWith,
//                                              requeue: true)
//                    } catch {
//                        // TODO: Report
//                    }
//                    
//                    continue
//                }
//                
//                log.info("[√] share task completed for item \(item.identifier)")
//                
//                do {
//                    try self.markAsSuccessful(
//                        localIdentifier: localIdentifier,
//                        globalIdentifier: globalIdentifier,
//                        groupId: newGroupId,
//                        sharedWith: uploadRequest.sharedWith
//                    )
//                } catch {
//                    // TODO: Report
//                }
//                
//                count += 1
//                
//                guard !self.isCancelled else {
//                    log.info("share task cancelled. Finishing")
//                    state = .finished
//                    break
//                }
//            }
//        } catch {
//            log.error("error executing share task: \(error.localizedDescription)")
//        }
//        
//        state = .finished
//    }
//}
//
//public class SHAssetShareQueueProcessor : SHOperationQueueProcessor<SHShareOperation> {
//    /// Singleton (with private initializer)
//    public static var shared = SHAssetShareQueueProcessor(
//        delayedStartInSeconds: 2,
//        dispatchIntervalInSeconds: 2
//    )
//    
//    private override init(delayedStartInSeconds: Int = 0,
//                          dispatchIntervalInSeconds: Int? = nil) {
//        super.init(delayedStartInSeconds: delayedStartInSeconds, dispatchIntervalInSeconds: dispatchIntervalInSeconds)
//    }
//}
