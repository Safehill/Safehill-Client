import Foundation
import os
import KnowledgeBase
import Async

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
        var asset: SHEncryptedAsset? = nil
        var error: Error? = nil
        
        let group = AsyncGroup()
        group.enter()
        self.serverProxy.getLocalAssets(withGlobalIdentifiers: [globalIdentifier],
                                        versions: nil /* all versions */) { result in
            switch result {
            case .success(let dict):
                if let a = dict[globalIdentifier] {
                    asset = a
                } else {
                    error = SHBackgroundOperationError.unexpectedData(dict)
                }
            case .failure(let err):
                error = err
            }
            group.leave()
        }
        
        let dispatchResult = group.wait()
        
        guard dispatchResult != .timedOut else {
            throw SHBackgroundOperationError.timedOut
        }
        
        guard error == nil else {
            throw error!
        }

        return asset!
    }
    
    private func createServerAsset(_ asset: SHEncryptedAsset) throws -> SHServerAsset {
        var serverAsset: SHServerAsset? = nil
        var error: Error? = nil
        
        let group = AsyncGroup()
        
        group.enter()
        self.serverProxy.create(asset: asset) { result in
            switch result {
            case .success(let obj):
                serverAsset = obj
            case .failure(let err):
                error = err
            }
            group.leave()
        }
        
        let dispatchResult = group.wait()
        guard dispatchResult != .timedOut else {
            throw SHBackgroundOperationError.timedOut
        }
        
        guard error == nil else {
            throw error!
        }

        return serverAsset!
    }
    
    private func upload(serverAsset: SHServerAsset,
                        asset: SHEncryptedAsset) throws {
        var error: Error? = nil
        let group = AsyncGroup()
        group.enter()
        self.serverProxy.upload(serverAsset: serverAsset, asset: asset) { result in
            if case .failure(let err) = result {
                error = err
            }
            group.leave()
        }
        let dispatchResult = group.wait(seconds: Double(SHUploadTimeoutInMilliseconds/1000))
        
        guard dispatchResult != .timedOut else {
            throw SHBackgroundOperationError.timedOut
        }
        
        if let error = error {
            throw error
        }
    }
    
    private func deleteAssetFromServer(globalIdentifier: String) throws {
        log.info("deleting asset \(globalIdentifier) from server")
        
        let group = AsyncGroup()
        group.enter()
        self.serverProxy.deleteAssets(withGlobalIdentifiers: [globalIdentifier]) { [weak self] result in
            if case .failure(let err) = result {
                self?.log.info("failed to remove asset \(globalIdentifier) from server: \(err.localizedDescription)")
            }
        }
        let dispatchResult = group.wait()
        
        guard dispatchResult != .timedOut else {
            throw SHBackgroundOperationError.timedOut
        }
    }
    
    public func markAsFailed(localIdentifier: String,
                             globalIdentifier: String,
                             groupId: String,
                             eventOriginator: SHServerUser,
                             sharedWith: [SHServerUser]) throws {
        // Enquque to failed
        log.info("enqueueing upload request for asset \(localIdentifier) in the FAILED queue")
        let failedUploadQueueItem = SHFailedUploadRequestQueueItem(localIdentifier: localIdentifier,
                                                                   groupId: groupId,
                                                                   eventOriginator: eventOriginator,
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
                    groupId: groupId,
                    sharedWith: sharedWith
                )
            }
        }
    }
    
    public func markAsSuccessful(
        localIdentifier: String,
        globalIdentifier: String,
        groupId: String,
        eventOriginator: SHServerUser,
        sharedWith: [SHServerUser]
    ) throws {
        
        // Enquque to success history
        log.info("UPLOAD succeeded. Enqueueing upload request in the SUCCESS queue (upload history) for asset \(globalIdentifier)")
        
        let succesfulUploadQueueItem = SHUploadHistoryItem(localIdentifier: localIdentifier,
                                                           groupId: groupId,
                                                           eventOriginator: eventOriginator,
                                                           sharedWith: [self.user])
        
        do { try succesfulUploadQueueItem.enqueue(in: UploadHistoryQueue, with: localIdentifier) }
        catch {
            log.fault("asset \(localIdentifier) was upload but will never be recorded as uploaded because enqueueing to SUCCESS queue failed")
            throw error
        }
        
        // Dequeque from UploadQueue
        log.info("dequeueing upload request for asset \(globalIdentifier) from the UPLOAD queue")
        
        do { _ = try UploadQueue.dequeue() }
        catch {
            log.warning("asset \(localIdentifier) was uploaded but dequeuing from UPLOAD queue failed, so this operation will be attempted again")
            throw error
        }
        
        // Start the sharing part if needed
        
        let willShare = sharedWith.count > 0 || (sharedWith.count == 1 && sharedWith.first!.identifier == self.user.identifier)
        
        if willShare { // Enquque to FETCH queue (fetch is needed for encrypting for sharing)
            log.info("enqueueing upload request in the FETCH+SHARE queue for asset \(localIdentifier)")

            let fetchRequest = SHLocalFetchRequestQueueItem(
                localIdentifier: localIdentifier,
                groupId: groupId + "-share",
                eventOriginator: eventOriginator,
                sharedWith: sharedWith,
                shouldUpload: false
            )

            do { try fetchRequest.enqueue(in: FetchQueue, with: localIdentifier) }
            catch {
                log.fault("asset \(localIdentifier) was uploaded but will never be shared because enqueueing to FETCH queue failed")
                throw error
            }
        }

#if DEBUG
        log.debug("items in the UPLOAD queue after dequeueing \((try? UploadQueue.peekItems(createdWithin: DateInterval(start: .distantPast, end: Date())))?.count ?? 0)")
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
                
                guard let uploadRequest = try? content(ofQueueItem: item) as? SHUploadRequestQueueItem else {
                    log.error("unexpected data found in UPLOAD queue. Dequeueing")
                    
                    do { _ = try UploadQueue.dequeue() }
                    catch {
                        log.fault("dequeuing failed of unexpected data in UPLOAD queue. ATTENTION: this operation will be attempted again.")
                        throw error
                    }
                    
                    throw KBError.unexpectedData(item.content)
                }
                
                let globalIdentifier = uploadRequest.globalAssetId
                let localIdentifier = uploadRequest.assetId
                
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
                    log.error("failed to retrieve stored data for item \(count), with identifier \(item.identifier): \(error.localizedDescription). Dequeueing item, as it's unlikely to succeed again.")
                    do {
                        try self.markAsFailed(localIdentifier: localIdentifier,
                                              globalIdentifier: globalIdentifier,
                                              groupId: uploadRequest.groupId,
                                              eventOriginator: uploadRequest.eventOriginator,
                                              sharedWith: uploadRequest.sharedWith)
                    } catch {
                        log.critical("failed to mark UPLOAD as failed. This will likely cause infinite loops")
                        // TODO: Handle
                    }
                    
                    continue
                }
                
                guard globalIdentifier == encryptedAsset?.globalIdentifier else {
                    do {
                        try self.markAsFailed(localIdentifier: localIdentifier,
                                              globalIdentifier: globalIdentifier,
                                              groupId: uploadRequest.groupId,
                                              eventOriginator: uploadRequest.eventOriginator,
                                              sharedWith: uploadRequest.sharedWith)
                    } catch {
                        log.critical("failed to mark UPLOAD as failed. This will likely cause infinite loops")
                        // TODO: Handle
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
                                              eventOriginator: uploadRequest.eventOriginator,
                                              sharedWith: uploadRequest.sharedWith)
                    } catch {
                        log.critical("failed to mark UPLOAD as failed. This will likely cause infinite loops")
                        // TODO: Handle
                    }
                    continue
                }
                
                guard globalIdentifier == serverAsset?.globalIdentifier else {
                    do {
                        try self.markAsFailed(localIdentifier: localIdentifier,
                                              globalIdentifier: globalIdentifier,
                                              groupId: uploadRequest.groupId,
                                              eventOriginator: uploadRequest.eventOriginator,
                                              sharedWith: uploadRequest.sharedWith)
                    } catch {
                        log.critical("failed to mark UPLOAD as failed. This will likely cause infinite loops")
                        // TODO: Handle
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
                                              eventOriginator: uploadRequest.eventOriginator,
                                              sharedWith: uploadRequest.sharedWith)
                    } catch {
                        log.critical("failed to mark UPLOAD as failed. This will likely cause infinite loops")
                        // TODO: Handle
                    }
                    
                    try self.deleteAssetFromServer(globalIdentifier: globalIdentifier)
                    
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
                        eventOriginator: uploadRequest.eventOriginator,
                        sharedWith: uploadRequest.sharedWith
                    )
                } catch {
                    log.critical("failed to mark UPLOAD as successful. This will likely cause infinite loops")
                    // TODO: Handle
                    continue
                }
                
                log.info("[√] upload task completed for item \(count) with identifier \(item.identifier)")
                
                count += 1
                
                guard !self.isCancelled else {
                    log.info("upload task cancelled. Finishing")
                    state = .finished
                    break
                }
            }
        } catch {
            log.error("error executing upload task: \(error.localizedDescription). ATTENTION: This operation will be attempted again")
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
