import Foundation
import os
import KnowledgeBase


internal class SHEncryptAndShareOperation: SHEncryptionOperation {
    
    override var operationType: BackgroundOperationQueue.OperationType { .share }
    override var processingState: ProcessingState { .sharing }
    
    public override var log: Logger {
        Logger(subsystem: "com.gf.safehill", category: "BG-SHARE")
    }
    
    let delegatesQueue = DispatchQueue(label: "com.safehill.encryptAndShare.delegates")
    
    public override func clone() -> any SHBackgroundOperationProtocol {
        SHEncryptAndShareOperation(
            user: self.user,
            assetsDelegates: self.assetDelegates,
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
        item: KBQueueItem,
        encryptionRequest request: SHEncryptionRequestQueueItem,
        globalIdentifier: String?,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        do {
            try self.markAsFailed(encryptionRequest: request, queueItem: item)
            completionHandler(.success(()))
        } catch {
            completionHandler(.failure(error))
        }
        
    }
    
    public func markAsFailed(
        encryptionRequest request: SHEncryptionRequestQueueItem,
        queueItem: KBQueueItem
    ) throws {
        let localIdentifier = request.localIdentifier
        let versions = request.versions
        let groupId = request.groupId
        let eventOriginator = request.eventOriginator
        let users = request.sharedWith
        let shouldLinkToThread = request.shouldLinkToThread
        
        do { _ = try BackgroundOperationQueue.of(type: .share).dequeue(item: queueItem) }
        catch {
            log.error("asset \(localIdentifier) failed to share but dequeueing from SHARE queue failed. Sharing will be attempted again")
            throw error
        }
        
        /// Enquque to failed
        log.info("enqueueing share request for asset \(localIdentifier) versions \(versions) in the FAILED queue")
        
        let failedShare = SHFailedShareRequestQueueItem(
            localIdentifier: localIdentifier,
            versions: versions,
            groupId: groupId,
            eventOriginator: eventOriginator,
            sharedWith: users,
            shouldLinkToThread: shouldLinkToThread,
            isBackground: request.isBackground
        )

        do {
            let failedShareQueue = try BackgroundOperationQueue.of(type: .failedShare)
            let successfulShareQueue = try BackgroundOperationQueue.of(type: .successfulShare)
            try failedShare.enqueue(in: failedShareQueue)
            
            /// Remove items in the `ShareHistoryQueue` for the same identifier
            let _ = try? successfulShareQueue.removeValues(forKeysMatching: KBGenericCondition(.equal, value: failedShare.identifier))
        }
        catch {
            log.fault("asset \(localIdentifier) failed to share but will never be recorded as 'failed to share' because enqueueing to SHARE FAILED queue failed: \(error.localizedDescription)")
            throw error
        }
        
        guard request.isBackground == false else {
            /// Avoid other side-effects for background  `SHEncryptionRequestQueueItem`
            return
        }
        
        /// Notify the delegates
        for delegate in assetDelegates {
            if let delegate = delegate as? SHAssetSharerDelegate {
                delegate.didFailSharing(queueItemIdentifier: failedShare.identifier)
            }
        }
    }
    
    public override func markAsSuccessful(
        item: KBQueueItem,
        encryptionRequest request: SHEncryptionRequestQueueItem,
        globalIdentifier: String
    ) throws {
        let localIdentifier = request.localIdentifier
        let versions = request.versions
        let groupId = request.groupId
        let eventOriginator = request.eventOriginator
        let users = request.sharedWith
        let shouldLinkToThread = request.shouldLinkToThread
        
        /// Dequeque from ShareQueue
        log.info("dequeueing request for asset \(localIdentifier) from the SHARE queue")
        
        do { _ = try BackgroundOperationQueue.of(type: .share).dequeue(item: item) }
        catch {
            log.warning("asset \(localIdentifier) was uploaded but dequeuing from UPLOAD queue failed, so this operation will be attempted again")
        }
        
        let successfulShare = SHShareHistoryItem(
            localAssetId: localIdentifier,
            globalAssetId: globalIdentifier,
            versions: versions,
            groupId: groupId,
            eventOriginator: eventOriginator,
            sharedWith: users,
            shouldLinkToThread: shouldLinkToThread,
            isBackground: request.isBackground
        )
        
        do {
            /// Enquque to ShareHistoryQueue
            log.info("SHARING succeeded. Enqueueing sharing upload request in the SUCCESS queue (upload history) for asset \(localIdentifier)")
            let failedShareQueue = try BackgroundOperationQueue.of(type: .failedShare)
            let successfulShareQueue = try BackgroundOperationQueue.of(type: .successfulShare)
            
            /// Remove items in the `FailedShareQueue` for the same identifier
            let _ = try failedShareQueue.removeValues(forKeysMatching: KBGenericCondition(.equal, value: successfulShare.identifier))
            
            if request.isBackground == false {
                /// Remove items in the `ShareHistoryQueue` for the same asset identifier and group identifier but different users
                /// The contract with the client when adding users to an existing share is to ask to share with both new and old users.
                /// Which, in turn, means that the old share history item can be safely removed once the new one is inserted,
                /// as the new one will have both old and new users.
                let _ = try successfulShareQueue.removeValues(
                    forKeysMatching: KBGenericCondition(
                        .beginsWith,
                        value: [
                            localIdentifier, groupId
                        ].joined(separator: "+")
                    )
                )
            }
            
            try successfulShare.enqueue(in: successfulShareQueue)
        }
        catch {
            log.fault("asset \(localIdentifier) was shared but will never be recorded as shared because enqueueing to SUCCESS queue failed")
            throw error
        }
        
        guard request.isBackground == false else {
            /// Avoid other side-effects for background  `SHEncryptionRequestQueueItem`
            return
        }
        
        /// Notify the delegates
        for delegate in assetDelegates {
            if let delegate = delegate as? SHAssetSharerDelegate {
                delegate.didCompleteSharing(queueItemIdentifier: successfulShare.identifier)
            }
        }
    }
    
    private func storeSecrets(
        request: SHEncryptionForSharingRequestQueueItem,
        globalIdentifier: String,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        self.user.shareableEncryptedAsset(
            globalIdentifier: globalIdentifier,
            versions: request.versions,
            recipients: request.sharedWith,
            groupId: request.groupId
        ) { result in
            switch result {
            case .failure(let error):
                completionHandler(.failure(error))
            case .success(let shareableEncryptedAsset):
                self.serverProxy.shareAssetLocally(
                    shareableEncryptedAsset,
                    completionHandler: completionHandler
                )
            }
        }
    }
    
    private func share(
        globalIdentifier: String,
        via request: SHEncryptionForSharingRequestQueueItem,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        self.serverProxy.getLocalSharingInfo(
            forAssetIdentifier: globalIdentifier,
            for: request.sharedWith
        ) { result in
            switch result {
            case .success(let shareableEncryptedAsset):
                guard let shareableEncryptedAsset = shareableEncryptedAsset else {
                    let error = SHBackgroundOperationError.fatalError("Asset sharing information wasn't stored as expected during the encrypt step")
                    completionHandler(.failure(error))
                    return
                }
                
                self.serverProxy.share(
                    shareableEncryptedAsset,
                    shouldLinkToThread: request.shouldLinkToThread,
                    suppressNotification: request.isBackground
                ) { shareResult in
                    if case .failure(let err) = shareResult {
                        completionHandler(.failure(err))
                        return
                    }
                    completionHandler(.success(()))
                }
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    private func initializeGroupAndThread(
        shareRequest: SHEncryptionForSharingRequestQueueItem,
        skipThreadCreation: Bool,
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        var errorInitializingGroup: Error? = nil
        var errorInitializingThread: Error? = nil
        let dispatchGroup = DispatchGroup()
        
        var usersAndSelf = shareRequest.sharedWith
        usersAndSelf.append(self.user)
        
        self.log.debug("creating or updating group for request \(shareRequest.identifier)")
        dispatchGroup.enter()
        self.interactionsController.setupGroupEncryptionDetails(
            groupId: shareRequest.groupId,
            with: usersAndSelf
        ) { initializeGroupResult in
            switch initializeGroupResult {
            case .failure(let error):
                errorInitializingGroup = error
            default: break
            }
            dispatchGroup.leave()
        }
        
        if skipThreadCreation == false {
            self.log.debug("creating or updating thread for request \(shareRequest.identifier)")
            dispatchGroup.enter()
            self.interactionsController.setupThread(
                with: usersAndSelf
            ) {
                setupThreadResult in
                switch setupThreadResult {
                case .success:
                    break
                case .failure(let error):
                    errorInitializingThread = error
                }
                dispatchGroup.leave()
            }
        } else {
            self.log.info("skipping thread creation as instructed by request \(shareRequest.identifier)")
        }
        
        dispatchGroup.notify(queue: .global(qos: qos)) {
            guard errorInitializingGroup == nil,
                  errorInitializingThread == nil else {
                self.log.error("failed to initialize thread or group. \((errorInitializingGroup ?? errorInitializingThread!).localizedDescription)")
                // Mark as failed on any other error
                completionHandler(.failure(errorInitializingGroup ?? errorInitializingThread!))
                return
            }
            
            completionHandler(.success(()))
        }
    }
    
    internal override func process(
        _ item: KBQueueItem,
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        let shareRequest: SHEncryptionForSharingRequestQueueItem
        
        do {
            let content = try content(ofQueueItem: item)
            guard let content = content as? SHEncryptionForSharingRequestQueueItem else {
                ///
                /// Delegates can't be called as item content can't be read and it will be silently removed from the queue
                ///
                log.error("unexpected data found in SHARE queue. Dequeueing")
                throw SHBackgroundOperationError.unexpectedData(item.content)
            }
            shareRequest = content
        } catch {
            do { _ = try BackgroundOperationQueue.of(type: .share).dequeue(item: item) }
            catch {
                log.fault("dequeuing failed of unexpected data in SHARE queue. \(error.localizedDescription)")
            }
            completionHandler(.failure(error))
            return
        }
        
        let handleError = { (error: Error) in
            self.log.critical("Error in SHARE asset: \(error.localizedDescription)")
            do {
                try self.markAsFailed(
                    encryptionRequest: shareRequest,
                    queueItem: item
                )
            } catch {
                self.log.critical("failed to mark SHARE as failed. This will likely cause infinite loops")
                // TODO: Handle
            }
            completionHandler(.failure(error))
        }
        
        guard shareRequest.sharedWith.count > 0 else {
            handleError(SHBackgroundOperationError.fatalError("empty sharing information in SHEncryptionForSharingRequestQueueItem object. SHEncryptAndShareOperation can only operate on sharing operations, which require user identifiers"))
            return
        }
        
        log.info("sharing it with users \(shareRequest.sharedWith.map { $0.identifier })")
        
        if shareRequest.isBackground == false {
            for delegate in assetDelegates {
                if let delegate = delegate as? SHAssetSharerDelegate {
                    delegate.didStartSharing(queueItemIdentifier: shareRequest.identifier)
                }
            }
        }
        
        let globalIdentifier: GlobalIdentifier

        do {
            ///
            /// At this point the global identifier should be calculated by the `SHLocalFetchOperation`,
            /// serialized and deserialized as part of the `SHApplePhotoAsset` object.
            ///
            globalIdentifier = try shareRequest.asset.retrieveOrGenerateGlobalIdentifier()
        } catch {
            handleError(error)
            return
        }
        
        ///
        /// Store sharing information in the local server proxy
        ///
        log.info("storing encryption secrets for asset \(globalIdentifier) for OTHER users in local server proxy")
        self.storeSecrets(request: shareRequest, globalIdentifier: globalIdentifier) {
            result in
            
            switch result {
            case .failure(let error):
                handleError(error)
                return
            case .success:
                self.log.info("successfully stored asset \(globalIdentifier) sharing information in local server proxy")
                
                ///
                /// Share using Safehill Server API
                ///
#if DEBUG
                guard ErrorSimulator.percentageShareFailures == 0
                        || arc4random() % (100 / ErrorSimulator.percentageShareFailures) != 0 else {
                    self.log.debug("simulating SHARE failure")
                    let error = SHBackgroundOperationError.fatalError("share failed")
                    handleError(error)
                    return
                }
#endif
                
                let doShare = {
                    self.share(
                        globalIdentifier: globalIdentifier,
                        via: shareRequest
                    ) { result in
                        if case .failure(let error) = result {
                            handleError(error)
                            return
                        }
                        
                        do {
                            try self.markAsSuccessful(
                                item: item,
                                encryptionRequest: shareRequest,
                                globalIdentifier: globalIdentifier
                            )
                        } catch {
                            self.log.critical("failed to mark SHARE as successful. This will likely cause infinite loops")
                            handleError(error)
                        }
                        
                        completionHandler(.success(()))
                    }
                }
                
                if shareRequest.isBackground == false {
                    self.initializeGroupAndThread(
                        shareRequest: shareRequest,
                        skipThreadCreation: shareRequest.shouldLinkToThread == false,
                        qos: qos
                    ) { result in
                        switch result {
                        case .failure(let error):
                            self.log.error("failed to initialize thread or group. \(error.localizedDescription)")
                            // Mark as failed on any other error
                            handleError(error)
                        case .success:
                            do {
                                // Ingest into the graph
                                try SHKGQuery.ingestShare(
                                    of: globalIdentifier,
                                    from: self.user.identifier,
                                    to: shareRequest.sharedWith.map({ $0.identifier })
                                )
                                doShare()
                            } catch {
                                handleError(error)
                            }
                        }
                    }
                } else {
                    doShare()
                }
            }
        }
    }
}
