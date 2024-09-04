import Foundation
import os
import KnowledgeBase
import Photos


internal class SHEncryptAndShareOperation: SHEncryptionOperation {
    
    override var operationType: BackgroundOperationQueue.OperationType { .share }
    override var processingState: ProcessingState { .sharing }
    
    public override var log: Logger {
        Logger(subsystem: "com.gf.safehill", category: "BG-SHARE")
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
        globalIdentifier: GlobalIdentifier?,
        error: Error,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        do {
            try self.markAsFailed(
                encryptionRequest: request,
                queueItem: item,
                error: error
            )
            completionHandler(.success(()))
        } catch {
            completionHandler(.failure(error))
        }
        
    }
    
    public func markAsFailed(
        encryptionRequest request: SHEncryptionRequestQueueItem,
        queueItem: KBQueueItem,
        error: Error
    ) throws {
        let localIdentifier = request.localIdentifier
        let versions = request.versions
        let groupId = request.groupId
        let eventOriginator = request.eventOriginator
        let users = request.sharedWith
        let invitedUsers = request.invitedUsers
        let asPhotoMessageInThreadId = request.asPhotoMessageInThreadId
        
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
            invitedUsers: invitedUsers,
            asPhotoMessageInThreadId: asPhotoMessageInThreadId,
            isBackground: request.isBackground
        )

        do {
            let failedShareQueue = try BackgroundOperationQueue.of(type: .failedShare)
            try failedShare.enqueue(in: failedShareQueue)
        }
        catch {
            log.fault("asset \(localIdentifier) failed to share but will never be recorded as 'failed to share' because enqueueing to SHARE FAILED queue failed: \(error.localizedDescription)")
            throw error
        }
        
        guard request.isBackground == false, users.count > 0 else {
            /// Avoid other side-effects for background  `SHEncryptionRequestQueueItem`
            return
        }
        
        let assetsDelegates = self.assetsDelegates
        delegatesQueue.async {
            /// Notify the delegates
            for delegate in assetsDelegates {
                if let delegate = delegate as? SHAssetSharerDelegate {
                    delegate.didFailSharing(
                        ofAsset: localIdentifier,
                        with: users,
                        in: groupId,
                        error: error
                    )
                }
            }
        }
    }
    
    public override func markAsSuccessful(
        item: KBQueueItem,
        encryptionRequest request: SHEncryptionRequestQueueItem,
        globalIdentifier: GlobalIdentifier
    ) throws {
        let localIdentifier = request.localIdentifier
        let groupId = request.groupId
        
        /// Dequeque from ShareQueue
        log.info("dequeueing request for asset \(localIdentifier) from the SHARE queue")
        
        do { _ = try BackgroundOperationQueue.of(type: .share).dequeue(item: item) }
        catch {
            log.warning("asset \(localIdentifier) was uploaded but dequeuing from the SHARE queue failed, so this operation will be attempted again")
        }
        
        do {
            log.info("SHARING succeeded. Enqueueing sharing upload request in the SUCCESS queue (upload history) for asset \(localIdentifier)")
            let failedShareQueue = try BackgroundOperationQueue.of(type: .failedShare)
            /// Remove items in the `FailedShareQueue` for the same identifier
            let _ = try failedShareQueue.removeValues(forKeysMatching: KBGenericCondition(.equal, value: item.identifier))
        }
        catch {
            log.fault("asset \(localIdentifier) was shared but will never be recorded as shared because enqueueing to SUCCESS queue failed")
            throw error
        }
        
        guard request.isBackground == false else {
            /// Avoid other side-effects for background  `SHEncryptionRequestQueueItem`
            return
        }
        
        let assetsDelegates = self.assetsDelegates
        delegatesQueue.async {
            /// Notify the delegates
            for delegate in assetsDelegates {
                if let delegate = delegate as? SHAssetSharerDelegate {
                    delegate.didCompleteSharing(ofAsset: localIdentifier, with: request.sharedWith, in: groupId)
                }
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
    
    private func share(
        _ shareableEncryptedAsset: SHShareableEncryptedAsset,
        from shareRequest: SHEncryptionForSharingRequestQueueItem,
        queueItem item: KBQueueItem,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        self.serverProxy.share(
            shareableEncryptedAsset,
            asPhotoMessageInThreadId: shareRequest.asPhotoMessageInThreadId,
            suppressNotification: shareRequest.isBackground
        ) { shareResult in
            
            let handleSuccess = {
                do {
                    try self.markAsSuccessful(
                        item: item,
                        encryptionRequest: shareRequest,
                        globalIdentifier: shareableEncryptedAsset.globalIdentifier
                    )
                } catch {
                    self.log.critical("failed to mark SHARE as successful. This will likely cause infinite loops. \(error.localizedDescription)")
                    completionHandler(.failure(error))
                }
                
                completionHandler(.success(()))
            }
            
            switch shareResult {
            case .failure(let err):
                completionHandler(.failure(err))
                
            case .success:
                if shareRequest.isBackground == false,
                   shareRequest.invitedUsers.count > 0 {
                    
                    self.serverProxy.invite(
                        shareRequest.invitedUsers,
                        to: shareRequest.groupId
                    ) { inviteResult in
                        switch inviteResult {
                        case.success:
                            handleSuccess()
                            
                        case .failure(let error):
                            completionHandler(.failure(error))
                        }
                    }
                } else {
                    handleSuccess()
                }
            }
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
                    queueItem: item,
                    error: error
                )
            } catch {
                self.log.critical("failed to mark SHARE as failed. This will likely cause infinite loops")
                // TODO: Handle
            }
            completionHandler(.failure(error))
        }
        
        let globalIdentifier = shareRequest.asset.globalIdentifier
        guard let globalIdentifier else {
            ///
            /// At this point the global identifier should be calculated by the `SHLocalFetchOperation`,
            /// serialized and deserialized as part of the `SHApplePhotoAsset` object.
            ///
            handleError(SHBackgroundOperationError.globalIdentifierDisagreement(""))
            return
        }
        
        guard shareRequest.isSharingWithOrInvitingOtherUsers else {
            handleError(SHBackgroundOperationError.fatalError("empty sharing information in SHEncryptionForSharingRequestQueueItem object. SHEncryptAndShareOperation can only operate on sharing operations, which require user identifiers or invited phone numbers"))
            return
        }
        
        log.info("sharing it with users \(shareRequest.sharedWith.map { $0.identifier }) and invited \(shareRequest.invitedUsers)")
        
        guard shareRequest.isOnlyInvitingUsers == false else {
            
            ///
            /// If only inviting other users take this shortcut,
            /// only invite and end early.
            /// If in background, don't invite, cause the previous non-background item
            /// would have invited the users for this request
            ///
            
            let dequeue = {
                do { _ = try BackgroundOperationQueue.of(type: .share).dequeue(item: item) }
                catch {
                    self.log.warning("asset \(shareRequest.asset.phAsset.localIdentifier) was uploaded but dequeuing from the SHARE queue failed, so this operation will be attempted again")
                }
                
                completionHandler(.success(()))
            }
            
            if shareRequest.isBackground == false {
                self.serverProxy.invite(
                    shareRequest.invitedUsers,
                    to: shareRequest.groupId
                ) { result in
                    
                    dequeue()
                    completionHandler(result)
                }
            } else {
                dequeue()
                completionHandler(.success(()))
            }
            return
        }
        
        if shareRequest.isBackground == false {
            let assetDelegates = self.assetsDelegates
            self.delegatesQueue.async {
                for delegate in assetDelegates {
                    if let delegate = delegate as? SHAssetSharerDelegate {
                        delegate.didStartSharing(ofAsset: shareRequest.localIdentifier,
                                                 with: shareRequest.sharedWith,
                                                 in: shareRequest.groupId)
                    }
                }
            }
        }
        
        ///
        /// Generate sharing information from the locally encrypted asset
        ///
        log.info("generating encrypted assets for asset with id \(globalIdentifier) for users \(shareRequest.sharedWith.map({ $0.identifier }))")
        self.user.shareableEncryptedAsset(
            globalIdentifier: globalIdentifier,
            versions: shareRequest.versions,
            recipients: shareRequest.sharedWith,
            groupId: shareRequest.groupId
        ) {
            result in
            
            switch result {
            case .failure(let error):
                handleError(error)
                return
            case .success(let shareableEncryptedAsset):
                self.log.info("successfully generated asset \(globalIdentifier) sharing information")
                
                ///
                /// Share using Safehill Server API
                ///
#if DEBUG
                guard ErrorSimulator.percentageShareFailures == 0
                        || arc4random() % (100 / ErrorSimulator.percentageShareFailures) != 0 else {
                    self.log.debug("simulating SHARE failure")
                    let error = SHBackgroundOperationError.fatalError("simulated share failed")
                    handleError(error)
                    return
                }
#endif
                
                if shareRequest.isBackground == false {
                    self.initializeGroupAndThread(
                        shareRequest: shareRequest,
                        skipThreadCreation: false,
                        qos: qos
                    ) { result in
                        switch result {
                        case .failure(let error):
                            self.log.error("failed to initialize thread or group. \(error.localizedDescription)")
                            // Mark as failed on any other error
                            handleError(error)
                        case .success:
                            self.share(
                                shareableEncryptedAsset,
                                from: shareRequest,
                                queueItem: item
                            ) {
                                result in
                                
                                switch result {
                                case .success:
                                    do {
                                        // Ingest into the graph
                                        try SHKGQuery.ingestShare(
                                            of: globalIdentifier,
                                            from: self.user.identifier,
                                            to: shareRequest.sharedWith.map({ $0.identifier })
                                        )
                                    } catch {
                                        self.log.warning("failed to update the local graph with sharing information")
                                    }
                                    
                                    /// After remote sharing is successful, add `receiver::` rows in local server
                                    self.serverProxy.shareAssetLocally(shareableEncryptedAsset) { _ in }
                                    
                                    completionHandler(.success(()))
                                    
                                case .failure(let error):
                                    handleError(error)
                                }
                            }
                        }
                    }
                } else {
                    self.share(
                        shareableEncryptedAsset,
                        from: shareRequest,
                        queueItem: item,
                        completionHandler: completionHandler
                    )
                }
            }
        }
    }
}
