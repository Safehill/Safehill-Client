import Foundation
import os
import KnowledgeBase
import Photos


internal class SHEncryptAndShareOperation: SHEncryptionOperation, @unchecked Sendable {
    
    override var operationType: BackgroundOperationQueue.OperationType { .share }
    override var processingState: ProcessingState { .sharing }
    
    public override var log: Logger {
        Logger(subsystem: "com.gf.safehill", category: "BG-SHARE")
    }
    
    internal var interactionsController: SHUserInteractionController {
        SHUserInteractionController(user: self.user)
    }
    
    public override func markAsFailed(
        queueItem: KBQueueItem,
        encryptionRequest request: SHEncryptionRequestQueueItem,
        error: Error,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        do {
            try self.markAsFailed(
                queueItem: queueItem,
                encryptionRequest: request,
                error: error
            )
            completionHandler(.success(()))
        } catch {
            completionHandler(.failure(error))
        }
    }
    
    public func markAsFailed(
        queueItem: KBQueueItem,
        encryptionRequest request: SHEncryptionRequestQueueItem,
        error: Error
    ) throws {
        let globalIdentifier = request.asset.globalIdentifier
        let versions = request.versions
        let groupId = request.groupId
        let users = request.sharedWith
        
        do { _ = try BackgroundOperationQueue.of(type: .share).dequeue(item: queueItem) }
        catch {
            log.error("asset \(globalIdentifier) failed to share but dequeueing from SHARE queue failed. Sharing will be attempted again")
            throw error
        }
        
        /// Enquque to failed
        log.info("enqueueing share request for asset \(globalIdentifier) versions \(versions) in the FAILED queue")
        do {
            let failedShareQueue = try BackgroundOperationQueue.of(type: .failedShare)
            try request.enqueue(in: failedShareQueue, with: request.identifier)
        }
        catch {
            log.fault("asset \(globalIdentifier) failed to share but will never be recorded as 'failed to share' because enqueueing to SHARE FAILED queue failed: \(error.localizedDescription)")
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
                        ofAsset: globalIdentifier,
                        with: users,
                        in: groupId,
                        error: error
                    )
                }
            }
        }
    }
    
    public override func markAsSuccessful(
        queueItem: KBQueueItem,
        encryptionRequest request: SHEncryptionRequestQueueItem,
        globalIdentifier: GlobalIdentifier
    ) throws {
        let globalIdentifier = request.asset.globalIdentifier
        let groupId = request.groupId
        
        /// Dequeque from ShareQueue
        log.info("dequeueing request for asset \(globalIdentifier) from the SHARE queue")
        
        do { _ = try BackgroundOperationQueue.of(type: .share).dequeue(item: queueItem) }
        catch {
            log.warning("asset \(globalIdentifier) was uploaded but dequeuing from the SHARE queue failed, so this operation will be attempted again")
        }
        
        do {
            log.info("SHARING succeeded. Enqueueing sharing upload request in the SUCCESS queue (upload history) for asset \(globalIdentifier)")
            let failedShareQueue = try BackgroundOperationQueue.of(type: .failedShare)
            /// Remove items in the `FailedShareQueue` for the same identifier
            let _ = try failedShareQueue.removeValues(forKeysMatching: KBGenericCondition(.equal, value: queueItem.identifier))
        }
        catch {
            log.fault("asset \(globalIdentifier) was shared but will never be recorded as shared because enqueueing to SUCCESS queue failed")
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
                    delegate.didCompleteSharing(ofAsset: globalIdentifier, with: request.sharedWith, in: groupId)
                }
            }
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
                        queueItem: item,
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
                            let assetsDelegates = self.assetsDelegates
                            self.delegatesQueue.async {
                                /// Notify the delegates
                                for delegate in assetsDelegates {
                                    if let delegate = delegate as? SHAssetSharerDelegate {
                                        delegate.didFailInviting(
                                            phoneNumbers: shareRequest.invitedUsers,
                                            to: shareRequest.groupId,
                                            error: error
                                        )
                                    }
                                }
                            }
                            
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
                    queueItem: item,
                    encryptionRequest: shareRequest,
                    error: error
                )
            } catch {
                self.log.critical("failed to mark SHARE as failed. This will likely cause infinite loops")
                // TODO: Handle
            }
            completionHandler(.failure(error))
        }
        
        let globalIdentifier = shareRequest.asset.globalIdentifier
        
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
                    self.log.warning("asset \(shareRequest.asset.globalIdentifier) was uploaded but dequeuing from the SHARE queue failed, so this operation will be attempted again")
                }
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
                        delegate.didStartSharing(ofAsset: shareRequest.asset.globalIdentifier,
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
                    var usersAndSelf = shareRequest.sharedWith
                    usersAndSelf.append(self.user)
                    
                    self.log.debug("creating or updating group for request \(shareRequest.identifier)")
                    self.interactionsController.setupGroup(
                        title: shareRequest.groupTitle,
                        groupId: shareRequest.groupId,
                        with: usersAndSelf
                    ) { result in
                        switch result {
                        case .failure(let error):
                            self.log.error("failed to initialize group. \(error.localizedDescription)")
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
                                    self.serverProxy.shareAssetLocally(
                                        shareableEncryptedAsset,
                                        asPhotoMessageInThreadId: shareRequest.asPhotoMessageInThreadId
                                    ) { _ in }
                                    
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
