import Foundation
import os
import KnowledgeBase

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
        let invitedUsers = request.invitedUsers
        
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
        
        guard request.isBackground == false else {
            /// Avoid other side-effects for background  `SHEncryptionRequestQueueItem`
            return
        }
        
        /// Notify the delegates
        let assetsDelegates = self.assetsDelegates
        delegatesQueue.async {
            if users.count > 0 {
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
            
            if invitedUsers.count > 0 {
                for delegate in assetsDelegates {
                    if let delegate = delegate as? SHAssetSharerDelegate {
                        delegate.didFailInviting(
                            phoneNumbers: invitedUsers,
                            to: groupId,
                            error: error
                        )
                    }
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
        
        let globalIdentifier = shareRequest.asset.globalIdentifier
        
        let handleSuccess = {
            do {
                try self.markAsSuccessful(
                    queueItem: item,
                    encryptionRequest: shareRequest,
                    globalIdentifier: globalIdentifier
                )
            } catch {
                self.log.critical("failed to mark SHARE as successful. This will likely cause infinite loops. \(error.localizedDescription)")
                completionHandler(.failure(error))
            }
            
            completionHandler(.success(()))
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
        
        log.info("sharing it with users \(shareRequest.sharedWith.map { $0.identifier }) and invited \(shareRequest.invitedUsers)")
        
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
        
        Task(priority: qos.toTaskPriority()) {
            
            let assetSharingController = SHAssetSharingController(localUser: self.user)
            
            if shareRequest.isBackground == false {
                ///
                /// Create group encryption details and title
                ///
                var usersAndSelf = shareRequest.sharedWith
                usersAndSelf.append(self.user)
                
                do {
                    try await assetSharingController.createGroupEncryptionDetails(
                        for: usersAndSelf,
                        in: shareRequest.groupId,
                        updateGroupTitleTo: shareRequest.groupTitle
                    )
                } catch {
                    handleError(error)
                    return
                }
            }
            
            if shareRequest.isSharingWithOrInvitingOtherUsers == false {
                ///
                /// After setting up the group details terminate early
                /// if there's no user to share with and
                /// no phone number to invite
                ///
                handleSuccess()
            } else {
                ///
                /// Otherwise start sharing and inviting
                ///
                
                if shareRequest.isOnlyInvitingUsers == false {
                    do {
                        try await assetSharingController.shareAsset(
                            globalIdentifier: globalIdentifier,
                            versions: shareRequest.versions,
                            with: shareRequest.sharedWith,
                            via: shareRequest.groupId,
                            asPhotoMessageInThreadId: shareRequest.asPhotoMessageInThreadId,
                            isBackground: shareRequest.isBackground
                        )
                    } catch {
                        handleError(error)
                        return
                    }
                }
                
                self.serverProxy.invite(
                    shareRequest.invitedUsers,
                    to: shareRequest.groupId
                ) { inviteResult in
                    switch inviteResult {
                    case .failure(let error):
                        handleError(error)
                        
                    case.success:
                        handleSuccess()
                    }
                }
            }
        }
    }
}
