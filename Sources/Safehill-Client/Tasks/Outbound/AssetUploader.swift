import Foundation
import os
import KnowledgeBase


internal class SHUploadOperation: Operation, SHBackgroundQueueBackedOperationProtocol, SHOutboundBackgroundOperation, SHUploadStepBackgroundOperation, @unchecked Sendable {
    
    let operationType = BackgroundOperationQueue.OperationType.upload
    let processingState = ProcessingState.uploading
    
    let log = Logger(subsystem: "com.gf.safehill", category: "BG-UPLOAD")
    
    let limit: Int
    let user: SHAuthenticatedLocalUser
    let assetsDelegates: [SHOutboundAssetOperationDelegate]
    
    let delegatesQueue = DispatchQueue(label: "com.safehill.upload.delegates")
    
    var serverProxy: SHServerProxy {
        return self.user.serverProxy
    }
    
    public init(user: SHAuthenticatedLocalUser,
                assetsDelegates: [SHOutboundAssetOperationDelegate],
                limitPerRun limit: Int) {
        self.user = user
        self.limit = limit
        self.assetsDelegates = assetsDelegates
    }
    
    public func markAsFailed(
        item: KBQueueItem,
        uploadRequest request: SHUploadRequestQueueItem,
        error: Error,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        let globalIdentifier = request.asset.globalIdentifier
        let localIdentifier = request.asset.localIdentifier
        let versions = request.versions
        let groupId = request.groupId
        let users = request.sharedWith
        
        /// Dequeque from UploadQueue
        log.info("dequeueing request for asset \(globalIdentifier) from the UPLOAD queue")
        
        do {
            let uploadQueue = try BackgroundOperationQueue.of(type: .upload)
            _ = try uploadQueue.dequeue(item: item)
        }
        catch {
            log.error("asset \(globalIdentifier) failed to upload but dequeuing from UPLOAD queue failed, so this operation will be attempted again.")
            completionHandler(.failure(error))
            return
        }
        
        self.markAsFailed(
            request: request,
            error: error
        )
        
        self.markLocalAssetAsFailed(globalIdentifier: globalIdentifier, versions: versions) {
            guard request.isBackground == false else {
                /// Avoid other side-effects for background  `SHUploadRequestQueueItem`
                completionHandler(.success(()))
                return
            }
            
            /// Notify the delegates
            let assetsDelegates = self.assetsDelegates
            self.delegatesQueue.async {
                for delegate in assetsDelegates {
                    if let delegate = delegate as? SHAssetUploaderDelegate {
                        delegate.didFailUpload(
                            ofAsset: SHBackedUpAssetIdentifier(
                                globalIdentifier: globalIdentifier,
                                localIdentifier: localIdentifier
                            ),
                            in: groupId,
                            error: error
                        )
                    }
                    if users.count > 0 {
                        if let delegate = delegate as? SHAssetSharerDelegate {
                            delegate.didFailSharing(
                                ofAsset: SHBackedUpAssetIdentifier(
                                    globalIdentifier: globalIdentifier,
                                    localIdentifier: localIdentifier
                                ),
                                with: users,
                                in: groupId,
                                error: error
                            )
                        }
                    }
                }
            }
            
            completionHandler(.success(()))
        }
    }
    
    public func markAsSuccessful(
        item: KBQueueItem,
        uploadRequest request: SHUploadRequestQueueItem
    ) throws {
        let globalIdentifier = request.asset.globalIdentifier
        let localIdentifier = request.asset.localIdentifier
        let versions = request.versions
        let groupId = request.groupId
        let groupTitle = request.groupTitle
        let eventOriginator = request.eventOriginator
        let sharedWith = request.sharedWith
        let invitedUsers = request.invitedUsers
        let asPhotoMessageInThreadId = request.asPhotoMessageInThreadId
        let permissions = request.permissions
        let isBackground = request.isBackground
        
        /// Dequeue from Upload queue
        log.info("dequeueing item \(item.identifier) from the UPLOAD queue")
        
        do {
            let uploadQueue = try BackgroundOperationQueue.of(type: .upload)
            _ = try uploadQueue.dequeue(item: item)
        } catch {
            log.warning("item \(item.identifier) was completed but dequeuing from UPLOAD queue failed. This task will be attempted again")
            throw error
        }
        
        do {
            let failedUploadQueue = try BackgroundOperationQueue.of(type: .failedUpload)
            
            /// Remove items in the `FailedUploadQueue` for the same identifier
            let _ = try failedUploadQueue.removeValues(forKeysMatching: KBGenericCondition(.equal, value: item.identifier))
            
        } catch {
            log.warning("asset \(globalIdentifier) was upload but removal from the failed upload queue failed")
        }
        
        if isBackground == false {
            let assetsDelegates = self.assetsDelegates
            self.delegatesQueue.async {
                /// Notify the delegates
                for delegate in assetsDelegates {
                    if let delegate = delegate as? SHAssetUploaderDelegate {
                        delegate.didCompleteUpload(
                            ofAsset: SHBackedUpAssetIdentifier(
                                globalIdentifier: globalIdentifier,
                                localIdentifier: localIdentifier
                            ),
                            in: groupId
                        )
                    }
                }
            }
        }
        
        ///
        /// Start the sharing as needed
        ///
        let shareQueue = try BackgroundOperationQueue.of(type: .share)
        
        do {
            ///
            /// Enquque to SHARE queue for encrypting for sharing
            ///
            log.info("enqueueing upload request in the FETCH+SHARE queue for asset \(globalIdentifier) versions \(versions) isBackground=\(isBackground)")
            
            let request = SHEncryptionRequestQueueItem(
                asset: request.asset,
                versions: versions,
                groupId: groupId,
                groupTitle: groupTitle,
                eventOriginator: eventOriginator,
                sharedWith: sharedWith,
                invitedUsers: invitedUsers,
                asPhotoMessageInThreadId: asPhotoMessageInThreadId,
                permissions: permissions,
                isBackground: isBackground
            )
            try request.enqueue(in: shareQueue, with: request.identifier)
        } catch {
            log.fault("asset \(globalIdentifier) was uploaded but will never be shared because enqueueing to SHARE queue failed")
            throw error
        }
    }
    
    func process(
        _ item: KBQueueItem,
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        let uploadRequest: SHUploadRequestQueueItem
        
        do {
            let content = try content(ofQueueItem: item)
            guard let content = content as? SHUploadRequestQueueItem else {
                log.error("unexpected data found in UPLOAD queue. Dequeueing")
                // Delegates can't be called as item content can't be read and it will be silently removed from the queue
                throw SHBackgroundOperationError.unexpectedData(item.content)
            }
            uploadRequest = content
        } catch {
            do {
                let uploadQueue = try BackgroundOperationQueue.of(type: .upload)
                _ = try uploadQueue.dequeue(item: item)
            }
            catch {
                log.warning("dequeuing failed of unexpected data in UPLOAD queue. This task will be attempted again.")
            }
            completionHandler(.failure(error))
            return
        }
        
        let globalIdentifier = uploadRequest.asset.globalIdentifier
        let localIdentifier = uploadRequest.asset.localIdentifier
        let versions = uploadRequest.versions
        
        let handleError = { (error: Error) in
            self.log.error("FAIL in UPLOAD: \(error.localizedDescription)")
            
            self.markAsFailed(
                item: item,
                uploadRequest: uploadRequest,
                error: error
            ) { _ in
                completionHandler(.failure(error))
            }
        }
        
        if uploadRequest.isBackground == false {
            let assetsDelegates = self.assetsDelegates
            self.delegatesQueue.async {
                for delegate in assetsDelegates {
                    if let delegate = delegate as? SHAssetUploaderDelegate {
                        delegate.didStartUpload(
                            ofAsset: SHBackedUpAssetIdentifier(
                                globalIdentifier: globalIdentifier,
                                localIdentifier: localIdentifier
                            ),
                            in: uploadRequest.groupId
                        )
                    }
                }
            }
        }
        
        log.info("retrieving encrypted asset from local server proxy: \(globalIdentifier) versions=\(versions)")
        self.serverProxy.getLocalAssets(
            withGlobalIdentifiers: [globalIdentifier],
            versions: versions
        ) { result in
            
            switch result {
            case .failure(let error):
                self.log.error("failed to retrieve local server asset \(globalIdentifier): \(error.localizedDescription).")
                handleError(error)
            case .success(let encryptedAssets):
                guard let encryptedAsset = encryptedAssets[globalIdentifier] else {
                    let error = SHBackgroundOperationError.missingAssetInLocalServer(globalIdentifier)
                    handleError(error)
                    return
                }
                
                guard globalIdentifier == encryptedAsset.globalIdentifier else {
                    let error = SHBackgroundOperationError.globalIdentifierDisagreement(globalIdentifier, encryptedAsset.globalIdentifier)
                    handleError(error)
                    return
                }
                
#if DEBUG
                guard ErrorSimulator.percentageUploadFailures == 0
                      || arc4random() % (100 / ErrorSimulator.percentageUploadFailures) != 0 else {
                    self.log.debug("simulating CREATE ASSET failure")
                    let error = SHBackgroundOperationError.fatalError("simulated create asset failure")
                    handleError(error)
                    return
                }
#endif
                
                Task(priority: qos.toTaskPriority()) {
                    let fingerprint: AssetFingerprint
                    
                    do {
                        if uploadRequest.asset.fingerprint == nil {
                            try await uploadRequest.asset.calculateFingerprintIfNeeded()
                        }
                        fingerprint = uploadRequest.asset.fingerprint!
                    } catch {
                        let error = SHBackgroundOperationError.fatalError("failed to create embeddings: \(error.localizedDescription)")
                        handleError(error)
                        return
                    }
                    
                    let serverAsset: SHServerAsset
                    do {
                        serverAsset = try await SHAssetStoreController(user: self.user)
                            .upload(
                                asset: encryptedAsset,
                                fingerprint: fingerprint,
                                with: uploadRequest.groupId,
                                filterVersions: versions,
                                force: false
                            )
                    } catch {
                        let error = SHBackgroundOperationError.fatalError("failed to create server asset or upload asset to the CDN: \(error.localizedDescription)")
                        handleError(error)
                        return
                    }
                    
                    guard globalIdentifier == serverAsset.globalIdentifier else {
                        let error = SHBackgroundOperationError.globalIdentifierDisagreement(globalIdentifier, serverAsset.globalIdentifier)
                        handleError(error)
                        return
                    }
                    
                    ///
                    /// Upload is completed.
                    /// Remove the item in the upload queue, and add to the share queue if shared
                    ///
                    do {
                        try self.markAsSuccessful(
                            item: item,
                            uploadRequest: uploadRequest
                        )
                        completionHandler(.success(()))
                    } catch {
                        self.log.critical("failed to mark UPLOAD as successful. This will likely cause infinite loops")
                        handleError(error)
                    }
                }
            }
        }
    }
}
