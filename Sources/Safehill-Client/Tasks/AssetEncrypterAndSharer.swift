import Foundation
import os
import KnowledgeBase
import Safehill_Crypto


open class SHEncryptAndShareOperation: SHEncryptionOperation {
    
    public override var log: Logger {
        Logger(subsystem: "com.gf.safehill", category: "BG-SHARE")
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
        item: KBQueueItem,
        localIdentifier: String,
        groupId: String,
        eventOriginator: SHServerUser,
        sharedWith users: [SHServerUser]) throws
    {
        try self.markAsFailed(
            localIdentifier: localIdentifier,
            globalIdentifier: "",
            groupId: groupId,
            eventOriginator: eventOriginator,
            sharedWith: users
        )
    }
    
    public static func shareQueueItemKey(groupId: String, assetId: String, users: [SHServerUser]) -> String {
        return (
            assetId + "+" +
            groupId + "+" +
            SHHash.stringDigest(for: users
                .map({ $0.identifier })
                .sorted()
                .joined(separator: "+").data(using: .utf8)!
            )
        )
    }
    
    public func markAsFailed(
        localIdentifier: String,
        globalIdentifier: String,
        groupId: String,
        eventOriginator: SHServerUser,
        sharedWith users: [SHServerUser]) throws
    {
        // Enquque to failed
        log.info("enqueueing share request for asset \(localIdentifier) in the FAILED queue")
        
        let failedShare = SHFailedShareRequestQueueItem(localIdentifier: localIdentifier,
                                                        groupId: groupId,
                                                        eventOriginator: eventOriginator,
                                                        sharedWith: users)
        let key = SHEncryptAndShareOperation.shareQueueItemKey(groupId: groupId, assetId: localIdentifier, users: users)
        do {
            try failedShare.enqueue(in: FailedShareQueue, with: key)
        }
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
        item: KBQueueItem,
        localIdentifier: String,
        globalIdentifier: String,
        groupId: String,
        eventOriginator: SHServerUser,
        sharedWith users: [SHServerUser]
    ) throws {
        // Enquque to success history
        log.info("SHARING succeeded. Enqueueing sharing upload request in the SUCCESS queue (upload history) for asset \(localIdentifier)")
        
        let succesfulUploadQueueItem = SHShareHistoryItem(localIdentifier: localIdentifier,
                                                          groupId: groupId,
                                                          eventOriginator: eventOriginator,
                                                          sharedWith: users)
        
        let key = SHEncryptAndShareOperation.shareQueueItemKey(groupId: groupId, assetId: localIdentifier, users: users)
        do {
            try succesfulUploadQueueItem.enqueue(in: ShareHistoryQueue, with: key)
        }
        catch {
            log.fault("asset \(localIdentifier) was shared but will never be recorded as shared because enqueueing to SUCCESS queue failed")
            throw error
        }
        
        // Dequeque from ShareQueue
        log.info("dequeueing request for asset \(localIdentifier) from the SHARE queue")
        
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
    
    ///
    /// Get the private secret for the asset.
    /// The same secret should be used when encrypting for sharing with other users.
    /// Encrypting that secret again with the recipient's public key guarantees the privacy of that secret.
    ///
    /// By the time this method is called, the encrypted secret should be present in the local server,
    /// as the asset was previously encrypted on this device by the SHEncryptionOperation.
    ///
    /// Note that secrets are encrypted at rest, wheres the in-memory data is its decrypted (clear) version.
    ///
    /// - Returns: the decrypted shared secret for this asset
    /// - Throws: SHBackgroundOperationError if the shared secret couldn't be retrieved, other errors if the asset couldn't be retrieved from the Photos library
    ///
    private func retrieveCommonEncryptionKey(
        for asset: SHApplePhotoAsset,
        item: KBQueueItem,
        request shareRequest: SHEncryptionForSharingRequestQueueItem) throws -> Data
    {
        let quality = SHAssetQuality.lowResolution // Common encryption key (private secret) is constant across all versions. Any SHAssetQuality will return the same value
        let globalIdentifier = try asset.generateGlobalIdentifier(using: imageManager)
        
        let encryptedAsset = try SHLocalAssetStoreController(user: self.user)
            .encryptedAsset(
                with: globalIdentifier,
                versions: [quality]
            )
        guard let version = encryptedAsset.encryptedVersions[quality] else {
            log.error("failed to retrieve shared secret for asset \(globalIdentifier)")
            throw SHBackgroundOperationError.missingAssetInLocalServer(globalIdentifier)
        }
        
        let encryptedSecret = SHShareablePayload(
            ephemeralPublicKeyData: version.publicKeyData,
            cyphertext: version.encryptedSecret,
            signature: version.publicSignatureData
        )
        return try SHCypher.decrypt(
            encryptedSecret,
            using: self.user.shUser.privateKeyData,
            from: self.user.publicSignatureData
        )
    }
    
    private func storeSecrets(
        request: SHEncryptionForSharingRequestQueueItem,
        encryptedAsset: any SHEncryptedAsset
    ) throws {
        var shareableEncryptedVersions = [SHShareableEncryptedAssetVersion]()
        for otherUser in request.sharedWith {
            for quality in encryptedAsset.encryptedVersions.keys {
                let encryptedVersion = encryptedAsset.encryptedVersions[quality]!
                let shareableEncryptedVersion = SHGenericShareableEncryptedAssetVersion(
                    quality: quality,
                    userPublicIdentifier: otherUser.identifier,
                    encryptedSecret: encryptedVersion.encryptedSecret,
                    ephemeralPublicKey: encryptedVersion.publicKeyData,
                    publicSignature: encryptedVersion.publicSignatureData
                )
                shareableEncryptedVersions.append(shareableEncryptedVersion)
            }
        }
        
        let shareableEncryptedAsset = SHGenericShareableEncryptedAsset(
            globalIdentifier: encryptedAsset.globalIdentifier,
            sharedVersions: shareableEncryptedVersions,
            groupId: request.groupId
        )
        
        var error: Error? = nil
        let group = DispatchGroup()
        group.enter()
        serverProxy.shareAssetLocally(shareableEncryptedAsset) { result in
            if case .failure(let err) = result {
                error = err
            }
            group.leave()
        }
        
        let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds))
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
        guard error == nil else {
            throw error!
        }
    }
    
    private func share(
        encryptedAsset: any SHEncryptedAsset,
        via request: SHEncryptionForSharingRequestQueueItem
    ) throws {
        var error: Error? = nil
        let group = DispatchGroup()
        group.enter()
        self.serverProxy.getLocalSharingInfo(
            forAssetIdentifier: encryptedAsset.globalIdentifier,
            for: request.sharedWith
        ) { result in
            switch result {
            case .success(let shareableEncryptedAsset):
                guard let shareableEncryptedAsset = shareableEncryptedAsset else {
                    error = SHBackgroundOperationError.fatalError("Asset sharing information wasn't stored as expected during the encrypt step")
                    group.leave()
                    return
                }
                self.serverProxy.share(shareableEncryptedAsset) { shareResult in
                    if case .failure(let err) = shareResult {
                        error = err
                    }
                    group.leave()
                }
            case .failure(let err):
                error = err
                group.leave()
            }
        }
        
        let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds))
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
        guard error == nil else {
            throw error!
        }
    }
    
    ///
    /// Best attempt to remove the same item from the any other queue in the same pipeline
    ///
    private func tryRemoveExistingQueueItems(
        localIdentifier: String,
        groupId: String,
        sharedWith users: [SHServerUser]
    ) {
        for queue in [ShareHistoryQueue, FailedShareQueue] {

            let key = SHEncryptAndShareOperation.shareQueueItemKey(groupId: groupId, assetId: localIdentifier, users: users)
            let condition = KBGenericCondition(.equal, value: key)
            let _ = try? queue.removeValues(forKeysMatching: condition)
        }
    }
    
    private func process(_ item: KBQueueItem) throws {
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
            do { _ = try ShareQueue.dequeue(item: item) }
            catch {
                log.fault("dequeuing failed of unexpected data in SHARE queue. ATTENTION: this operation will be attempted again.")
            }

            throw SHBackgroundOperationError.unexpectedData(item.content)
        }
        
        let asset = shareRequest.asset
        let encryptedAsset: any SHEncryptedAsset

        do {
            guard shareRequest.sharedWith.count > 0 else {
                log.error("empty sharing information in SHEncryptionForSharingRequestQueueItem object. SHEncryptAndShareOperation can only operate on sharing operations, which require user identifiers")
                throw SHBackgroundOperationError.fatalError("sharingWith emtpy. No sharing info")
            }
            
            self.tryRemoveExistingQueueItems(
                localIdentifier: asset.phAsset.localIdentifier,
                groupId: shareRequest.groupId,
                sharedWith: shareRequest.sharedWith
            )

            log.info("sharing it with users \(shareRequest.sharedWith.map { $0.identifier })")

            for delegate in delegates {
                if let delegate = delegate as? SHAssetSharerDelegate {
                    delegate.didStartSharing(
                        itemWithLocalIdentifier: asset.phAsset.localIdentifier,
                        groupId: shareRequest.groupId,
                        with: shareRequest.sharedWith
                    )
                }
            }
            
            ///
            /// This is the common asset private key that allows anyone to decrypt the asset.
            /// This secret will be encrypted for each user with their public key, so that
            /// they are the only one that can decrypt the secret to decrypt the asset.
            ///
            let decryptedSecretData: Data = try self.retrieveCommonEncryptionKey(
                for: asset,
                item: item,
                request: shareRequest
            )
            encryptedAsset = try self.generateEncryptedAsset(
                for: asset,
                versions: [.lowResolution, .midResolution],
                usingPrivateSecret: decryptedSecretData,
                recipients: shareRequest.sharedWith,
                request: shareRequest
            )
            
            ///
            /// Store sharing information in the local server proxy
            ///
            do {
                log.info("storing encryption secrets for asset \(encryptedAsset.globalIdentifier) for OTHER users in local server proxy")

                try self.storeSecrets(request: shareRequest, encryptedAsset: encryptedAsset)
                
                log.info("successfully stored asset \(encryptedAsset.globalIdentifier) sharing information in local server proxy")
            } catch {
                log.error("failed to locally share encrypted item \(item.identifier) with users \(shareRequest.sharedWith.map { $0.identifier }): \(error.localizedDescription)")
                throw SHBackgroundOperationError.fatalError("failed to store secrets")
            }
            
            ///
            /// Share using Safehill Server API
            ///
#if DEBUG
            guard kSHSimulateBackgroundOperationFailures == false || arc4random() % 20 != 0 else {
                log.debug("simulating SHARE failure")
                throw SHBackgroundOperationError.fatalError("share failed")
            }
#endif
            
            do {
                try self.share(encryptedAsset: encryptedAsset, via: shareRequest)
            } catch {
                log.error("failed to share with users \(shareRequest.sharedWith.map { $0.identifier })")
                throw SHBackgroundOperationError.fatalError("share failed")
            }

        } catch {
            do {
                try self.markAsFailed(
                    item: item,
                    localIdentifier: asset.phAsset.localIdentifier,
                    groupId: shareRequest.groupId,
                    eventOriginator: shareRequest.eventOriginator,
                    sharedWith: shareRequest.sharedWith
                )
            } catch {
                log.critical("failed to mark SHARE as failed. This will likely cause infinite loops")
                // TODO: Handle
            }
            
            throw error
        }

        ///
        /// Finish
        ///
        log.info("[√] share task completed for item \(item.identifier)")

        try self.markAsSuccessful(
            item: item,
            localIdentifier: asset.phAsset.localIdentifier,
            globalIdentifier: encryptedAsset.globalIdentifier,
            groupId: shareRequest.groupId,
            eventOriginator: shareRequest.eventOriginator,
            sharedWith: shareRequest.sharedWith
        )
    }
    
    public override func runOnce() throws {
        while let item = try ShareQueue.peek() {
            guard processingState(for: item.identifier) != .sharing else {
                break
            }
            
            log.info("sharing item \(item.identifier) created at \(item.createdAt)")
            
            setProcessingState(.sharing, for: item.identifier)
            
            do {
                try self.process(item)
                log.info("[√] share task completed for item \(item.identifier)")
            } catch {
                log.error("[x] share task failed for item \(item.identifier): \(error.localizedDescription)")
            }
            
            setProcessingState(nil, for: item.identifier)
        }
    }
    
    public override func main() {
        guard !self.isCancelled else {
            state = .finished
            return
        }
        
        state = .executing
        
        let items: [KBQueueItem]
        
        do {
            items = try ShareQueue.peekNext(self.limit)
        } catch {
            log.error("failed to fetch items from the ENCRYPT queue")
            state = .finished
            return
        }
        
        for item in items {
            guard processingState(for: item.identifier) != .sharing else {
                break
            }
            
            log.info("sharing item \(item.identifier) created at \(item.createdAt)")
            
            setProcessingState(.sharing, for: item.identifier)
            
            DispatchQueue.global(qos: .background).async { [self] in
                guard !isCancelled else {
                    log.info("share task cancelled. Finishing")
                    setProcessingState(nil, for: item.identifier)
                    return
                }
                do {
                    try self.process(item)
                    log.info("[√] share task completed for item \(item.identifier)")
                } catch {
                    log.error("[x] share task failed for item \(item.identifier): \(error.localizedDescription)")
                }
                
                setProcessingState(nil, for: item.identifier)
            }
            
            guard !self.isCancelled else {
                log.info("share task cancelled. Finishing")
                break
            }
        }
        
        state = .finished
    }
}

public class SHAssetEncryptAndShareQueueProcessor : SHBackgroundOperationProcessor<SHEncryptAndShareOperation> {
    /// Singleton (with private initializer)
    public static var shared = SHAssetEncryptAndShareQueueProcessor(
        delayedStartInSeconds: 5,
        dispatchIntervalInSeconds: 2
    )
    
    private override init(delayedStartInSeconds: Int = 0,
                          dispatchIntervalInSeconds: Int? = nil) {
        super.init(delayedStartInSeconds: delayedStartInSeconds, dispatchIntervalInSeconds: dispatchIntervalInSeconds)
    }
}
