import Foundation
import Safehill_Crypto
import KnowledgeBase
import os
import CryptoKit
#if os(iOS)
import UIKit
#else
import AppKit
#endif


internal class SHEncryptionOperation: Operation, SHBackgroundQueueBackedOperationProtocol, SHOutboundBackgroundOperation, SHUploadStepBackgroundOperation, @unchecked Sendable {
    typealias OperationResult = Result<Void, Error>
    
    var operationType: BackgroundOperationQueue.OperationType { .encryption }
    var processingState: ProcessingState { .encrypting }
    
    public var log: Logger {
        Logger(subsystem: "com.gf.safehill", category: "BG-ENCRYPT")
    }
    
    public let limit: Int
    public let user: SHAuthenticatedLocalUser
    public var assetsDelegates: [SHOutboundAssetOperationDelegate]
    
    let delegatesQueue = DispatchQueue(label: "com.safehill.encrypt.delegates")
    
    public init(user: SHAuthenticatedLocalUser,
                assetsDelegates: [SHOutboundAssetOperationDelegate],
                limitPerRun limit: Int) {
        self.user = user
        self.limit = limit
        self.assetsDelegates = assetsDelegates
    }
    
    var serverProxy: SHServerProxy {
        self.user.serverProxy
    }
    
    private func encrypt(
        asset: SHUploadableAsset,
        versions: [SHAssetQuality],
        for recipients: [SHServerUser],
        usingSecret privateSecret: Data
    ) throws -> any SHEncryptedAsset {
        
        log.info("encrypting asset \(asset.globalIdentifier) versions to be shared with users \(recipients.map { $0.identifier }) using symmetric key \(privateSecret.base64EncodedString(), privacy: .private(mask: .hash))")
        
        ///
        /// Encrypt all asset versions with a new symmetric keys generated at encryption time
        ///
        let privateSecret = try SymmetricKey(rawRepresentation: privateSecret)
        let encryptedPayloads = try versions.map {
            guard let data = asset.data[$0] else {
                throw SHBackgroundOperationError.unexpectedData(asset.data)
            }
            return try SHEncryptedData(privateSecret: privateSecret, clearData: data)
        }
        
        log.debug("encrypting asset \(asset.globalIdentifier): generated encrypted payloads")
        
        var encryptedVersions = [SHAssetQuality: SHEncryptedAssetVersion]()
        
        for recipient in recipients {
            ///
            /// Encrypt the secret using the recipient's public key
            /// so that it can be stored securely on the server
            ///
            let encryptedAssetSecret = try self.user.createShareablePayload(
                from: privateSecret.rawRepresentation,
                toShareWith: recipient
            )
            
            for (idx, quality) in versions.enumerated() {
                encryptedVersions[quality] = SHGenericEncryptedAssetVersion(
                    quality: quality,
                    encryptedData: encryptedPayloads[idx].encryptedData,
                    encryptedSecret: encryptedAssetSecret.cyphertext,
                    publicKeyData: encryptedAssetSecret.ephemeralPublicKeyData,
                    publicSignatureData: encryptedAssetSecret.signature
                )
            }
        }
        
        return SHGenericEncryptedAsset(
            globalIdentifier: asset.globalIdentifier,
            localIdentifier: asset.localIdentifier,
            fingerprint: asset.fingerprint,
            creationDate: asset.creationDate,
            encryptedVersions: encryptedVersions
        )
    }
    
    /// Encrypt the asset for the reciepients. This method is called by both the SHEncryptionOperation and the SHEncryptAndShareOperation. In the first case, the recipient will be the user (for backup), in the second it's the list of users to share the assets with
    /// - Parameters:
    ///   - asset: the asset to encrypt
    ///   - versions: the versions to encrypt
    ///   - privateSecret: the secret to use for the encryption
    ///   - recipients: the list of recipients (using their public key for encryption)
    /// - Returns: the encrypted asset
    func generateEncryptedAsset(
        asset: SHUploadableAsset,
        versions: [SHAssetQuality],
        usingPrivateSecret privateSecret: Data,
        recipients: [any SHServerUser]
    ) async throws -> any SHEncryptedAsset {
        let encryptedAsset: any SHEncryptedAsset
        
        let encryptStart = CFAbsoluteTimeGetCurrent()
        do {
            encryptedAsset = try self.encrypt(
                asset: asset,
                versions: versions,
                for: recipients,
                usingSecret: privateSecret
            )
        } catch {
            log.error("failed to encrypt data for asset \(asset.globalIdentifier). Dequeueing item, as it's unlikely to succeed again.")
            throw SHBackgroundOperationError.fatalError("failed to encrypt data for asset \(asset.globalIdentifier)")
        }
        let encryptEnd = CFAbsoluteTimeGetCurrent()
        log.debug("[PERF] it took \(CFAbsoluteTime(encryptEnd - encryptStart)) to encrypt the data. versions=\(versions)")
        
        return encryptedAsset
    }
    
    public func markAsFailed(
        queueItem: KBQueueItem,
        encryptionRequest request: SHEncryptionRequestQueueItem,
        error: Error,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        let globalIdentifier = request.asset.globalIdentifier
        let versions = request.versions
        let groupId = request.groupId
        let users = request.sharedWith
        
        ///
        /// Dequeue from Encryption queue
        ///
        log.info("dequeueing request for asset \(globalIdentifier) from the ENCRYPT queue")
        
        do {
            let encryptionQueue = try BackgroundOperationQueue.of(type: .encryption)
            _ = try encryptionQueue.dequeue(item: queueItem)
        }
        catch {
            log.warning("dequeuing failed of unexpected data in ENCRYPT queue. This task will be attempted again.")
            completionHandler(.failure(error))
            return
        }
        
        self.markAsFailed(
            request: request,
            error: error
        )
        
        self.markLocalAssetAsFailed(globalIdentifier: globalIdentifier, versions: versions) {
            guard request.isBackground == false else {
                /// Avoid other side-effects for background  `SHEncryptionRequestQueueItem`
                completionHandler(.success(()))
                return
            }
            
            let assetsDelegates = self.assetsDelegates
            self.delegatesQueue.async {
                /// Notify the delegates
                for delegate in assetsDelegates {
                    if let delegate = delegate as? SHAssetEncrypterDelegate {
                        delegate.didFailEncryption(ofAsset: globalIdentifier, in: groupId, error: error)
                    }
                    if users.count > 0 {
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
            
            completionHandler(.success(()))
        }
    }
    
    public func markAsSuccessful(
        queueItem: KBQueueItem,
        encryptionRequest request: SHGenericShareableGroupableQueueItem,
        globalIdentifier: String
    ) throws {
        let globalIdentifier = request.asset.globalIdentifier
        let versions = request.versions
        let groupId = request.groupId
        let isBackground = request.isBackground
        
        do {
            ///
            /// Dequeue from Encryption queue
            ///
            log.info("dequeueing item \(queueItem.identifier) from the ENCRYPT queue")
            let encryptionQueue = try BackgroundOperationQueue.of(type: .encryption)
            _ = try encryptionQueue.dequeue(item: queueItem)
        } catch {
            log.warning("item \(queueItem.identifier) was completed but dequeuing failed. This task will be attempted again.")
            throw error
        }
        
        do {
            ///
            /// Enqueue to Upload queue
            ///
            log.info("enqueueing upload request in the UPLOAD queue for asset \(globalIdentifier) versions \(versions) isBackground=\(isBackground)")
            
            let uploadQueue = try BackgroundOperationQueue.of(type: .upload)
            try request.enqueue(in: uploadQueue, with: request.identifier)
        } catch {
            log.fault("asset \(globalIdentifier) was encrypted but will never be uploaded because enqueueing to UPLOAD queue failed")
            throw error
        }
        
        guard request.isBackground == false else {
            /// Avoid other side-effects for background  `SHEncryptionRequestQueueItem`
            return
        }
        
        let assetsDelegates = self.assetsDelegates
        self.delegatesQueue.async {
            /// Notify the delegates
            for delegate in assetsDelegates {
                if let delegate = delegate as? SHAssetEncrypterDelegate {
                    delegate.didCompleteEncryption(ofAsset: globalIdentifier, in: groupId)
                }
            }
        }
    }
    
    internal func process(
        _ queueItem: KBQueueItem,
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        let encryptionRequest: SHEncryptionRequestQueueItem
        
        do {
            let content = try content(ofQueueItem: queueItem)
            guard let content = content as? SHEncryptionRequestQueueItem else {
                log.error("unexpected data found in ENCRYPT queue. Dequeueing")
                // Delegates can't be called as item content can't be read and it will be silently removed from the queue
                throw SHBackgroundOperationError.unexpectedData(queueItem.content)
            }
            encryptionRequest = content
        } catch {
            do {
                let encryptionQueue = try BackgroundOperationQueue.of(type: .encryption)
                _ = try encryptionQueue.dequeue(item: queueItem)
            }
            catch {
                log.fault("dequeuing failed of unexpected data in ENCRYPT queue")
            }
            completionHandler(.failure(error))
            return
        }
        
        let asset = encryptionRequest.asset
        
        let handleError = { (error: Error) in
            self.log.error("FAIL in ENCRYPT: \(error.localizedDescription)")
            
            self.markAsFailed(
                queueItem: queueItem,
                encryptionRequest: encryptionRequest,
                error: error
            ) { _ in
                completionHandler(.failure(error))
            }
        }
        
        if encryptionRequest.isBackground == false {
            let assetsDelegates = self.assetsDelegates
            self.delegatesQueue.async {
                for delegate in assetsDelegates {
                    if let delegate = delegate as? SHAssetEncrypterDelegate {
                        delegate.didStartEncryption(
                            ofAsset: encryptionRequest.asset.globalIdentifier,
                            in: encryptionRequest.groupId
                        )
                    }
                }
            }
            
            do {
                ///
                /// As soon as the global identifier can be calculated (because the asset is fetched and ready to be encrypted)
                /// ingest that identifier into the graph as a provisional share.
                ///
                try SHKGQuery.ingestProvisionalShare(
                    of: asset.globalIdentifier,
                    localIdentifier: encryptionRequest.asset.localIdentifier,
                    from: self.user.identifier,
                    to: encryptionRequest.sharedWith.map({ $0.identifier })
                )
            } catch {
                handleError(error)
                return
            }
        }
        
        ///
        /// The symmetric key is used to encrypt this asset (all of its version) moving forward.
        /// For assets that are already in the local store, do a best effort to retrieve the key from the store.
        /// The first time the asset the `retrieveCommonEncryptionKey` will throw a `missingAssetInLocalServer` error, so a new one is generated.
        /// The key is stored unencrypted in the local database, but it's encrypted for each user (including self)
        /// before leaving the device (at remote asset saving or sharing time)
        ///
        var privateSecret: SymmetricKey? = nil
        var secretRetrievalError: Error? = nil
        let dispatchGroup = DispatchGroup()
        
        dispatchGroup.enter()
        SHLocalAssetStoreController(user: self.user).retrieveCommonEncryptionKey(
            for: asset.globalIdentifier,
            signedBy: self.user
        ) {
            result in
            switch result {
            case .failure(let error):
                switch error {
                case SHBackgroundOperationError.missingAssetInLocalServer(_):
                    privateSecret = SymmetricKey(size: .bits256)
                default:
                    secretRetrievalError = error
                }
            case .success(let privateSecretData):
                do {
                    privateSecret = try SymmetricKey(rawRepresentation: privateSecretData)
                } catch {
                    secretRetrievalError = error
                }
            }
            
            dispatchGroup.leave()
        }
        
        dispatchGroup.notify(queue: .global(qos: qos)) {
            guard secretRetrievalError == nil else {
                handleError(secretRetrievalError!)
                return
            }
            guard let privateSecret else {
                handleError(SHBackgroundOperationError.fatalError("failed to retrieve secret"))
                return
            }
            
            Task(priority: qos.toTaskPriority()) {
                let encryptedAsset: any SHEncryptedAsset
                
                do {
                    encryptedAsset = try await self.generateEncryptedAsset(
                        asset: asset,
                        versions: encryptionRequest.versions,
                        usingPrivateSecret: privateSecret.rawRepresentation,
                        recipients: [self.user]
                    )
                } catch {
                    handleError(error)
                    return
                }
                
                self.log.info("storing asset \(encryptedAsset.globalIdentifier) and encryption secrets for SELF user in local server proxy")
                
                self.serverProxy.localServer.create(
                    assets: [encryptedAsset],
                    groupId: encryptionRequest.groupId,
                    createdBy: self.user,
                    createdAt: Date(),
                    createdFromThreadId: encryptionRequest.asPhotoMessageInThreadId,
                    permissions: encryptionRequest.permissions,
                    filterVersions: nil,
                    overwriteFileIfExists: true
                ) { result in
                    switch result {
                    case .failure(let error):
                        handleError(error)
                    case .success(_):
                        ///
                        /// Encryption is completed.
                        /// Remove the item in the encrypt queue and add to the upload queue
                        ///
                        do {
                            try self.markAsSuccessful(
                                queueItem: queueItem,
                                encryptionRequest: encryptionRequest,
                                globalIdentifier: encryptedAsset.globalIdentifier
                            )
                            completionHandler(.success(()))
                        } catch {
                            self.log.critical("failed to mark ENCRYPT as successful. This will likely cause infinite loops")
                            handleError(error)
                        }
                    }
                }
            }
        }
    }
}
