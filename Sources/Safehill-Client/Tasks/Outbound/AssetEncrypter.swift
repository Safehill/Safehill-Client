import Foundation
import Safehill_Crypto
import KnowledgeBase
import Photos
import os
import CryptoKit
#if os(iOS)
import UIKit
#else
import AppKit
#endif


extension SHApplePhotoAsset {
    func data(for versions: [SHAssetQuality]) -> [SHAssetQuality: Result<Data, Error>] {
        var dict = [SHAssetQuality: Result<Data, Error>]()
        
        for version in versions {
            let size = kSHSizeForQuality(quality: version)
            self.phAsset.data(
                forSize: size,
                usingImageManager: self.imageManager,
                synchronousFetch: true,
                deliveryMode: .highQualityFormat
            ) { result in
                dict[version] = result
            }
        }

        return dict
    }
}

internal class SHEncryptionOperation: Operation, SHBackgroundQueueProcessorOperationProtocol, SHOutboundBackgroundOperation, SHUploadStepBackgroundOperation {
    
    typealias OperationResult = Result<Void, Error>
    
    
    var operationType: BackgroundOperationQueue.OperationType { .encryption }
    var processingState: ProcessingState { .encrypting }
    
    public var log: Logger {
        Logger(subsystem: "com.gf.safehill", category: "BG-ENCRYPT")
    }
    
    public let limit: Int
    public let user: SHAuthenticatedLocalUser
    public var assetDelegates: [SHOutboundAssetOperationDelegate]
    
    internal var interactionsController: SHUserInteractionController {
        SHUserInteractionController(user: self.user)
    }
    
    var imageManager: PHCachingImageManager
    
    public init(user: SHAuthenticatedLocalUser,
                assetsDelegates: [SHOutboundAssetOperationDelegate],
                limitPerRun limit: Int,
                imageManager: PHCachingImageManager? = nil) {
        self.user = user
        self.limit = limit
        self.assetDelegates = assetsDelegates
        self.imageManager = imageManager ?? PHCachingImageManager()
    }
    
    var serverProxy: SHServerProxy {
        self.user.serverProxy
    }
    
    public func clone() -> any SHBackgroundOperationProtocol {
        SHEncryptionOperation(
            user: self.user,
            assetsDelegates: self.assetDelegates,
            limitPerRun: self.limit,
            imageManager: self.imageManager
        )
    }
    
    public func content(ofQueueItem item: KBQueueItem) throws -> SHSerializableQueueItem {
        guard let data = item.content as? Data else {
            throw SHBackgroundOperationError.unexpectedData(item.content)
        }
        
        let unarchiver: NSKeyedUnarchiver
        if #available(macOS 10.13, *) {
            unarchiver = try NSKeyedUnarchiver(forReadingFrom: data)
        } else {
            unarchiver = NSKeyedUnarchiver(forReadingWith: data)
        }
        
        guard let encryptionRequest = unarchiver.decodeObject(of: SHEncryptionRequestQueueItem.self,
                                                              forKey: NSKeyedArchiveRootObjectKey)
        else {
            throw SHBackgroundOperationError.unexpectedData(data)
        }
        
        return encryptionRequest
    }
    
    private func encrypt(
        asset: SHApplePhotoAsset,
        with globalIdentifier: String,
        for recipients: [SHServerUser],
        usingSecret privateSecret: Data,
        payloads: [SHAssetQuality: Data]) throws -> any SHEncryptedAsset
    {
        log.info("encrypting asset \(globalIdentifier) versions to be shared with users \(recipients.map { $0.identifier }) using symmetric key \(privateSecret.base64EncodedString(), privacy: .private(mask: .hash))")
        
        ///
        /// Encrypt all asset versions with a new symmetric keys generated at encryption time
        ///
        let privateSecret = try SymmetricKey(rawRepresentation: privateSecret)
        let encryptedPayloads = try payloads.mapValues {
            try SHEncryptedData(privateSecret: privateSecret, clearData: $0)
        }
        
        log.debug("encrypting asset \(globalIdentifier): generated encrypted payloads")
        
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
            
            for quality in payloads.keys {
                encryptedVersions[quality] = SHGenericEncryptedAssetVersion(
                    quality: quality,
                    encryptedData: encryptedPayloads[quality]!.encryptedData,
                    encryptedSecret: encryptedAssetSecret.cyphertext,
                    publicKeyData: encryptedAssetSecret.ephemeralPublicKeyData,
                    publicSignatureData: encryptedAssetSecret.signature
                )
            }
        }
        
        return SHGenericEncryptedAsset(
            globalIdentifier: globalIdentifier,
            localIdentifier: asset.phAsset.localIdentifier,
            creationDate: asset.phAsset.creationDate,
            encryptedVersions: encryptedVersions
        )
    }
    
    /// Encrypt the asset for the reciepients. This method is called by both the SHEncryptionOperation and the SHEncryptAndShareOperation. In the first case, the recipient will be the user (for backup), in the second it's the list of users to share the assets with
    /// - Parameters:
    ///   - asset: the asset to encrypt
    ///   - recipients: the list of recipients (using their public key for encryption)
    ///   - item: the queue item
    ///   - request: the corresponding request
    /// - Returns: the encrypted asset
    func generateEncryptedAsset(
        for asset: SHApplePhotoAsset,
        with globalIdentifier: String,
        usingPrivateSecret privateSecret: Data,
        recipients: [SHServerUser],
        request: SHEncryptionRequestQueueItem
    ) throws -> any SHEncryptedAsset
    {
        let versions = request.versions
        
        let localIdentifier = asset.phAsset.localIdentifier
        var payloads: [SHAssetQuality: Data]
        
        let fetchStart = CFAbsoluteTimeGetCurrent()
        do {
            let dataResults = asset.data(for: versions)
            payloads = try dataResults.mapValues({
                switch $0 {
                case .success(let data):
                    return data
                case .failure(let err):
                    throw err
                }
            })
        } catch {
            log.error("failed to retrieve data for localIdentifier \(localIdentifier). Dequeueing item, as it's unlikely to succeed again.")
            throw SHBackgroundOperationError.fatalError("failed to retrieve data for localIdentifier \(localIdentifier)")
        }
        let fetchEnd = CFAbsoluteTimeGetCurrent()
        log.debug("[PERF] it took \(CFAbsoluteTime(fetchEnd - fetchStart)) to fetch the data to encrypt. versions=\(versions)")
        
        let encryptedAsset: any SHEncryptedAsset
        
        let encryptStart = CFAbsoluteTimeGetCurrent()
        do {
            encryptedAsset = try self.encrypt(
                asset: asset,
                with: globalIdentifier,
                for: recipients,
                usingSecret: privateSecret,
                payloads: payloads
            )
        } catch {
            log.error("failed to encrypt data for localIdentifier \(localIdentifier). Dequeueing item, as it's unlikely to succeed again.")
            throw SHBackgroundOperationError.fatalError("failed to encrypt data for localIdentifier \(localIdentifier)")
        }
        let encryptEnd = CFAbsoluteTimeGetCurrent()
        log.debug("[PERF] it took \(CFAbsoluteTime(encryptEnd - encryptStart)) to encrypt the data. versions=\(versions)")
        
        return encryptedAsset
    }
    
    public func markAsFailed(
        item: KBQueueItem,
        encryptionRequest request: SHEncryptionRequestQueueItem,
        globalIdentifier: String?,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        let localIdentifier = request.localIdentifier
        let versions = request.versions
        let groupId = request.groupId
        let eventOriginator = request.eventOriginator
        let users = request.sharedWith
        let shouldLinkToThread = request.shouldLinkToThread
        
        ///
        /// Dequeue from Encryption queue
        ///
        log.info("dequeueing request for asset \(localIdentifier) from the ENCRYPT queue")
        
        do {
            let encryptionQueue = try BackgroundOperationQueue.of(type: .encryption)
            _ = try encryptionQueue.dequeue(item: item)
        }
        catch {
            log.warning("dequeuing failed of unexpected data in ENCRYPT queue. This task will be attempted again.")
            completionHandler(.failure(error))
            return
        }
        
        self.markAsFailed(
            localIdentifier: localIdentifier,
            versions: versions,
            groupId: groupId,
            eventOriginator: eventOriginator,
            sharedWith: users,
            shouldLinkToThread: shouldLinkToThread,
            isBackground: request.isBackground
        )
        
        if let globalIdentifier = globalIdentifier {
            self.markLocalAssetAsFailed(globalIdentifier: globalIdentifier, versions: versions) {
                guard request.isBackground == false else {
                    /// Avoid other side-effects for background  `SHEncryptionRequestQueueItem`
                    completionHandler(.success(()))
                    return
                }
                
                /// Notify the delegates
                for delegate in self.assetDelegates {
                    if let delegate = delegate as? SHAssetEncrypterDelegate {
                        delegate.didFailEncryption(queueItemIdentifier: request.identifier)
                    }
                    if users.count > 0 {
                        if let delegate = delegate as? SHAssetSharerDelegate {
                            delegate.didFailSharing(queueItemIdentifier: request.identifier)
                        }
                    }
                }
                
                completionHandler(.success(()))
            }
        }
    }
    
    public func markAsSuccessful(
        item: KBQueueItem,
        encryptionRequest request: SHEncryptionRequestQueueItem,
        globalIdentifier: String
    ) throws {
        let localIdentifier = request.localIdentifier
        let versions = request.versions
        let groupId = request.groupId
        let eventOriginator = request.eventOriginator
        let users = request.sharedWith
        let isBackground = request.isBackground
        let shouldLinkToThread = request.shouldLinkToThread
        
        do {
            ///
            /// Dequeue from Encryption queue
            ///
            log.info("dequeueing item \(item.identifier) from the ENCRYPT queue")
            let encryptionQueue = try BackgroundOperationQueue.of(type: .encryption)
            _ = try encryptionQueue.dequeue(item: item)
        } catch {
            log.warning("item \(item.identifier) was completed but dequeuing failed. This task will be attempted again.")
            throw error
        }
        
        
        let uploadRequest = SHUploadRequestQueueItem(
            localAssetId: localIdentifier,
            globalAssetId: globalIdentifier,
            versions: versions,
            groupId: groupId,
            eventOriginator: eventOriginator,
            sharedWith: users,
            shouldLinkToThread: shouldLinkToThread,
            isBackground: isBackground
        )
        
        do {
            ///
            /// Enqueue to Upload queue
            ///
            log.info("enqueueing upload request in the UPLOAD queue for asset \(localIdentifier) versions \(versions) isBackground=\(isBackground)")
            
            let uploadQueue = try BackgroundOperationQueue.of(type: .upload)
            try uploadRequest.enqueue(in: uploadQueue)
        } catch {
            log.fault("asset \(localIdentifier) was encrypted but will never be uploaded because enqueueing to UPLOAD queue failed")
            throw error
        }
        
        guard request.isBackground == false else {
            /// Avoid other side-effects for background  `SHEncryptionRequestQueueItem`
            return
        }
        /// Notify the delegates
        for delegate in assetDelegates {
            if let delegate = delegate as? SHAssetEncrypterDelegate {
                delegate.didCompleteEncryption(queueItemIdentifier: uploadRequest.identifier)
            }
        }
    }
    
    internal func process(
        _ item: KBQueueItem,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        let encryptionRequest: SHEncryptionRequestQueueItem
        
        do {
            let content = try content(ofQueueItem: item)
            guard let content = content as? SHEncryptionRequestQueueItem else {
                log.error("unexpected data found in ENCRYPT queue. Dequeueing")
                // Delegates can't be called as item content can't be read and it will be silently removed from the queue
                throw SHBackgroundOperationError.unexpectedData(item.content)
            }
            encryptionRequest = content
        } catch {
            do {
                let encryptionQueue = try BackgroundOperationQueue.of(type: .encryption)
                _ = try encryptionQueue.dequeue(item: item)
            }
            catch {
                log.fault("dequeuing failed of unexpected data in ENCRYPT queue")
            }
            completionHandler(.failure(error))
            return
        }
        
        let asset = encryptionRequest.asset
        let globalIdentifier: GlobalIdentifier
        
        do {
            ///
            /// At this point the global identifier should be calculated by the `SHLocalFetchOperation`,
            /// serialized and deserialized as part of the `SHApplePhotoAsset` object.
            ///
            globalIdentifier = try asset.retrieveOrGenerateGlobalIdentifier()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        let handleError = { (error: Error) in
            self.log.error("FAIL in ENCRYPT: \(error.localizedDescription)")
            
            self.markAsFailed(
                item: item,
                encryptionRequest: encryptionRequest,
                globalIdentifier: globalIdentifier
            ) { _ in
                completionHandler(.failure(error))
            }
        }
        
        if encryptionRequest.isBackground == false {
            for delegate in assetDelegates {
                if let delegate = delegate as? SHAssetEncrypterDelegate {
                    delegate.didStartEncryption(queueItemIdentifier: encryptionRequest.identifier)
                }
            }
            
            do {
                ///
                /// As soon as the global identifier can be calculated (because the asset is fetched and ready to be encrypted)
                /// ingest that identifier into the graph as a provisional share.
                ///
                try SHKGQuery.ingestProvisionalShare(
                    of: globalIdentifier,
                    localIdentifier: encryptionRequest.localIdentifier,
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
            for: globalIdentifier
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
        
        dispatchGroup.notify(queue: .global()) {
            guard secretRetrievalError == nil else {
                handleError(secretRetrievalError!)
                return
            }
            guard let privateSecret else {
                handleError(SHBackgroundOperationError.fatalError("failed to retrieve secret"))
                return
            }
            
            let encryptedAsset: any SHEncryptedAsset
            
            do {
                encryptedAsset = try self.generateEncryptedAsset(
                    for: asset,
                    with: globalIdentifier,
                    usingPrivateSecret: privateSecret.rawRepresentation,
                    recipients: [self.user],
                    request: encryptionRequest
                )
            } catch {
                handleError(error)
                return
            }
            
            self.log.info("storing asset \(encryptedAsset.globalIdentifier) and encryption secrets for SELF user in local server proxy")
            
            self.serverProxy.localServer.create(
                assets: [encryptedAsset],
                groupId: encryptionRequest.groupId,
                filterVersions: nil
            ) { result in
                switch result {
                case .failure(let error):
                    handleError(error)
                case .success(_):
                    ///
                    /// Encryption is completed.
                    /// Create an item in the history queue for this upload, and remove the one in the upload queue
                    ///
                    do {
                        try self.markAsSuccessful(
                            item: item,
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
    
    public func run(
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        self.runOnce(completionHandler: completionHandler)
    }
}
