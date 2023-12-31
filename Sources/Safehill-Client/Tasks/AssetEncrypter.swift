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
    
    /// Generate a shareable version of the asset, namely a structure that can be used by the sender
    /// to securely share an asset with the specified recipients.
    ///
    /// - Parameters:
    ///   - globalIdentifier: the asset global identifier
    ///   - versions: the versions to share (resulting in `SHShareableEncryptedAssetVersion` in the `SHShareableEncryptedAsset` returned)
    ///   - sender: the user wanting to share the asset
    ///   - recipients: the list of users the asset should be made shareable to
    ///   - groupId: the unique identifier of the share request
    /// - Returns: the `SHShareableEncryptedAsset`
    func shareableEncryptedAsset(globalIdentifier: String,
                                 versions: [SHAssetQuality],
                                 sender: SHLocalUser,
                                 recipients: [any SHServerUser],
                                 groupId: String) throws -> any SHShareableEncryptedAsset {
        let privateSecret = try self.retrieveCommonEncryptionKey(sender: sender, globalIdentifier: globalIdentifier)
        var shareableVersions = [SHShareableEncryptedAssetVersion]()
        
        for recipient in recipients {
            ///
            /// Encrypt the secret using the recipient's public key
            /// so that it can be stored securely on the server
            ///
            let encryptedAssetSecret = try sender.createShareablePayload(
                from: privateSecret,
                toShareWith: recipient
            )
            
            for quality in versions {
                let shareableVersion = SHGenericShareableEncryptedAssetVersion(
                    quality: quality,
                    userPublicIdentifier: recipient.identifier,
                    encryptedSecret: encryptedAssetSecret.cyphertext,
                    ephemeralPublicKey: encryptedAssetSecret.ephemeralPublicKeyData,
                    publicSignature: encryptedAssetSecret.signature
                )
                shareableVersions.append(shareableVersion)
            }
        }
        
        return SHGenericShareableEncryptedAsset(
            globalIdentifier: globalIdentifier,
            sharedVersions: shareableVersions,
            groupId: groupId
        )
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
    fileprivate func retrieveCommonEncryptionKey(
        sender user: SHLocalUser,
        globalIdentifier: String
    ) throws -> Data {
        let quality = SHAssetQuality.lowResolution // Common encryption key (private secret) is constant across all versions. Any SHAssetQuality will return the same value
        
        let encryptedAsset = try SHLocalAssetStoreController(user: user)
            .encryptedAsset(
                with: globalIdentifier,
                versions: [quality],
                cacheHiResolution: false
            )
        guard let version = encryptedAsset.encryptedVersions[quality] else {
            log.error("failed to retrieve shared secret for asset \(globalIdentifier)")
            throw SHBackgroundOperationError.missingAssetInLocalServer(globalIdentifier)
        }
        guard let salt = user.encryptionProtocolSalt else {
            log.error("No protocol salt set from server")
            throw SHBackgroundOperationError.fatalError("No protocol salt set from server")
        }
        
        let encryptedSecret = SHShareablePayload(
            ephemeralPublicKeyData: version.publicKeyData,
            cyphertext: version.encryptedSecret,
            signature: version.publicSignatureData
        )
        return try SHCypher.decrypt(
            encryptedSecret,
            encryptionKeyData: user.shUser.privateKeyData,
            protocolSalt: salt,
            from: user.publicSignatureData
        )
    }
    
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

open class SHEncryptionOperation: SHAbstractBackgroundOperation, SHUploadStepBackgroundOperation, SHBackgroundQueueProcessorOperationProtocol {
    
    public var log: Logger {
        Logger(subsystem: "com.gf.safehill", category: "BG-ENCRYPT")
    }
    
    public let limit: Int
    public let user: SHLocalUser
    public var delegates: [SHOutboundAssetOperationDelegate]
    
    var imageManager: PHCachingImageManager
    
    public init(user: SHLocalUser,
                delegates: [SHOutboundAssetOperationDelegate],
                limitPerRun limit: Int,
                imageManager: PHCachingImageManager? = nil) {
        self.user = user
        self.limit = limit
        self.delegates = delegates
        self.imageManager = imageManager ?? PHCachingImageManager()
    }
    
    var serverProxy: SHServerProxy {
        SHServerProxy(user: self.user)
    }
    
    public func clone() -> SHBackgroundOperationProtocol {
        SHEncryptionOperation(
            user: self.user,
            delegates: self.delegates,
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
        request: SHEncryptionRequestQueueItem) throws -> any SHEncryptedAsset
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
        globalIdentifier: String?
    ) throws
    {
        let localIdentifier = request.localIdentifier
        let versions = request.versions
        let groupId = request.groupId
        let eventOriginator = request.eventOriginator
        let users = request.sharedWith
        
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
            throw error
        }
        
        self.markAsFailed(
            localIdentifier: localIdentifier,
            versions: versions,
            groupId: groupId,
            eventOriginator: eventOriginator,
            sharedWith: users,
            isBackground: request.isBackground
        )
        
        if let globalIdentifier = globalIdentifier {
            self.markLocalAssetAsFailed(globalIdentifier: globalIdentifier, versions: versions)
        }
        
        guard request.isBackground == false else {
            /// Avoid other side-effects for background  `SHEncryptionRequestQueueItem`
            return
        }
        
        /// Notify the delegates
        for delegate in delegates {
            if let delegate = delegate as? SHAssetEncrypterDelegate {
                delegate.didFailEncryption(queueItemIdentifier: request.identifier)
            }
            if users.count > 0 {
                if let delegate = delegate as? SHAssetSharerDelegate {
                    delegate.didFailSharing(queueItemIdentifier: request.identifier)
                }
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
        for delegate in delegates {
            if let delegate = delegate as? SHAssetEncrypterDelegate {
                delegate.didCompleteEncryption(queueItemIdentifier: uploadRequest.identifier)
            }
        }
    }
    
    private func process(_ item: KBQueueItem) {
        
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
            return
        }
        
        var globalIdentifier: GlobalIdentifier? = nil
        
        do {
            let asset = encryptionRequest.asset
            let encryptedAsset: any SHEncryptedAsset
            
            if encryptionRequest.isBackground == false {
                for delegate in delegates {
                    if let delegate = delegate as? SHAssetEncrypterDelegate {
                        delegate.didStartEncryption(queueItemIdentifier: encryptionRequest.identifier)
                    }
                }
            }
            
            /// 
            /// At this point the global identifier should be calculated by the `SHLocalFetchOperation`,
            /// serialized and deserialized as part of the `SHApplePhotoAsset` object.
            ///
            globalIdentifier = try asset.retrieveOrGenerateGlobalIdentifier()
            
            if encryptionRequest.isBackground == false {
                ///
                /// As soon as the global identifier can be calculated (because the asset is fetched and ready to be encrypted)
                /// ingest that identifier into the graph as a provisional share.
                ///
                try SHKGQuery.ingestShare(
                    of: globalIdentifier!,
                    from: self.user.identifier,
                    to: encryptionRequest.sharedWith.map({ $0.identifier }),
                    provisional: true
                )
            }
            
            
            ///
            /// The symmetric key is used to encrypt this asset (all of its version) moving forward.
            /// For assets that are already in the local store, do a best effort to retrieve the key from the store.
            /// The first time the asset the `retrieveCommonEncryptionKey` will throw a `missingAssetInLocalServer` error, so a new one is generated.
            /// The key is stored unencrypted in the local database, but it's encrypted for each user (including self)
            /// before leaving the device (at remote asset saving or sharing time)
            ///
            let privateSecret: SymmetricKey
            do {
                let privateSecretData = try asset.retrieveCommonEncryptionKey(
                    sender: self.user,
                    globalIdentifier: globalIdentifier!
                )
                privateSecret = try SymmetricKey(rawRepresentation: privateSecretData)
            } catch SHBackgroundOperationError.missingAssetInLocalServer(_) {
                privateSecret = SymmetricKey(size: .bits256)
            }
            
            encryptedAsset = try self.generateEncryptedAsset(
                for: asset,
                with: globalIdentifier!,
                usingPrivateSecret: privateSecret.rawRepresentation,
                recipients: [self.user],
                request: encryptionRequest
            )
            
            var error: Error? = nil
            let group = DispatchGroup()
            
            log.info("storing asset \(encryptedAsset.globalIdentifier) and encryption secrets for SELF user in local server proxy")
            
            group.enter()
            serverProxy.localServer.create(
                assets: [encryptedAsset],
                groupId: encryptionRequest.groupId,
                filterVersions: nil
            ) { result in
                if case .failure(let err) = result {
                    error = err
                }
                group.leave()
            }
            
            let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds))
            guard dispatchResult == .success, error == nil else {
                log.error("failed to store data for localIdentifier \(asset.phAsset.localIdentifier). Dequeueing item, as it's unlikely to succeed again.")
                
                throw SHBackgroundOperationError.fatalError("failed to create local asset")
            }
            
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
            } catch {
                log.critical("failed to mark ENCRYPT as successful. This will likely cause infinite loops")
                throw error
            }
        } catch {
            do {
                try self.markAsFailed(item: item, encryptionRequest: encryptionRequest, globalIdentifier: globalIdentifier)
            } catch {
                log.critical("failed to mark ENCRYPT as failed. This will likely cause infinite loops")
                // TODO: Handle
            }
        }
    }
    
    public func run(forQueueItemIdentifiers queueItemIdentifiers: [String]) throws {
        let encryptionQueue = try BackgroundOperationQueue.of(type: .encryption)
        
        var queueItems = [KBQueueItem]()
        var error: Error? = nil
        let group = DispatchGroup()
        group.enter()
        encryptionQueue.retrieveItems(withIdentifiers: queueItemIdentifiers) {
            result in
            switch result {
            case .success(let items):
                queueItems = items
            case .failure(let err):
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
        
        for item in queueItems {
            guard processingState(for: item.identifier) != .encrypting else {
                continue
            }
            
            log.info("encrypting item \(item.identifier) created at \(item.createdAt)")
            
            setProcessingState(.encrypting, for: item.identifier)
            
            self.process(item)
            log.info("[√] encryption task completed for item \(item.identifier)")
            
            setProcessingState(nil, for: item.identifier)
        }
    }
    
    public func runOnce(maxItems: Int? = nil) throws {
        var count = 0
        let encryptionQueue = try BackgroundOperationQueue.of(type: .encryption)
        
        while let item = try encryptionQueue.peek() {
            guard maxItems == nil || count < maxItems! else {
                break
            }
            guard processingState(for: item.identifier) != .encrypting else {
                continue
            }
            
            log.info("encrypting item \(item.identifier) created at \(item.createdAt)")
            
            setProcessingState(.encrypting, for: item.identifier)
            
            self.process(item)
            log.info("[√] encryption task completed for item \(item.identifier)")
            
            setProcessingState(nil, for: item.identifier)
            
            count += 1
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
            let encryptionQueue = try BackgroundOperationQueue.of(type: .encryption)
            items = try encryptionQueue.peekNext(self.limit)
        } catch {
            log.error("failed to fetch items from the ENCRYPT queue")
            state = .finished
            return
        }
        
        for item in items {
            guard processingState(for: item.identifier) != .encrypting else {
                break
            }
            
            log.info("encrypting item \(item.identifier) created at \(item.createdAt)")
            
            setProcessingState(.encrypting, for: item.identifier)
            
            DispatchQueue.global().async { [self] in
                guard !isCancelled else {
                    log.info("encryption task cancelled. Finishing")
                    setProcessingState(nil, for: item.identifier)
                    return
                }
                
                self.process(item)
                log.info("[√] encryption task completed for item \(item.identifier)")
                
                setProcessingState(nil, for: item.identifier)
            }
                
            guard !self.isCancelled else {
                log.info("encryption task cancelled. Finishing")
                break
            }
        }
        
        state = .finished
    }
}

public class SHAssetsEncrypterQueueProcessor : SHBackgroundOperationProcessor<SHEncryptionOperation> {
    /// Singleton (with private initializer)
    public static var shared = SHAssetsEncrypterQueueProcessor(
        delayedStartInSeconds: 3,
        dispatchIntervalInSeconds: 2
    )
    
    private override init(delayedStartInSeconds: Int = 0,
                          dispatchIntervalInSeconds: Int? = nil) {
        super.init(delayedStartInSeconds: delayedStartInSeconds, dispatchIntervalInSeconds: dispatchIntervalInSeconds)
    }
}
