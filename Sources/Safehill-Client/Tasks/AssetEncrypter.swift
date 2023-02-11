import Foundation
import Safehill_Crypto
import KnowledgeBase
import Photos
import os
import CryptoKit


extension SHApplePhotoAsset {
    
    /// This operation is expensive if the asset is not cached. Use it carefully
    func generateGlobalIdentifier(using imageManager: PHImageManager) throws -> String {
        return try self.phAsset.globalIdentifier(using: imageManager)
    }
}

open class SHEncryptionOperation: SHAbstractBackgroundOperation, SHBackgroundQueueProcessorOperationProtocol {
    
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
    
    public var serverProxy: SHServerProxy {
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
    
    private func retrieveAssetData(for asset: SHApplePhotoAsset, versions: [SHAssetQuality]) -> [SHAssetQuality: Result<Data, Error>] {
        var dict = [SHAssetQuality: Result<Data, Error>]()
        
        let sizeForVersion: [SHAssetQuality: CGSize] = versions.reduce([:]) { partialResult, quality in
            var res = partialResult
            switch quality {
            case .lowResolution:
                res[quality] = kSHLowResPictureSize
            case .midResolution:
                res[quality] = kSHMidResPictureSize
            case .hiResolution:
                res[quality] = kSHHiResPictureSize
//            case .fullResolution:
//                result[quality] = kSHFullResPictureSize
            }
            return res
        }
        
        for version in versions {
            let size = sizeForVersion[version]!
            
            do {
                let data = try asset.cachedData(forSize: size)
                if let data = data {
                    dict[version] = Result<Data, Error>.success(data)
                } else {
                    asset.phAsset.data(forSize: size,
                                       usingImageManager: imageManager,
                                       synchronousFetch: true) { (result: Result<Data, Error>) in
                        dict[version] = result
#if DEBUG
                        if case .success(let data) = result {
                            let bcf = ByteCountFormatter()
                            bcf.allowedUnits = [.useMB] // optional: restricts the units to MB only
                            bcf.countStyle = .file
                            self.log.debug("\(version.rawValue) bytes (\(bcf.string(fromByteCount: Int64(data.count))))")
                        }
#endif
                    }
                }
            } catch {
                dict[version] = Result.failure(error)
            }
        }

        return dict
    }
    
    private func encrypt(
        asset: SHApplePhotoAsset,
        usingSecret privateSecret: Data,
        payloads: [SHAssetQuality: Data],
        forSharingWith users: [SHServerUser]) throws -> any SHEncryptedAsset
    {
        let globalIdentifier = try asset.generateGlobalIdentifier(using: self.imageManager)
        
        log.info("encrypting asset \(globalIdentifier) versions to be shared with users \(users.map { $0.identifier }) using symmetric key \(privateSecret.base64EncodedString(), privacy: .private(mask: .hash))")
        
        ///
        /// Encrypt all asset versions with a new symmetric keys generated at encryption time
        ///
        let privateSecret = try SymmetricKey(rawRepresentation: privateSecret)
        let encryptedPayloads = try payloads.mapValues {
            try SHEncryptedData(privateSecret: privateSecret, clearData: $0)
        }
        
        var versions = [SHAssetQuality: SHEncryptedAssetVersion]()
        
        for user in users {
            ///
            /// Encrypt the secret using the recipient's public key
            /// so that it can be stored securely on the server
            ///
            let encryptedAssetSecret = try self.user.shareable(
                data: privateSecret.rawRepresentation,
                with: user
            )
            
            for quality in payloads.keys {
                versions[quality] = SHGenericEncryptedAssetVersion(
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
            encryptedVersions: versions
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
        versions: [SHAssetQuality],
        usingPrivateSecret privateSecret: Data,
        recipients: [SHServerUser],
        request: SHEncryptionRequestQueueItem) throws -> any SHEncryptedAsset
    {
        let localIdentifier = asset.phAsset.localIdentifier
        var payloads: [SHAssetQuality: Data]
        
        do {
            let retrieveResults = self.retrieveAssetData(
                for: asset,
                versions: versions
            )
            payloads = try retrieveResults.mapValues({
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
        
        let encryptedAsset: any SHEncryptedAsset
        
        do {
            encryptedAsset = try self.encrypt(
                asset: asset,
                usingSecret: privateSecret,
                payloads: payloads,
                forSharingWith: recipients
            )
        } catch {
            log.error("failed to encrypt data for localIdentifier \(localIdentifier). Dequeueing item, as it's unlikely to succeed again.")
            throw SHBackgroundOperationError.fatalError("failed to encrypt data for localIdentifier \(localIdentifier)")
        }
        
        return encryptedAsset
    }
    
    private func markLocalAssetAsFailed(globalIdentifier: String) throws {
        let group = DispatchGroup()
        for quality in SHAssetQuality.all {
            group.enter()
            self.serverProxy.localServer.markAsset(with: globalIdentifier, quality: quality, as: .failed) { result in
                if case .failure(let err) = result {
                    self.log.info("failed to mark local asset \(globalIdentifier) as failed: \(err.localizedDescription)")
                }
                group.leave()
            }
        }
        
        let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds))
        
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
    }
    
    public func markAsFailed(
        item: KBQueueItem,
        localIdentifier: String,
        groupId: String,
        eventOriginator: SHServerUser,
        sharedWith users: [SHServerUser]) throws
    {
        ///
        /// Dequeue from Encryption queue
        ///
        log.info("dequeueing request for asset \(localIdentifier) from the ENCRYPT queue")
        
        do { _ = try EncryptionQueue.dequeue(item: item) }
        catch {
            log.warning("dequeuing failed of unexpected data in ENCRYPT queue. This task will be attempted again.")
        }
        
        ///
        /// Enquque to FailedUpload queue
        ///
        log.info("enqueueing upload request for asset \(localIdentifier) in the FAILED queue")
        
        let failedUploadQueueItem = SHFailedUploadRequestQueueItem(localIdentifier: localIdentifier,
                                                                   groupId: groupId,
                                                                   eventOriginator: eventOriginator,
                                                                   sharedWith: users)
        
        do {
            try failedUploadQueueItem.enqueue(in: FailedUploadQueue, with: localIdentifier)
        }
        catch {
            log.fault("asset \(localIdentifier) failed to upload but will never be recorded as failed because enqueueing to FAILED queue failed: \(error.localizedDescription)")
            throw error
        }
        
        ///
        /// Notify the delegates
        ///
        for delegate in delegates {
            if let delegate = delegate as? SHAssetEncrypterDelegate {
                delegate.didFailEncryption(
                    itemWithLocalIdentifier: localIdentifier,
                    groupId: groupId
                )
            }
        }
    }
    
    public func markAsSuccessful(
        item: KBQueueItem,
        localIdentifier: String,
        globalIdentifier: String,
        groupId: String,
        eventOriginator: SHServerUser,
        sharedWith: [SHServerUser]) throws
    {
        ///
        /// Dequeue from Encryption queue
        ///
        log.info("dequeueing item \(item.identifier) from the ENCRYPT queue")
        
        do { _ = try EncryptionQueue.dequeue(item: item) }
        catch {
            log.warning("item \(item.identifier) was completed but dequeuing failed. This task will be attempted again.")
        }
        
#if DEBUG
        log.debug("items in the ENCRYPT queue after dequeueing \((try? EncryptionQueue.peekItems(createdWithin: DateInterval(start: .distantPast, end: Date())))?.count ?? 0)")
#endif
        
        ///
        /// Enqueue to Upload queue
        ///
        let uploadRequest = SHUploadRequestQueueItem(localAssetId: localIdentifier,
                                                     globalAssetId: globalIdentifier,
                                                     groupId: groupId,
                                                     eventOriginator: eventOriginator,
                                                     sharedWith: sharedWith)
        log.info("enqueueing upload request in the UPLOAD queue for asset \(localIdentifier)")
        
        do { try uploadRequest.enqueue(in: UploadQueue, with: localIdentifier) }
        catch {
            log.fault("asset \(localIdentifier) was encrypted but will never be uploaded because enqueueing to UPLOAD queue failed")
            throw error
        }
        
#if DEBUG
        log.debug("items in the UPLOAD queue after enqueueing \((try? UploadQueue.peekItems(createdWithin: DateInterval(start: .distantPast, end: Date())))?.count ?? 0)")
#endif
        
        ///
        /// Notify the delegates
        ///
        for delegate in delegates {
            if let delegate = delegate as? SHAssetEncrypterDelegate {
                delegate.didCompleteEncryption(
                    itemWithLocalIdentifier: localIdentifier,
                    globalIdentifier: globalIdentifier,
                    groupId: groupId
                )
            }
        }
    }
    
    private func process(_ item: KBQueueItem) throws {
        
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
            do { _ = try EncryptionQueue.dequeue(item: item) }
            catch {
                log.warning("dequeuing failed of unexpected data in UPLOAD queue. This task will be attempted again.")
            }
            throw error
        }
        
        let asset = encryptionRequest.asset
        let encryptedAsset: any SHEncryptedAsset
        
        do {
            for delegate in delegates {
                if let delegate = delegate as? SHAssetEncrypterDelegate {
                    delegate.didStartEncryption(
                        itemWithLocalIdentifier: asset.phAsset.localIdentifier,
                        groupId: encryptionRequest.groupId
                    )
                }
            }
            
            ///
            /// Generate a new symmetric key, which will be used to encrypt this asset (all of its version) moving forward
            /// The key is stored unencrypted only in the local database, but encrypted for each user (including self)
            /// before leaving the device
            ///
            let privateSecret = SymmetricKey(size: .bits256)
            
            let versions: [SHAssetQuality] = [
                .lowResolution,
                .midResolution,
//                .hiResolution
            ]
            
            encryptedAsset = try self.generateEncryptedAsset(
                for: asset,
                versions: versions,
                usingPrivateSecret: privateSecret.rawRepresentation,
                recipients: [self.user],
                request: encryptionRequest
            )
            
            var error: Error? = nil
            let group = DispatchGroup()
            
            log.info("storing asset \(encryptedAsset.globalIdentifier) and encryption secrets for SELF user in local server proxy")
            
            group.enter()
            serverProxy.createLocalAssets([encryptedAsset],
                                          groupId: encryptionRequest.groupId,
                                          senderUserIdentifier: self.user.identifier) { result in
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
        } catch {
            do {
                try self.markAsFailed(item: item,
                                      localIdentifier: asset.phAsset.localIdentifier,
                                      groupId: encryptionRequest.groupId,
                                      eventOriginator: encryptionRequest.eventOriginator,
                                      sharedWith: encryptionRequest.sharedWith)
            } catch {
                log.critical("failed to mark ENCRYPT as failed. This will likely cause infinite loops")
                // TODO: Handle
            }
            
            throw error
        }
        
        ///
        /// Encryption is completed.
        /// Create an item in the history queue for this upload, and remove the one in the upload queue
        ///
        do {
            try self.markAsSuccessful(
                item: item,
                localIdentifier: asset.phAsset.localIdentifier,
                globalIdentifier: encryptedAsset.globalIdentifier,
                groupId: encryptionRequest.groupId,
                eventOriginator: encryptionRequest.eventOriginator,
                sharedWith: encryptionRequest.sharedWith
            )
        } catch {
            log.critical("failed to mark ENCRYPT as successful. This will likely cause infinite loops")
            // TODO: Handle
        }
    }
    
    public func runOnce() throws {
        while let item = try EncryptionQueue.peek() {
            guard processingState(for: item.identifier) != .encrypting else {
                break
            }
            
            log.info("encrypting item \(item.identifier) created at \(item.createdAt)")
            
            setProcessingState(.encrypting, for: item.identifier)
            
            do {
                try self.process(item)
                log.info("[√] encryption task completed for item \(item.identifier)")
            } catch {
                log.error("[x] encryption task failed for item \(item.identifier): \(error.localizedDescription)")
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
            items = try EncryptionQueue.peekNext(self.limit)
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
            
            DispatchQueue.global(qos: .background).async { [self] in
                guard !isCancelled else {
                    log.info("encryption task cancelled. Finishing")
                    setProcessingState(nil, for: item.identifier)
                    return
                }
                do {
                    try self.process(item)
                    log.info("[√] encryption task completed for item \(item.identifier)")
                } catch {
                    log.error("[x] encryption task failed for item \(item.identifier): \(error.localizedDescription)")
                }
                
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
