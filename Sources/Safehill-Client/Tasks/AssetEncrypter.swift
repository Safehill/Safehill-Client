import Foundation
import Safehill_Crypto
import KnowledgeBase
import Photos
import os
import Async
import CryptoKit


extension KBPhotoAsset {
    public func getCachedData(using imageManager: PHImageManager) throws -> Data {
        guard self.cachedData == nil else {
            log.trace("retriving high resolution asset \(self.phAsset.localIdentifier) from cache")
            return self.cachedData!
        }
        
        log.info("retrieving high resolution asset \(self.phAsset.localIdentifier) from Photos library")
        var error: Error? = nil
        var hiResData: Data? = nil
        self.phAsset.data(usingImageManager: imageManager,
                           synchronousFetch: true) { result in
            switch result {
            case .success(let d):
                hiResData = d
            case .failure(let err):
                error = err
            }
        }
        guard error == nil else {
            throw error!
        }
        
        self.cachedData = hiResData
        return hiResData!
    }
    
    
    /// This operation is expensive if the asset is not cached. Use it carefully
    func generateGlobalIdentifier(using imageManager: PHImageManager) throws -> String {
        return SHHash.stringDigest(for: try self.getCachedData(using: imageManager))
    }
}

open class SHEncryptionOperation: SHAbstractBackgroundOperation, SHBackgroundOperationProtocol {
    
    public var log: Logger {
        Logger(subsystem: "com.gf.safehill", category: "BG-ENCRYPT")
    }
    
    public let limit: Int?
    public let user: SHLocalUser
    public var delegates: [SHOutboundAssetOperationDelegate]
    var imageManager: PHCachingImageManager
    
    public init(user: SHLocalUser,
                delegates: [SHOutboundAssetOperationDelegate],
                limitPerRun limit: Int? = nil,
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
            throw KBError.unexpectedData(item.content)
        }
        
        let unarchiver: NSKeyedUnarchiver
        if #available(macOS 10.13, *) {
            unarchiver = try NSKeyedUnarchiver(forReadingFrom: data)
        } else {
            unarchiver = NSKeyedUnarchiver(forReadingWith: data)
        }
        
        guard let encryptionRequest = unarchiver.decodeObject(of: SHEncryptionRequestQueueItem.self, forKey: NSKeyedArchiveRootObjectKey) else {
            throw KBError.unexpectedData(item)
        }
        
        return encryptionRequest
    }
    
    private func retrieveAssetData(for asset: KBPhotoAsset, item: KBQueueItem) throws -> (Data, Data) {
        var (lowResData, hiResData): (Data?, Data?) = (nil, nil)
        
        log.info("retrieving low resolution asset \(asset.phAsset.localIdentifier)")
        
        var error: Error? = nil
        let size = CGSize(width: kSHLowResPictureSize.width, height: kSHLowResPictureSize.height)
        
        asset.phAsset.data(forSize: size,
                           usingImageManager: imageManager,
                           synchronousFetch: true) { result in
            switch result {
            case .success(let d):
                lowResData = d
            case .failure(let err):
                error = err
            }
        }
        
        if let error = error {
            throw error
        }
        
        hiResData = try asset.getCachedData(using: imageManager)
        
        #if DEBUG
        let bcf = ByteCountFormatter()
        bcf.allowedUnits = [.useMB] // optional: restricts the units to MB only
        bcf.countStyle = .file
        log.debug("lowRes bytes (\(bcf.string(fromByteCount: Int64(lowResData!.count))))")
        log.debug("hiRes bytes (\(bcf.string(fromByteCount: Int64(hiResData!.count))))")
        #endif

        return (lowResData!, hiResData!)
    }
    
    private func encrypt(
        asset: KBPhotoAsset,
        usingSecret privateSecret: Data,
        lowResData: Data,
        hiResData: Data,
        forSharingWith users: [SHServerUser],
        groupId: String) throws -> SHEncryptedAsset
    {
        let contentIdentifier = try asset.generateGlobalIdentifier(using: self.imageManager)
        
        log.info("encrypting asset \(contentIdentifier) versions to be shared with users \(users.map { $0.identifier }) using symmetric key \(privateSecret.base64EncodedString(), privacy: .private(mask: .hash))")
        
        // encrypt this asset with the symmetric keys generated at encryption time
        let privateSecret = try SymmetricKey(rawRepresentation: privateSecret)
        let hiResEncryptedContent = try SHEncryptedData(
            privateSecret: privateSecret,
            clearData: hiResData
        )
        let lowResEncryptedContent = try SHEncryptedData(
            privateSecret: privateSecret,
            clearData: lowResData
        )
                  
        log.debug("lowRes-encryptedData (\(contentIdentifier)): \(lowResEncryptedContent.encryptedData.base64EncodedString())")
        
        var versions = [SHEncryptedAssetVersion]()
        
        for user in users {
            /// Encrypt the secret using the recipient's public key
            /// so that it can be stored securely on the server
            let encryptedAssetSecret = try self.user.shareable(
                data: privateSecret.rawRepresentation,
                with: user
            )
            
            let lowResEncryptedVersion = SHGenericEncryptedAssetVersion(
                quality: .lowResolution,
                encryptedData: lowResEncryptedContent.encryptedData,
                encryptedSecret: encryptedAssetSecret.cyphertext,
                publicKeyData: encryptedAssetSecret.ephemeralPublicKeyData,
                publicSignatureData: encryptedAssetSecret.signature
            )
            let hiResEncryptedVersion = SHGenericEncryptedAssetVersion(
                quality: .hiResolution,
                encryptedData: hiResEncryptedContent.encryptedData,
                encryptedSecret: encryptedAssetSecret.cyphertext,
                publicKeyData: encryptedAssetSecret.ephemeralPublicKeyData,
                publicSignatureData: encryptedAssetSecret.signature
            )
            
            versions.append(lowResEncryptedVersion)
            versions.append(hiResEncryptedVersion)
        }
        
        return SHGenericEncryptedAsset(
            globalIdentifier: contentIdentifier,
            localIdentifier: asset.phAsset.localIdentifier,
            creationDate: asset.phAsset.creationDate,
            groupId: groupId,
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
        for asset: KBPhotoAsset,
        usingPrivateSecret privateSecret: Data,
        recipients: [SHServerUser],
        item: KBQueueItem,
        request: SHEncryptionRequestQueueItem) throws -> SHEncryptedAsset
    {
        var (lowResData, hiResData): (Data?, Data?) = (nil, nil)
        
        do {
            (lowResData, hiResData) = try self.retrieveAssetData(for: asset, item: item)
        } catch {
            log.error("failed to retrieve data for item \(item.identifier). Dequeueing item, as it's unlikely to succeed again.")
            
            do {
                try self.markAsFailed(localIdentifier: item.identifier,
                                      groupId: request.groupId,
                                      eventOriginator: request.eventOriginator,
                                      sharedWith: request.sharedWith)
            } catch {
                log.critical("failed to mark ENCRYPT as failed. This will likely cause infinite loops")
                // TODO: Handle
            }
            
            throw SHBackgroundOperationError.fatalError("failed to retrieve data for item \(item.identifier)")
        }
        
        guard let lowResData = lowResData, let hiResData = hiResData else {
            log.error("failed to retrieve data for item \(item.identifier). Dequeuing item because it's unlikely this will succeed again.")
            
            do {
                try self.markAsFailed(localIdentifier: item.identifier,
                                      groupId: request.groupId,
                                      eventOriginator: request.eventOriginator,
                                      sharedWith: request.sharedWith)
            } catch {
                log.critical("failed to mark ENCRYPT as failed. This will likely cause infinite loops")
                // TODO: Handle
            }
            
            throw SHBackgroundOperationError.fatalError("failed to retrieve data for item \(item.identifier)")
        }
        
        let encryptedAsset: SHEncryptedAsset
        
        do {
            encryptedAsset = try self.encrypt(
                asset: asset,
                usingSecret: privateSecret,
                lowResData: lowResData,
                hiResData: hiResData,
                forSharingWith: recipients,
                groupId: request.groupId
            )
        } catch {
            log.error("failed to encrypt data for item \(item.identifier). Dequeueing item, as it's unlikely to succeed again.")
            
            do {
                try self.markAsFailed(localIdentifier: item.identifier,
                                      groupId: request.groupId,
                                      eventOriginator: request.eventOriginator,
                                      sharedWith: request.sharedWith)
            } catch {
                log.critical("failed to mark ENCRYPT as failed. This will likely cause infinite loops")
                // TODO: Handle
            }
            
            throw SHBackgroundOperationError.fatalError("failed to encrypt data for item \(item.identifier)")
        }
        
        return encryptedAsset
    }
    
    public func markAsFailed(
        localIdentifier: String,
        groupId: String,
        eventOriginator: SHServerUser,
        sharedWith users: [SHServerUser]) throws
    {
        // Enquque to failed
        log.info("enqueueing upload request for asset \(localIdentifier) in the FAILED queue")
        
        let failedUploadQueueItem = SHFailedUploadRequestQueueItem(localIdentifier: localIdentifier,
                                                                   groupId: groupId,
                                                                   eventOriginator: eventOriginator,
                                                                   sharedWith: users)
        
        do { try failedUploadQueueItem.enqueue(in: FailedUploadQueue, with: localIdentifier) }
        catch {
            log.fault("asset \(localIdentifier) failed to upload but will never be recorded as failed because enqueueing to FAILED queue failed: \(error.localizedDescription)")
            throw error
        }
        
        // Dequeue from encryption queue
        log.info("dequeueing upload request for asset \(localIdentifier) from the ENCRYPT queue")
        
        do { _ = try EncryptionQueue.dequeue() }
        catch {
            log.error("asset \(localIdentifier) failed to encrypt but dequeuing from ENCRYPT queue failed, so this operation will be attempted again.")
            throw error
        }
        
#if DEBUG
        log.debug("items in the ENCRYPT queue after dequeueing \((try? EncryptionQueue.peekItems(createdWithin: DateInterval(start: .distantPast, end: Date())))?.count ?? 0)")
#endif
        
        // Notify the delegates
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
        localIdentifier: String,
        globalIdentifier: String,
        groupId: String,
        eventOriginator: SHServerUser,
        sharedWith: [SHServerUser]) throws
    {
        let uploadRequest = SHUploadRequestQueueItem(localAssetId: localIdentifier,
                                                     globalAssetId: globalIdentifier,
                                                     groupId: groupId,
                                                     eventOriginator: eventOriginator,
                                                     sharedWith: sharedWith)
        log.info("enqueueing upload request in the UPLOAD queue for asset \(localIdentifier)")
        
        do { try uploadRequest.enqueue(in: UploadQueue, with: globalIdentifier) }
        catch {
            log.fault("asset \(localIdentifier) was encrypted but will never be uploaded because enqueueing to UPLOAD queue failed")
            throw error
        }
        
        // Dequeue from EncryptionQueue
        log.info("dequeueing upload request for asset \(localIdentifier) from the ENCRYPT queue")
        
        do { _ = try EncryptionQueue.dequeue() }
        catch {
            log.warning("asset \(localIdentifier) was encrypted but dequeuing failed, so this operation will be attempted again.")
            throw error
        }
        
#if DEBUG
        log.debug("items in the ENCRYPT queue after dequeueing \((try? EncryptionQueue.peekItems(createdWithin: DateInterval(start: .distantPast, end: Date())))?.count ?? 0)")
#endif
        
        // Notify the delegates
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
    
    public override func main() {
        guard !self.isCancelled else {
            state = .finished
            return
        }
        
        state = .executing
        
        do {
            // Retrieve assets in the queue
            
            var count = 1
            
            while let item = try EncryptionQueue.peek() {
                if let limit = limit {
                    guard count < limit else {
                        break
                    }
                }
                
                log.info("encrypting and storing item \(count), with identifier \(item.identifier) created at \(item.createdAt)")
                
                guard let encryptionRequest = try? content(ofQueueItem: item) as? SHEncryptionRequestQueueItem else {
                    log.error("unexpected data found in ENCRYPT queue. Dequeueing")
                    
                    do { _ = try EncryptionQueue.dequeue() }
                    catch {
                        log.fault("dequeuing failed of unexpected data in ENCRYPT. ATTENTION: this operation will be attempted again.")
                        throw error
                    }
                    
                    throw KBError.unexpectedData(item.content)
                }
                
                let asset = encryptionRequest.asset
                
                for delegate in delegates {
                    if let delegate = delegate as? SHAssetEncrypterDelegate {
                        delegate.didStartEncryption(
                            itemWithLocalIdentifier: item.identifier,
                            groupId: encryptionRequest.groupId
                        )
                    }
                }
                
                // Generate a new symmetric key, which will be used to encrypt this asset moving forward
                // The key is only stored clear in the local database, but encrypted for each user (including self)
                // as the encryptedSecret 
                let privateSecret = SymmetricKey(size: .bits256)
                guard let encryptedAsset = try? self.generateEncryptedAsset(
                    for: asset,
                    usingPrivateSecret: privateSecret.rawRepresentation,
                    recipients: [self.user],
                    item: item,
                    request: encryptionRequest
                ) else {
                    continue
                }
                
                var error: Error? = nil
                let group = AsyncGroup()
                
                log.info("storing asset \(encryptedAsset.localIdentifier ?? encryptedAsset.globalIdentifier) versions in local server proxy")
                
                group.enter()
                serverProxy.storeAssetsLocally([encryptedAsset],
                                               senderUserIdentifier: self.user.identifier) { result in
                    if case .failure(let err) = result {
                        error = err
                    }
                    group.leave()
                }
                
                let dispatchResult = group.wait()
                guard dispatchResult != .timedOut, error == nil else {
                    log.error("failed to store data for item \(count), with identifier \(item.identifier). Dequeueing item, as it's unlikely to succeed again.")
                    
                    do {
                        try self.markAsFailed(localIdentifier: item.identifier,
                                              groupId: encryptionRequest.groupId,
                                              eventOriginator: encryptionRequest.eventOriginator,
                                              sharedWith: encryptionRequest.sharedWith)
                    } catch {
                        log.critical("failed to mark ENCRYPT as failed. This will likely cause infinite loops")
                        // TODO: Handle
                    }
                    
                    continue
                }
                
                log.info("[√] encryption task completed for item \(item.identifier)")
                
                do {
                    try self.markAsSuccessful(
                        localIdentifier: item.identifier,
                        globalIdentifier: encryptedAsset.globalIdentifier,
                        groupId: encryptionRequest.groupId,
                        eventOriginator: encryptionRequest.eventOriginator,
                        sharedWith: encryptionRequest.sharedWith
                    )
                } catch {
                    log.critical("failed to mark ENCRYPT as successful. This will likely cause infinite loops")
                    // TODO: Handle
                }
                
                count += 1
                
                guard !self.isCancelled else {
                    log.info("encrypt task cancelled. Finishing")
                    state = .finished
                    break
                }
            }
        } catch {
            log.error("error executing encrypt task: \(error.localizedDescription)")
        }
        
        state = .finished
    }
}

public class SHAssetsEncrypterQueueProcessor : SHOperationQueueProcessor<SHEncryptionOperation> {
    /// Singleton (with private initializer)
    public static var shared = SHAssetsEncrypterQueueProcessor(
        delayedStartInSeconds: 2,
        dispatchIntervalInSeconds: 2
    )
    
    private override init(delayedStartInSeconds: Int = 0,
                          dispatchIntervalInSeconds: Int? = nil) {
        super.init(delayedStartInSeconds: delayedStartInSeconds, dispatchIntervalInSeconds: dispatchIntervalInSeconds)
    }
}
