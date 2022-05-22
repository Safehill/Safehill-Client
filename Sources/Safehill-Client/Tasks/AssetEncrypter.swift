import Foundation
import Safehill_Crypto
import KnowledgeBase
import Photos
import os

open class SHEncryptionOperation: SHAbstractBackgroundOperation, SHBackgroundOperationProtocol, SHBackgroundUploadOperationProtocol {
    
    public var log: Logger {
        Logger(subsystem: "com.safehill.enkey", category: "BG-ENCRYPT")
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
    
    public func content(ofQueueItem item: KBQueueItem) throws -> SHGroupableUploadQueueItem {
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
        let dispatch = KBTimedDispatch()
        var (lowResData, hiResData): (Data?, Data?) = (nil, nil)
        
        log.info("retrieving low resolution asset \(asset.phAsset.localIdentifier)")
        
        let size = CGSize(width: kSHLowResPictureSize.width, height: kSHLowResPictureSize.height)
        
        dispatch.group.enter()
        asset.phAsset.data(forSize: size,
                           usingImageManager: imageManager,
                           synchronousFetch: true) { result in
            switch result {
            case .success(let d):
                lowResData = d
                dispatch.group.leave()
            case .failure(let error):
                dispatch.interrupt(error)
            }
        }
        
        if let cachedData = asset.cachedData {
            log.info("retriving high resolution asset \(asset.phAsset.localIdentifier) from cache")
            hiResData = cachedData
        } else {
            log.info("retrieving high resolution asset \(asset.phAsset.localIdentifier)")
            
            dispatch.group.enter()
            asset.phAsset.data(usingImageManager: imageManager,
                               synchronousFetch: true) { result in
                switch result {
                case .success(let d):
                    hiResData = d
                    dispatch.group.leave()
                case .failure(let error):
                    dispatch.interrupt(error)
                }
            }
        }
        
        try dispatch.wait()

        return (lowResData!, hiResData!)
    }
    
    private func encrypt(
        asset: KBPhotoAsset,
        lowResData: Data,
        hiResData: Data,
        usingIdentifier contentIdentifier: String,
        with user: SHServerUser) throws -> SHEncryptedAsset
    {
        log.info("encrypting asset \(asset.phAsset.localIdentifier) versions to be shared with user \(user.identifier)")
        
        // encrypt this asset with a new set of ephemeral symmetric keys
        let hiResEncryptedContent = try SHEncryptedData(clearData: hiResData)
        let lowResEncryptedContent = try SHEncryptedData(clearData: lowResData)
        
        // encrypt the secret using this user's public key so that it can be stored securely on the server
        let lowResEncryptedAssetSecret = try self.user.shareable(
            data: lowResEncryptedContent.privateSecret.rawRepresentation,
            with: user
        )
        let hiResEncryptedAssetSecret = try self.user.shareable(
            data: hiResEncryptedContent.privateSecret.rawRepresentation,
            with: user
        )
        
        let lowResEncryptedVersion = SHGenericEncryptedAssetVersion(
            quality: .lowResolution,
            encryptedData: lowResEncryptedContent.encryptedData,
            encryptedSecret: lowResEncryptedAssetSecret.cyphertext,
            publicKeyData: lowResEncryptedAssetSecret.ephemeralPublicKeyData,
            publicSignatureData: lowResEncryptedAssetSecret.signature
        )
        let hiResEncryptedVersion = SHGenericEncryptedAssetVersion(
            quality: .hiResolution,
            encryptedData: hiResEncryptedContent.encryptedData,
            encryptedSecret: hiResEncryptedAssetSecret.cyphertext,
            publicKeyData: hiResEncryptedAssetSecret.ephemeralPublicKeyData,
            publicSignatureData: hiResEncryptedAssetSecret.signature
        )
        
        return SHGenericEncryptedAsset(
            globalIdentifier: contentIdentifier,
            localIdentifier: asset.phAsset.localIdentifier,
            creationDate: asset.phAsset.creationDate,
            encryptedVersions: [lowResEncryptedVersion, hiResEncryptedVersion]
        )
    }
    
    func generateEncryptedAsset(
        for asset: KBPhotoAsset,
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
                                      sharedWith: request.sharedWith)
            } catch {
                // TODO: Report
            }
            
            throw SHAssetFetchError.fatalError("failed to retrieve data for item \(item.identifier)")
        }
        
        // TODO: Update cache (in case this needs to be picked up later
//                asset.cachedData = hiResData
        
        guard let lowResData = lowResData, let hiResData = hiResData else {
            log.error("failed to retrieve data for item \(item.identifier). Dequeuing item because it's unlikely this will succeed again.")
            
            do {
                try self.markAsFailed(localIdentifier: item.identifier,
                                      groupId: request.groupId,
                                      sharedWith: request.sharedWith)
            } catch {
                // TODO: Report
            }
            
            throw SHAssetFetchError.fatalError("failed to retrieve data for item \(item.identifier)")
        }
        
        let contentIdentifier = SHHash.stringDigest(for: hiResData)
        let encryptedAsset: SHEncryptedAsset
        
        do {
            encryptedAsset = try self.encrypt(
                asset: asset,
                lowResData: lowResData,
                hiResData: hiResData,
                usingIdentifier: contentIdentifier,
                with: self.user
            )
        } catch {
            log.error("failed to encrypt data for item \(item.identifier). Dequeueing item, as it's unlikely to succeed again.")
            
            do {
                try self.markAsFailed(localIdentifier: item.identifier,
                                      groupId: request.groupId,
                                      sharedWith: request.sharedWith)
            } catch {
                // TODO: Report
            }
            
            throw SHAssetFetchError.fatalError("failed to encrypt data for item \(item.identifier)")
        }
        
        return encryptedAsset
    }
    
    public func markAsFailed(
        localIdentifier: String,
        groupId: String,
        sharedWith users: [SHServerUser]) throws
    {
        // Enquque to failed
        log.info("enqueueing upload request for asset \(localIdentifier) in the FAILED queue")
        
        let failedUploadQueueItem = SHFailedUploadRequestQueueItem(assetId: localIdentifier, groupId: groupId, sharedWith: users)
        
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
        sharedWith: [SHServerUser]) throws
    {   
        let uploadRequest = SHUploadRequestQueueItem(localAssetId: localIdentifier,
                                                     globalAssetId: globalIdentifier,
                                                     groupId: groupId,
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
                
                guard let encryptionRequest = try content(ofQueueItem: item) as? SHEncryptionRequestQueueItem else {
                    log.error("unexpected data found in ENCRYPT queue. Dequeueing")
                    
                    do { _ = try EncryptionQueue.dequeue() }
                    catch {
                        log.fault("dequeuing failed of unexpected data. ATTENTION: this operation will be attempted again.")
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
                
                guard let encryptedAsset = try? self.generateEncryptedAsset(
                    for: asset,
                    item: item,
                    request: encryptionRequest
                ) else {
                    continue
                }
                
                let dispatch = KBTimedDispatch()
                
                log.info("storing asset \(encryptedAsset.localIdentifier ?? encryptedAsset.globalIdentifier) versions in local server proxy")
                
                serverProxy.storeAssetLocally(encryptedAsset) { result in
                    switch result {
                    case .success(_):
                        dispatch.semaphore.signal()
                    case .failure(let error):
                        dispatch.interrupt(error)
                    }
                }
                
                do {
                    try dispatch.wait()
                } catch {
                    log.error("failed to store data for item \(count), with identifier \(item.identifier). Dequeueing item, as it's unlikely to succeed again.")
                    
                    do {
                        try self.markAsFailed(localIdentifier: item.identifier,
                                              groupId: encryptionRequest.groupId,
                                              sharedWith: encryptionRequest.sharedWith)
                    } catch {
                        // TODO: Report
                    }
                    
                    continue
                }
                
                log.info("[√] encryption task completed for item \(item.identifier)")
                
                do {
                    try self.markAsSuccessful(
                        localIdentifier: item.identifier,
                        globalIdentifier: encryptedAsset.globalIdentifier,
                        groupId: encryptionRequest.groupId,
                        sharedWith: encryptionRequest.sharedWith
                    )
                } catch {
                    // TODO: Report
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
        super.init(dispatchIntervalInSeconds: dispatchIntervalInSeconds)
    }
}
