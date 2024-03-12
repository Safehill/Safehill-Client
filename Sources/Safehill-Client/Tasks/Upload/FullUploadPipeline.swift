import Foundation
import KnowledgeBase
import Photos
import os

open class SHFullUploadPipelineOperation: SHAbstractBackgroundOperation, SHBackgroundOperationProtocol {
    
    public enum ParallelizationOption {
        case aggressive, conservative
    }
    
    public var log: Logger {
        Logger(subsystem: "com.gf.safehill", category: "BG")
    }
    
    public let user: SHAuthenticatedLocalUser
    public var assetsDelegates: [SHOutboundAssetOperationDelegate]
    public var threadsDelegates: [SHThreadSyncingDelegate]
    var imageManager: PHCachingImageManager
    var photoIndexer: SHPhotosIndexer?
    
    let parallelization: ParallelizationOption
    
    public init(user: SHAuthenticatedLocalUser,
                assetsDelegates: [SHOutboundAssetOperationDelegate],
                threadsDelegates: [SHThreadSyncingDelegate],
                parallelization: ParallelizationOption = .conservative,
                imageManager: PHCachingImageManager? = nil,
                photoIndexer: SHPhotosIndexer? = nil) {
        self.user = user
        self.assetsDelegates = assetsDelegates
        self.threadsDelegates = threadsDelegates
        self.parallelization = parallelization
        self.imageManager = imageManager ?? PHCachingImageManager()
        self.photoIndexer = photoIndexer
    }
    
    public func clone() -> SHBackgroundOperationProtocol {
        SHFullUploadPipelineOperation(
            user: self.user,
            assetsDelegates: self.assetsDelegates,
            threadsDelegates: self.threadsDelegates,
            imageManager: self.imageManager,
            photoIndexer: self.photoIndexer
        )
    }
    
    public func run(forAssetLocalIdentifiers localIdentifiers: [String],
                    groupId: String,
                    sharedWith: [SHServerUser]) throws {
        let versions = SHAbstractShareableGroupableQueueItem.recommendedVersions(forSharingWith: sharedWith)
        let queueItemIdentifiers = localIdentifiers.map({
            SHUploadPipeline.queueItemIdentifier(groupId: groupId,
                                                 assetLocalIdentifier: $0,
                                                 versions: versions,
                                                 users: sharedWith)
        })
        
        
        let fetchOperation = SHLocalFetchOperation(
            delegates: assetsDelegates,
            limitPerRun: 0,
            imageManager: imageManager,
            photoIndexer: photoIndexer
        )
        try fetchOperation.run(forQueueItemIdentifiers: queueItemIdentifiers)
        
        let encryptOperation = SHEncryptionOperation(
            user: self.user,
            assetsDelegates: assetsDelegates,
            threadsDelegates: threadsDelegates,
            limitPerRun: 0,
            imageManager: imageManager
        )
        try encryptOperation.run(forQueueItemIdentifiers: queueItemIdentifiers)
        
        let uploadOperation = SHUploadOperation(
            user: self.user,
            localAssetStoreController: SHLocalAssetStoreController(user: self.user),
            delegates: assetsDelegates,
            limitPerRun: 0
        )
        try uploadOperation.run(forQueueItemIdentifiers: queueItemIdentifiers)
        
        try fetchOperation.run(forQueueItemIdentifiers: queueItemIdentifiers)
        
        let shareOperation = SHEncryptAndShareOperation(
            user: self.user,
            assetsDelegates: assetsDelegates,
            threadsDelegates: threadsDelegates,
            limitPerRun: 0,
            imageManager: imageManager
        )
        try shareOperation.run(forQueueItemIdentifiers: queueItemIdentifiers)
    }
    
    public func runOnce() throws {
        
        state = .executing
        
        let step: ((() throws -> Void, String) -> Void) = { throwingMethod, methodIdentifier in
            do { try throwingMethod() }
            catch {
                self.log.critical("error running step '\(methodIdentifier)': \(error.localizedDescription)")
            }
            guard !self.isCancelled else {
                self.log.info("upload pipeline cancelled. Finishing")
                self.state = .finished
                return
            }
        }
        
        step(runFetchCycle, "FetchForEncryptionUpload")
        step(runEncryptionCycle, "Encryption")
        step(runUploadCycle, "Upload")
        step(runFetchCycle, "FetchForEncryptionShare")
        step(runShareCycle, "Share")
        
        state = .finished
    }
    
    private func runFetchCycle() throws {
        var limit: Int? = nil // no limit (parallelization == .aggressive)
        
        if parallelization == .conservative {
            let maxFetches = 7
            let ongoingFetches = items(inState: .fetching)?.count ?? 0
            limit = maxFetches - ongoingFetches
            guard limit! > 0 else {
                return
            }
        }
        
        let fetchOperation = SHLocalFetchOperation(
            delegates: assetsDelegates,
            limitPerRun: 0,
            imageManager: imageManager,
            photoIndexer: photoIndexer
        )
        try fetchOperation.runOnce(maxItems: limit)
    }
    
    private func runEncryptionCycle() throws {
        var limit: Int? = nil // no limit (parallelization == .aggressive)
        
        if parallelization == .conservative {
            let maxEncryptions = 5
            let ongoingEncryptions = items(inState: .encrypting)?.count ?? 0
            limit = maxEncryptions - ongoingEncryptions
            guard limit! > 0 else {
                return
            }
        }
        
        let encryptOperation = SHEncryptionOperation(
            user: self.user,
            assetsDelegates: assetsDelegates,
            threadsDelegates: threadsDelegates,
            limitPerRun: 0,
            imageManager: imageManager
        )
        try encryptOperation.runOnce(maxItems: limit)
    }
    
    private func runUploadCycle() throws {
        var limit: Int? = nil // no limit (parallelization == .aggressive)
        
        if parallelization == .conservative {
            let maxUploads = 5
            let ongoingUploads = items(inState: .uploading)?.count ?? 0
            limit = maxUploads - ongoingUploads
            guard limit! > 0 else {
                return
            }
        }
        
        let uploadOperation = SHUploadOperation(
            user: self.user,
            localAssetStoreController: SHLocalAssetStoreController(user: self.user),
            delegates: assetsDelegates,
            limitPerRun: 0
        )
        try uploadOperation.runOnce(maxItems: limit)
    }
    
    private func runShareCycle() throws {
        var limit: Int? = nil // no limit (parallelization == .aggressive)
        
        if parallelization == .conservative {
            let maxShares = 10
            let ongoingShares = items(inState: .sharing)?.count ?? 0
            limit = maxShares - ongoingShares
            guard limit! > 0 else {
                return
            }
        }
        
        let shareOperation = SHEncryptAndShareOperation(
            user: self.user,
            assetsDelegates: assetsDelegates,
            threadsDelegates: threadsDelegates,
            limitPerRun: 0,
            imageManager: imageManager
        )
        try shareOperation.runOnce(maxItems: limit)
    }
    
    public override func main() {
        guard !self.isCancelled else {
            state = .finished
            return
        }
        
        state = .executing
        
        do {
            try self.runOnce()
        } catch {
            log.error("failed to run full upload pipeline. \(error.localizedDescription)")
        }
        
        state = .finished
    }
}

public class SHFullUploadPipelineProcessor : SHBackgroundOperationProcessor<SHFullUploadPipelineOperation> {
    /// Singleton (with private initializer)
    public static var shared = SHFullUploadPipelineProcessor(
        delayedStartInSeconds: 1,
        dispatchIntervalInSeconds: 7
    )
    
    private override init(delayedStartInSeconds: Int = 0,
                          dispatchIntervalInSeconds: Int? = nil) {
        super.init(delayedStartInSeconds: delayedStartInSeconds, dispatchIntervalInSeconds: dispatchIntervalInSeconds)
    }
}
