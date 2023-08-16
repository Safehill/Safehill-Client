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
    
    public let user: SHLocalUser
    public var delegates: [SHOutboundAssetOperationDelegate]
    var imageManager: PHCachingImageManager
    var photoIndexer: SHPhotosIndexer?
    
    let parallelization: ParallelizationOption
    
    public init(user: SHLocalUser,
                delegates: [SHOutboundAssetOperationDelegate],
                parallelization: ParallelizationOption = .conservative,
                imageManager: PHCachingImageManager? = nil,
                photoIndexer: SHPhotosIndexer? = nil) {
        self.user = user
        self.delegates = delegates
        self.parallelization = parallelization
        self.imageManager = imageManager ?? PHCachingImageManager()
        self.photoIndexer = photoIndexer
    }
    
    public func clone() -> SHBackgroundOperationProtocol {
        SHFullUploadPipelineOperation(
            user: self.user,
            delegates: self.delegates,
            imageManager: self.imageManager,
            photoIndexer: self.photoIndexer
        )
    }
    
    public func runOnce() {
        state = .executing
        
        do { try runFetchCycle() }
        catch {
            log.critical("error running fetch cycle: \(error.localizedDescription)")
        }
        guard !isCancelled else {
            log.info("upload pipeline cancelled. Finishing")
            state = .finished
            return
        }
        
        do { try runEncryptionCycle() }
        catch {
            log.critical("error running encryption cycle: \(error.localizedDescription)")
        }
        guard !isCancelled else {
            log.info("upload pipeline cancelled. Finishing")
            state = .finished
            return
        }
        
        do { try runUploadCycle() }
        catch {
            log.critical("error running upload cycle: \(error.localizedDescription)")
        }
        guard !isCancelled else {
            log.info("upload pipeline cancelled. Finishing")
            state = .finished
            return
        }
        
        do { try runFetchCycle() }
        catch {
            log.critical("error running share+fetch cycle: \(error.localizedDescription)")
        }
        guard !isCancelled else {
            log.info("upload pipeline cancelled. Finishing")
            state = .finished
            return
        }
        
        do { try runShareCycle() }
        catch {
            log.critical("error running share cycle: \(error.localizedDescription)")
        }
        guard !isCancelled else {
            log.info("upload pipeline cancelled. Finishing")
            state = .finished
            return
        }
        
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
            delegates: delegates,
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
            user: user,
            delegates: delegates,
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
            user: user,
            delegates: delegates,
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
            user: user,
            delegates: delegates,
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
        
        self.runOnce()
        
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
