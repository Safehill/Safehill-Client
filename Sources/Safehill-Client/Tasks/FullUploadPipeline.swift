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
    
    let parallelization: ParallelizationOption
    
    public init(user: SHLocalUser,
                delegates: [SHOutboundAssetOperationDelegate],
                parallelization: ParallelizationOption = .conservative,
                imageManager: PHCachingImageManager? = nil) {
        self.user = user
        self.delegates = delegates
        self.parallelization = parallelization
        self.imageManager = imageManager ?? PHCachingImageManager()
    }
    
    public func clone() -> SHBackgroundOperationProtocol {
        SHFullUploadPipelineOperation(
            user: self.user,
            delegates: self.delegates,
            imageManager: self.imageManager
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
        var maxFetches: Int = 0 // no limit (parallelization == .aggressive)
        
        if parallelization == .conservative {
            let maxEncryptions = 7
            let ongoingEncryptions = try BackgroundOperationQueue.of(type: .encryption).keys().count
            maxFetches = maxEncryptions - ongoingEncryptions
            guard maxFetches > 0 else {
                return
            }
        }
        
        let fetchOperation = SHLocalFetchOperation(
            delegates: delegates,
            limitPerRun: maxFetches,
            imageManager: imageManager
        )
        try fetchOperation.runOnce()
    }
    
    private func runEncryptionCycle() throws {
        var maxEncryptions: Int = 0 // no limit (parallelization == .aggressive)
        
        if parallelization == .conservative {
            let maxUploads = 5
            let ongoingUploads = try BackgroundOperationQueue.of(type: .upload).keys().count
            maxEncryptions = maxUploads - ongoingUploads
            guard maxEncryptions > 0 else {
                return
            }
        }
        
        let encryptOperation = SHEncryptionOperation(
            user: user,
            delegates: delegates,
            limitPerRun: maxEncryptions,
            imageManager: imageManager
        )
        try encryptOperation.runOnce()
    }
    
    private func runUploadCycle() throws {
        var limit: Int = 0 // no limit (parallelization == .aggressive)
        
        if parallelization == .conservative {
            let maxUploads = 5
            let ongoingUploads = try BackgroundOperationQueue.of(type: .upload).keys().count
            limit = maxUploads - ongoingUploads
            guard limit > 0 else {
                return
            }
        }
        
        let uploadOperation = SHUploadOperation(
            user: user,
            delegates: delegates,
            limitPerRun: limit
        )
        try uploadOperation.runOnce()
    }
    
    private func runShareCycle() throws {
        var limit: Int = 0 // no limit (parallelization == .aggressive)
        
        if parallelization == .conservative {
            let maxShares = 10
            let ongoingShares = try BackgroundOperationQueue.of(type: .share).keys().count
            limit = maxShares - ongoingShares
            guard limit > 0 else {
                return
            }
        }
        
        let shareOperation = SHEncryptAndShareOperation(
            user: user,
            delegates: delegates,
            limitPerRun: limit,
            imageManager: imageManager
        )
        try shareOperation.runOnce()
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
