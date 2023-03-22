import Foundation
import KnowledgeBase
import Photos
import os

open class SHFullUploadPipelineOperation: SHAbstractBackgroundOperation, SHBackgroundOperationProtocol {
    
    public var log: Logger {
        Logger(subsystem: "com.gf.safehill", category: "BG")
    }
    
    public let user: SHLocalUser
    public var delegates: [SHOutboundAssetOperationDelegate]
    var imageManager: PHCachingImageManager
    
    public init(user: SHLocalUser,
                delegates: [SHOutboundAssetOperationDelegate],
                imageManager: PHCachingImageManager? = nil) {
        self.user = user
        self.delegates = delegates
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
        let fetchOperation = SHLocalFetchOperation(
            delegates: delegates,
            limitPerRun: 0,
            imageManager: imageManager
        )
        try fetchOperation.runOnce()
    }
    
    private func runEncryptionCycle() throws {
        let encryptOperation = SHEncryptionOperation(
            user: user,
            delegates: delegates,
            limitPerRun: 0,
            imageManager: imageManager
        )
        try encryptOperation.runOnce()
    }
    
    private func runUploadCycle() throws {
        let uploadOperation = SHUploadOperation(
            user: user,
            delegates: delegates,
            limitPerRun: 0
        )
        try uploadOperation.runOnce()
    }
    
    private func runShareCycle() throws {
        let shareOperation = SHEncryptAndShareOperation(
            user: user,
            delegates: delegates,
            limitPerRun: 0,
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
        dispatchIntervalInSeconds: 5
    )
    
    private override init(delayedStartInSeconds: Int = 0,
                          dispatchIntervalInSeconds: Int? = nil) {
        super.init(delayedStartInSeconds: delayedStartInSeconds, dispatchIntervalInSeconds: dispatchIntervalInSeconds)
    }
}
