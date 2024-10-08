import Foundation
import KnowledgeBase
import Photos
import os

open class SHFullUploadPipelineOperation: Operation, SHBackgroundOperationProtocol, @unchecked Sendable {
    
    public enum ParallelizationOption {
        case aggressive, conservative
    }
    
    public var log: Logger {
        Logger(subsystem: "com.gf.safehill", category: "BG")
    }
    
    public let user: SHAuthenticatedLocalUser
    public var assetsDelegates: [SHOutboundAssetOperationDelegate]
    var photoIndexer: SHPhotosIndexer
    
    let parallelization: ParallelizationOption
    
    public init(user: SHAuthenticatedLocalUser,
                assetsDelegates: [SHOutboundAssetOperationDelegate],
                parallelization: ParallelizationOption = .conservative,
                photoIndexer: SHPhotosIndexer) {
        self.user = user
        self.assetsDelegates = assetsDelegates
        self.parallelization = parallelization
        self.photoIndexer = photoIndexer
    }
    
    public func run(
        forAssetLocalIdentifiers localIdentifiers: [String],
        groupId: String,
        sharedWith: [SHServerUser],
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        let queueItemIdentifiers = localIdentifiers.flatMap({
            [
                SHUploadPipeline.queueItemIdentifier(
                    groupId: groupId,
                    assetLocalIdentifier: $0,
                    versions: [.lowResolution, .midResolution],
                    users: sharedWith
                ),
                SHUploadPipeline.queueItemIdentifier(
                    groupId: groupId,
                    assetLocalIdentifier: $0,
                    versions: [.lowResolution, .hiResolution],
                    users: sharedWith
                )
            ]
        })
        
        let encryptOperation = SHEncryptionOperation(
            user: self.user,
            assetsDelegates: assetsDelegates,
            limitPerRun: 0
        )
        
        let uploadOperation = SHUploadOperation(
            user: self.user,
            assetsDelegates: assetsDelegates,
            limitPerRun: 0
        )
        
        let shareOperation = SHEncryptAndShareOperation(
            user: self.user,
            assetsDelegates: assetsDelegates,
            limitPerRun: 0
        )
        
        let log = self.log
        
        log.debug("Running on-demand ENCRYPT for items \(queueItemIdentifiers)")
        encryptOperation.run(forQueueItemIdentifiers: queueItemIdentifiers, qos: qos) { result in
            guard case .success = result else {
                completionHandler(result)
                return
            }
            
            log.debug("Running on-demand UPLOAD for items \(queueItemIdentifiers)")
            uploadOperation.run(forQueueItemIdentifiers: queueItemIdentifiers, qos: qos) { result in
                guard case .success = result else {
                    completionHandler(result)
                    return
                }
                
                log.debug("Running on-demand SHARE for items \(queueItemIdentifiers)")
                shareOperation.run(
                    forQueueItemIdentifiers: queueItemIdentifiers,
                    qos: qos,
                    completionHandler: completionHandler
                )
            }
        }
    }
    
    public func run(
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        self.runEncryptionCycle(qos: qos) { result in
            switch result {
            case .failure(let error):
                self.log.critical("error running ENCRYPT step: \(error.localizedDescription)")
                completionHandler(.failure(error))
            case .success:
                guard !self.isCancelled else {
                    self.log.info("upload pipeline cancelled. Finishing")
                    completionHandler(.success(()))
                    return
                }
                self.runUploadCycle(qos: qos) { result in
                    switch result {
                    case .failure(let error):
                        self.log.critical("error running UPLOAD step: \(error.localizedDescription)")
                        completionHandler(.failure(error))
                    case .success:
                        guard !self.isCancelled else {
                            self.log.info("upload pipeline cancelled. Finishing")
                            completionHandler(.success(()))
                            return
                        }
                        self.runShareCycle(qos: qos, completionHandler: completionHandler)
                    }
                }
            }
        }
    }
    
    private func runEncryptionCycle(
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
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
            limitPerRun: 0
        )
        log.debug("Running ENCRYPT step")
        encryptOperation.run(qos: qos, completionHandler: completionHandler)
    }
    
    private func runUploadCycle(
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
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
            assetsDelegates: assetsDelegates,
            limitPerRun: 0
        )
        log.debug("Running UPLOAD step")
        uploadOperation.run(qos: qos, completionHandler: completionHandler)
    }
    
    private func runShareCycle(
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
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
            limitPerRun: 0
        )
        log.debug("Running SHARE step")
        shareOperation.run(qos: qos, completionHandler: completionHandler)
    }
}

public let FullUploadPipelineProcessor = SHBackgroundOperationProcessor<SHFullUploadPipelineOperation>()
