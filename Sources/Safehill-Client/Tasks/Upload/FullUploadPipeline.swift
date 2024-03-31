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
    
    public func run(
        forAssetLocalIdentifiers localIdentifiers: [String],
        groupId: String,
        sharedWith: [SHServerUser],
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
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
        
        let encryptOperation = SHEncryptionOperation(
            user: self.user,
            assetsDelegates: assetsDelegates,
            threadsDelegates: threadsDelegates,
            limitPerRun: 0,
            imageManager: imageManager
        )
        
        let uploadOperation = SHUploadOperation(
            user: self.user,
            localAssetStoreController: SHLocalAssetStoreController(user: self.user),
            delegates: assetsDelegates,
            limitPerRun: 0
        )
        
        let shareOperation = SHEncryptAndShareOperation(
            user: self.user,
            assetsDelegates: assetsDelegates,
            threadsDelegates: threadsDelegates,
            limitPerRun: 0,
            imageManager: imageManager
        )
        
        fetchOperation.run(forQueueItemIdentifiers: queueItemIdentifiers) { result in
            guard case .success = result else {
                completionHandler(result)
                return
            }
            
            encryptOperation.run(forQueueItemIdentifiers: queueItemIdentifiers) { result in
                guard case .success = result else {
                    completionHandler(result)
                    return
                }
                
                uploadOperation.run(forQueueItemIdentifiers: queueItemIdentifiers) { result in
                    guard case .success = result else {
                        completionHandler(result)
                        return
                    }
                    
                    fetchOperation.run(forQueueItemIdentifiers: queueItemIdentifiers) { result in
                        guard case .success = result else {
                            completionHandler(result)
                            return
                        }
                        
                        shareOperation.run(
                            forQueueItemIdentifiers: queueItemIdentifiers,
                            completionHandler: completionHandler
                        )
                    }
                }
            }
        }
    }
    
    public override func runOnce(
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        self.runFetchCycle { result in
            switch result {
            case .failure(let error):
                self.log.critical("error running FETCH step: \(error.localizedDescription)")
                completionHandler(.failure(error))
            case .success:
                guard !self.isCancelled else {
                    self.log.info("upload pipeline cancelled. Finishing")
                    completionHandler(.success(()))
                    return
                }
                self.runEncryptionCycle { result in
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
                        self.runUploadCycle { result in
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
                                self.runFetchCycle { result in
                                    switch result {
                                    case .failure(let error):
                                        self.log.critical("error running step FETCH': \(error.localizedDescription)")
                                        completionHandler(.failure(error))
                                    case .success:
                                        guard !self.isCancelled else {
                                            self.log.info("upload pipeline cancelled. Finishing")
                                            completionHandler(.success(()))
                                            return
                                        }
                                        self.runShareCycle(completionHandler: completionHandler)
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    private func runFetchCycle(
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
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
            limitPerRun: limit ?? 0,
            imageManager: imageManager,
            photoIndexer: photoIndexer
        )
        fetchOperation.runOnce(completionHandler: completionHandler)
    }
    
    private func runEncryptionCycle(
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
            threadsDelegates: threadsDelegates,
            limitPerRun: 0,
            imageManager: imageManager
        )
        encryptOperation.runOnce(completionHandler: completionHandler)
    }
    
    private func runUploadCycle(
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
            localAssetStoreController: SHLocalAssetStoreController(user: self.user),
            delegates: assetsDelegates,
            limitPerRun: 0
        )
        uploadOperation.runOnce(completionHandler: completionHandler)
    }
    
    private func runShareCycle(
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
            threadsDelegates: threadsDelegates,
            limitPerRun: 0,
            imageManager: imageManager
        )
        shareOperation.runOnce(completionHandler: completionHandler)
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
