import Foundation
import KnowledgeBase
import os


extension SHServerProxy {
    
    private func syncDescriptors(completionHandler: @escaping (Swift.Result<AssetDescriptorsDiff, Error>) -> ()) {
        var localDescriptors = [any SHAssetDescriptor](), remoteDescriptors = [any SHAssetDescriptor]()
        var localError: Error? = nil, remoteError: Error? = nil
        
        let group = DispatchGroup()
        group.enter()
        self.getLocalAssetDescriptors { localResult in
            switch localResult {
            case .success(let descriptors):
                localDescriptors = descriptors
            case .failure(let err):
                localError = err
            }
            group.leave()
        }
        
        group.enter()
        self.getRemoteAssetDescriptors { remoteResult in
            switch remoteResult {
            case .success(let descriptors):
                remoteDescriptors = descriptors
            case .failure(let err):
                remoteError = err
            }
            group.leave()
        }
        
        let dispatcResult = group.wait(timeout: .now() + .milliseconds(SHDefaultNetworkTimeoutInMilliseconds))
        guard dispatcResult == .success else {
            completionHandler(.failure(SHBackgroundOperationError.timedOut))
            return
        }
        guard localError == nil, remoteError == nil else {
            completionHandler(.failure(localError ?? remoteError!))
            return
        }
        
        ///
        /// Handle the following cases:
        /// 1. The asset has been encrypted but not yet downloaded (so the server doesn't know about that asset yet)
        ///     -> needs to be kept as the user encryption secret - necessary for uploading and sharing - is stored there
        /// 2. The descriptor exists on the server but not locally
        ///     -> It will be created locally, created in the ShareHistory or UploadHistory queue item by any DownloadOperation. Nothing to do here.
        /// 3. The descriptor exists locally but not on the server
        ///     -> remove it as long as it's not case 1
        /// 4. The local upload state doesn't match the remote state
        ///     -> inefficient solution is to verify the asset is in S3. Efficient is to trust the value from the server
        ///
        let diff = AssetDescriptorsDiff.generateUsing(server: remoteDescriptors,
                                                      local: localDescriptors,
                                                      for: self.localServer.requestor)
        
        if diff.assetsRemovedOnServer.count > 0 {
            let globalIdentifiers = diff.assetsRemovedOnServer.compactMap { $0.globalIdentifier }
            self.localServer.deleteAssets(withGlobalIdentifiers: globalIdentifiers) { result in
                if case .failure(let error) = result {
                    log.error("some assets were deleted on server but couldn't be deleted from local cache. This operation will be attempted again, but for now the cache is out of sync. error=\(error.localizedDescription)")
                }
            }
        }
        
        for stateChangeDiff in diff.stateDifferentOnServer {
            self.localServer.markAsset(with: stateChangeDiff.globalIdentifier,
                                       quality: stateChangeDiff.quality,
                                       as: stateChangeDiff.newUploadState) { result in
                if case .failure(let error) = result {
                    log.error("some assets were marked as uploaded on server but couldn't be deleted from lcoal cache. THis operation will be attempted again, but for now the cache is out of sync. error=\(error.localizedDescription)")
                }
            }
        }
        
        completionHandler(.success(diff))
    }
    
    public func sync(delegate: SHAssetSyncingDelegate?) {
        self.syncDescriptors { result in
            switch result {
            case .success(let diff):
                if diff.assetsRemovedOnServer.count > 0 {
                    // TODO: The deletion from the queues defined in the framework is taken care of by the `AssetUploadController` which is a client of the framework. Consider moving `AssetUploadController` and the sister controllers to the framework
                    delegate?.handleDeletion(of: diff.assetsRemovedOnServer)
                }
                if diff.stateDifferentOnServer.count > 0 {
                    // TODO: Do we need to mark things as failed/pending depending on state?
                }
            case .failure(let err):
                log.error("failed to update local descriptors from server descriptors: \(err.localizedDescription)")
            }
        }
    }
}

// MARK: - Sync Operation

public class SHSyncOperation: SHAbstractBackgroundOperation, SHBackgroundOperationProtocol {
    
    public let log = Logger(subsystem: "com.safehill", category: "BG-SYNC")
    
    let user: SHLocalUser
    
    let delegate: SHAssetSyncingDelegate?
    
    public var serverProxy: SHServerProxy {
        SHServerProxy(user: self.user)
    }
    
    public init(user: SHLocalUser, delegate: SHAssetSyncingDelegate?) {
        self.user = user
        self.delegate = delegate
    }
    
    public func clone() -> SHBackgroundOperationProtocol {
        SHSyncOperation(user: self.user, delegate: self.delegate)
    }
    
    public override func main() {
        guard !self.isCancelled else {
            state = .finished
            return
        }
        
        state = .executing
        
        self.serverProxy.sync(delegate: delegate)
        
        self.state = .finished
    }
}


public class SHSyncProcessor : SHBackgroundOperationProcessor<SHSyncOperation> {
    
    public static var shared = SHSyncProcessor(
        delayedStartInSeconds: 6,
        dispatchIntervalInSeconds: 15
    )
    private override init(delayedStartInSeconds: Int,
                          dispatchIntervalInSeconds: Int? = nil) {
        super.init(delayedStartInSeconds: delayedStartInSeconds,
                   dispatchIntervalInSeconds: dispatchIntervalInSeconds)
    }
}