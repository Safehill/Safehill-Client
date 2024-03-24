import Foundation
import os

/// 
/// Responsible for syncing:
/// - full list of threads with server
/// - LAST 20 interactions in each
///
public class SHThreadsInteractionsSyncOperation: SHAbstractBackgroundOperation, SHBackgroundOperationProtocol {
    
    public let log = Logger(subsystem: "com.safehill", category: "BG-INTERACTIONS-SYNC")
    
    let delegatesQueue = DispatchQueue(label: "com.safehill.threads-interactions-sync.delegates")
    
    let user: SHLocalUserProtocol
    
    let assetsSyncDelegates: [SHAssetSyncingDelegate]
    let threadsSyncDelegates: [SHThreadSyncingDelegate]
    
    public init(user: SHLocalUserProtocol,
                assetsSyncDelegates: [SHAssetSyncingDelegate],
                threadsSyncDelegates: [SHThreadSyncingDelegate]) {
        self.user = user
        self.assetsSyncDelegates = assetsSyncDelegates
        self.threadsSyncDelegates = threadsSyncDelegates
    }
    
    var serverProxy: SHServerProxy { self.user.serverProxy }
    
    public func clone() -> SHBackgroundOperationProtocol {
        SHThreadsInteractionsSyncOperation(
            user: self.user,
            assetsSyncDelegates: self.assetsSyncDelegates,
            threadsSyncDelegates: self.threadsSyncDelegates
        )
    }
    
    public func runOnce(
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        
        guard self.user is SHAuthenticatedLocalUser else {
            completionHandler(.failure(SHLocalUserError.notAuthenticated))
            return
        }
        
        let syncOperation = SHSyncOperation(
            user: self.user as! SHAuthenticatedLocalUser,
            assetsDelegates: self.assetsSyncDelegates,
            threadsDelegates: self.threadsSyncDelegates
        )
        syncOperation.syncLastThreadInteractions(qos: qos, completionHandler: completionHandler)
    }
    
    public override func main() {
        guard !self.isCancelled else {
            state = .finished
            return
        }
        
        state = .executing
        
        self.runOnce(qos: .background) { _ in
            self.state = .finished
        }
    }
}

// MARK: - Threads Interactions Sync Operation Processor

public class SHThreadsInteractionsSyncProcessor : SHBackgroundOperationProcessor<SHThreadsInteractionsSyncOperation> {
    
    public static var shared = SHThreadsInteractionsSyncProcessor(
        delayedStartInSeconds: 0,
        dispatchIntervalInSeconds: 2
    )
    private override init(delayedStartInSeconds: Int,
                          dispatchIntervalInSeconds: Int? = nil) {
        super.init(delayedStartInSeconds: delayedStartInSeconds, dispatchIntervalInSeconds: dispatchIntervalInSeconds)
    }
}
