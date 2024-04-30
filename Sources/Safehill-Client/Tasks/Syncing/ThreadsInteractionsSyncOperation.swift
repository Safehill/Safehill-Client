import Foundation
import os

/// 
/// Responsible for syncing:
/// - full list of threads with server
/// - LAST `ThreadLastInteractionSyncLimit` interactions in each
///
public class SHThreadsInteractionsSyncOperation: Operation, SHBackgroundOperationProtocol {
    
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
    
    public func run(qos: DispatchQoS.QoSClass,
                    completionHandler: @escaping (Result<Void, Error>) -> Void) {
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
}

let ThreadsSyncProcessor = SHBackgroundOperationProcessor<SHThreadsInteractionsSyncOperation>()
