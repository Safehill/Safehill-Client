import Foundation
import os
import KnowledgeBase

protocol SHUploadStepBackgroundOperation {
    var log: Logger { get }
    var serverProxy: SHServerProxy { get }
    
    func markLocalAssetAsFailed(
        globalIdentifier: GlobalIdentifier,
        versions: [SHAssetQuality],
        completionHandler: @escaping () -> Void
    )
    
    func markAsFailed(
        localIdentifier: String,
        versions: [SHAssetQuality],
        groupId: String,
        eventOriginator: SHServerUser,
        sharedWith: [SHServerUser],
        shouldLinkToThread: Bool,
        isBackground: Bool
    )
}

extension SHUploadStepBackgroundOperation {
    
    func markLocalAssetAsFailed(
        globalIdentifier: String, versions: [SHAssetQuality],
        completionHandler: @escaping () -> Void
    ) {
        let group = DispatchGroup()
        for quality in versions {
            group.enter()
            self.serverProxy.localServer.markAsset(with: globalIdentifier, quality: quality, as: .failed) { result in
                if case .failure(let err) = result {
                    if case SHAssetStoreError.noEntries = err {
                        self.log.critical("No entries found when trying to update local asset upload state for \(globalIdentifier)::\(quality.rawValue)")
                    }
                    self.log.critical("failed to mark local asset \(globalIdentifier) as failed in local server: \(err.localizedDescription)")
                }
                group.leave()
            }
        }
        
        group.notify(queue: .global()) {
            completionHandler()
        }
    }
    
    func markAsFailed(
        localIdentifier: String,
        versions: [SHAssetQuality],
        groupId: String,
        eventOriginator: SHServerUser,
        sharedWith users: [SHServerUser],
        shouldLinkToThread: Bool,
        isBackground: Bool
    ) {
        let failedUploadQueueItem = SHFailedUploadRequestQueueItem(
            localIdentifier: localIdentifier,
            versions: versions,
            groupId: groupId,
            eventOriginator: eventOriginator,
            sharedWith: users,
            shouldLinkToThread: shouldLinkToThread,
            isBackground: isBackground
        )
        
        ///
        /// Enqueue to the FAILED UPLOAD queue
        ///
        do {
            log.info("enqueueing upload request for asset \(localIdentifier) versions \(versions) to the UPLOAD FAILED queue")
            
            let failedUploadQueue = try BackgroundOperationQueue.of(type: .failedUpload)
            let successfulUploadQueue = try BackgroundOperationQueue.of(type: .successfulUpload)
            
            try failedUploadQueueItem.enqueue(in: failedUploadQueue)
            
            /// Remove items in the `UploadHistoryQueue` for the same request identifier
            let _ = try? successfulUploadQueue.removeValues(forKeysMatching: KBGenericCondition(.equal, value: failedUploadQueueItem.identifier))
        }
        catch {
            log.fault("asset \(localIdentifier) failed to upload but will never be recorded as 'failed to upload' because enqueueing to UPLOAD FAILED queue failed: \(error.localizedDescription)")
        }
        
        if users.count > 0 {
            ///
            /// Enqueue to the FAILED SHARE queue if it was an upload in service of a share
            ///
            let failedShare = SHFailedShareRequestQueueItem(
                localIdentifier: localIdentifier,
                versions: versions,
                groupId: groupId,
                eventOriginator: eventOriginator,
                sharedWith: users,
                shouldLinkToThread: shouldLinkToThread,
                isBackground: isBackground
            )
            
            do {
                let failedShareQueue = try BackgroundOperationQueue.of(type: .failedShare)
                let successfulShareQueue = try BackgroundOperationQueue.of(type: .successfulShare)
                try failedShare.enqueue(in: failedShareQueue)
                
                /// Remove items in the `ShareHistoryQueue` for the same identifier
                let _ = try? successfulShareQueue.removeValues(forKeysMatching: KBGenericCondition(.equal, value: failedShare.identifier))
            }
            catch {
                log.fault("asset \(localIdentifier) failed to encrypt, hence it couldn't be shared, but will never be recorded as 'failed to share' because enqueueing to SHARE FAILED queue failed: \(error.localizedDescription)")
            }
        }
    }
}
