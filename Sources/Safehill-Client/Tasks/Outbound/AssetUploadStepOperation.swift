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
        eventOriginator: any SHServerUser,
        sharedWith: [any SHServerUser],
        invitedUsers: [String],
        isPhotoMessage: Bool,
        isBackground: Bool,
        error: Error
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
        eventOriginator: any SHServerUser,
        sharedWith users: [any SHServerUser],
        invitedUsers: [String],
        isPhotoMessage: Bool,
        isBackground: Bool,
        error: Error
    ) {
        let failedUploadQueueItem = SHFailedUploadRequestQueueItem(
            localIdentifier: localIdentifier,
            versions: versions,
            groupId: groupId,
            eventOriginator: eventOriginator,
            sharedWith: users,
            invitedUsers: invitedUsers,
            isPhotoMessage: isPhotoMessage,
            isBackground: isBackground
        )
        
        ///
        /// Enqueue to the FAILED UPLOAD queue
        ///
        do {
            log.info("enqueueing upload request for asset \(localIdentifier) versions \(versions) to the UPLOAD FAILED queue")
            
            let failedUploadQueue = try BackgroundOperationQueue.of(type: .failedUpload)
            try failedUploadQueueItem.enqueue(in: failedUploadQueue)
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
                invitedUsers: invitedUsers,
                isPhotoMessage: isPhotoMessage,
                isBackground: isBackground
            )
            
            do {
                let failedShareQueue = try BackgroundOperationQueue.of(type: .failedShare)
                try failedShare.enqueue(in: failedShareQueue)
            }
            catch {
                log.fault("asset \(localIdentifier) failed to encrypt, hence it couldn't be shared, but will never be recorded as 'failed to share' because enqueueing to SHARE FAILED queue failed: \(error.localizedDescription)")
            }
        }
    }
}
