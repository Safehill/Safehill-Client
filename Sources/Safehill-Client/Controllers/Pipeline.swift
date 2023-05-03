import Foundation
import Safehill_Crypto

///
/// For Sharing requests (where `sharedWith` is not empty):
/// - upload `.lowResolution` and `.midResolution` only. Once successful, repeat the process for `.hiResolution`, starting from a `SHLocalFetchOperation` object
/// For Upload-only request (where `sharedWith` is empty):
/// - upload `.lowResolution` and `.hiResolution` in one turn
///
/// The motivation is that for sharing requests are usually there's an expectation for a good-quality asset (`.midResolution`)
/// to be available on the other side relatively quickly.
/// For upload-only (backup) requests, there's no need to go through the trouble of uploading `.midResolution` before `.hiResolution`.
///
public struct SHUploadPipeline {
    
    ///
    /// Enqueue an upload operation, and if recipients is not empty, share it with the reciepients, too.
    /// If the assets should be shared, the `.hiResolution` version of the asset is proxied through
    /// a `.midResolution`, which is faster to upload and to receive on the other end.
    /// In this case, once the `.lowResolution` is uploaded (synchronously), a `.midResolution` version
    /// is uploaded (asynchronously). While the `.midResolution` is uploading, the `.hiResolution` upload
    /// background pipeline is triggered. Because the `midResolution` version is treated as a `.hiResolution`,
    /// when it finishes uploading, the whole asset is marked as uploaded.
    /// Once, the `.hiResolution` finishes uploading, it replaces the `.midResolution`.
    ///
    /// - Parameters:
    ///   - localIdentifier: the Apple Photos local identifier
    ///   - groupId: the request unique identifier
    ///   - sender: the user sending the asset
    ///   - recipients: the recipient users
    public static func enqueueUpload(
        localIdentifier: String,
        groupId: String,
        sender: SHServerUser,
        recipients: [SHServerUser]
    ) throws {
        do {
            let queueItem = SHLocalFetchRequestQueueItem(
                localIdentifier: localIdentifier,
                groupId: groupId,
                eventOriginator: sender,
                sharedWith: recipients,
                shouldUpload: true
            )
            try queueItem.enqueue(in: FetchQueue)
        } catch {
            let failedQueueItem = SHFailedUploadRequestQueueItem(
                localIdentifier: localIdentifier,
                groupId: groupId,
                eventOriginator: sender,
                sharedWith: recipients
            )
            try? failedQueueItem.enqueue(in: FailedUploadQueue)
            
            if recipients.count > 0 {
                let failedQueueItem = SHFailedShareRequestQueueItem(
                    localIdentifier: localIdentifier,
                    groupId: groupId,
                    eventOriginator: sender,
                    sharedWith: recipients
                )
                try? failedQueueItem.enqueue(in: FailedShareQueue)
            }
            
            throw error
        }
    }
    
    ///
    /// Enqueue a sharing operation, for an asset that was already uploaded.
    /// In case of a force-retry, the asset upload will be re-attempted too.
    /// - Parameters:
    ///   - localIdentifier: the Apple Photos local identifier
    ///   - groupId: the request unique identifier
    ///   - sender: the user sending the asset
    ///   - recipients: the recipient users
    ///   - forceUpload: whether or not it's a force-retry (and should re-upload)
    public static func enqueueShare(
        localIdentifier: String,
        groupId: String,
        sender: SHLocalUser,
        recipients: [SHServerUser],
        forceUpload: Bool
    ) throws {
        ///
        /// First, check if the asset already exists in the local store.
        /// If the hi resolution exists, share the `.hiResolution` directly,
        /// without proxying through the `.midResolution`
        ///
        let locallyEncryptedVersions = SHLocalAssetStoreController(
            user: sender
        ).locallyEncryptedVersions(
            forLocalIdentifier: localIdentifier
        )
        
        let versions: [SHAssetQuality]
        if locallyEncryptedVersions.contains(.hiResolution) {
            versions = [.lowResolution, .hiResolution]
        } else {
            versions = [.lowResolution, .midResolution]
        }
        
        do {
            let queueItem = SHLocalFetchRequestQueueItem(
                localIdentifier: localIdentifier,
                versions: versions,
                groupId: groupId,
                eventOriginator: sender,
                sharedWith: recipients,
                shouldUpload: forceUpload
            )
            try queueItem.enqueue(in: FetchQueue)
        } catch {
            let failedQueueItem = SHFailedShareRequestQueueItem(
                localIdentifier: localIdentifier,
                versions: versions,
                groupId: groupId,
                eventOriginator: sender,
                sharedWith: recipients
            )
            try? failedQueueItem.enqueue(in: FailedShareQueue)
            
            throw error
        }
    }
}

public extension SHUploadPipeline {
    
    static func queueItemIdentifier(groupId: String, assetLocalIdentifier: String, versions: [SHAssetQuality], users: [SHServerUser]) -> String {
        var components = [
            assetLocalIdentifier,
            groupId
        ]
        if users.count > 0 {
            components.append(SHHash.stringDigest(for: users
                .map({ $0.identifier })
                .sorted()
                .joined(separator: "+").data(using: .utf8)!
            ))
        }
        if versions.count > 0 {
            components.append(versions.map { $0.rawValue }.joined(separator: ":"))
        }
        return components.joined(separator: "+")
    }
    
    static func assetLocalIdentifier(fromItemIdentifier itemIdentifier: String) -> String? {
        let components = itemIdentifier.components(separatedBy: "+")
        guard components.count >= 3 else {
            return nil
        }
        return components[0]
    }
    
    static func groupIdentifier(fromItemIdentifier itemIdentifier: String) -> String? {
        let components = itemIdentifier.components(separatedBy: "+")
        guard components.count >= 3 else {
            return nil
        }
        return components[1]
    }
}
