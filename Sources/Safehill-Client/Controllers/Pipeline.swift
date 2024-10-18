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
    ///   - asset: the asset to upload
    ///   - groupId: the request unique identifier
    ///   - groupTitle: the message associated with the share (the Post message)
    ///   - sender: the user sending the asset
    ///   - recipients: the recipient users
    ///   - invitedUsers: the phone numbers invited to the share with groupId
    ///   - asPhotoMessageInThreadId: whether or not the asset is being shared in the context of a thread, and if so which thread
    public static func enqueueUpload(
        of asset: SHUploadableAsset,
        groupId: String,
        groupTitle: String?,
        sender: any SHServerUser,
        recipients: [any SHServerUser],
        invitedUsers: [String],
        asPhotoMessageInThreadId: String?
    ) throws {
        
        let request = SHEncryptionRequestQueueItem(
            asset: asset,
            versions: [.lowResolution, .hiResolution],
            groupId: groupId,
            groupTitle: groupTitle,
            eventOriginator: sender,
            sharedWith: recipients,
            invitedUsers: invitedUsers,
            asPhotoMessageInThreadId: asPhotoMessageInThreadId,
            isBackground: false
        )
        
        do {
            let queue = try BackgroundOperationQueue.of(type: .encryption)
            try request.enqueue(in: queue, with: request.identifier)
        } catch {
            if let failedUploadQueue = try? BackgroundOperationQueue.of(type: .failedUpload) {
                try? request.enqueue(in: failedUploadQueue, with: request.identifier)
            }
            
            if request.isSharingWithOrInvitingOtherUsers {
                if let failedShareQueue = try? BackgroundOperationQueue.of(type: .failedShare) {
                    try? request.enqueue(in: failedShareQueue, with: request.identifier)
                }
            }
            
            throw error
        }
    }
    
    ///
    /// Enqueue a sharing operation, for an asset that was already uploaded.
    /// In case of a force-retry, the asset upload will be re-attempted too.
    /// - Parameters:
    ///   - asset: the asset to share
    ///   - groupId: the request unique identifier
    ///   - groupTitle: the message associated with the share (the Post message)
    ///   - sender: the user sending the asset
    ///   - recipients: the recipient users
    ///   - asPhotoMessageInThreadId: whether or not the asset is being shared in the context of a thread, and if so which thread
    public static func enqueueShare(
        of asset: SHUploadableAsset,
        groupId: String,
        groupTitle: String?,
        sender: SHAuthenticatedLocalUser,
        recipients: [any SHServerUser],
        invitedUsers: [String],
        asPhotoMessageInThreadId: String?
    ) throws {
        let request = SHEncryptionRequestQueueItem(
            asset: asset,
            versions: [.lowResolution, .hiResolution],
            groupId: groupId,
            groupTitle: groupTitle,
            eventOriginator: sender,
            sharedWith: recipients,
            invitedUsers: invitedUsers,
            asPhotoMessageInThreadId: asPhotoMessageInThreadId,
            isBackground: false
        )
        
        do {
            let queue = try BackgroundOperationQueue.of(type: .share)
            try request.enqueue(in: queue, with: request.identifier)
        } catch {
            if let failedShareQueue = try? BackgroundOperationQueue.of(type: .failedShare) {
                try? request.enqueue(in: failedShareQueue, with: request.identifier)
            }
            
            throw error
        }
    }
}

public extension SHUploadPipeline {
    
    static func queueItemIdentifier(
        groupId: String,
        globalIdentifier: GlobalIdentifier,
        versions: [SHAssetQuality],
        users: [any SHServerUser]
    ) -> String {
        var components = [
            globalIdentifier,
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
    
    static func assetGlobalIdentifier(fromItemIdentifier itemIdentifier: String) -> GlobalIdentifier? {
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
