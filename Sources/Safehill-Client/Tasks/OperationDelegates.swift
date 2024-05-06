import Foundation
import Photos

/// Inbound operation delegate.
public protocol SHInboundAssetOperationDelegate {}

public protocol SHAssetSyncingDelegate: SHInboundAssetOperationDelegate {
    func assetIdsAreVisibleToUsers(_: [GlobalIdentifier: [SHServerUser]])
    
    func assetsWereDeleted(_ assets: [SHRemoteAssetIdentifier])
    func usersWereAddedToShare(
        of: GlobalIdentifier,
        groupIdByRecipientId: [UserIdentifier: String],
        groupInfoById: [String: SHAssetGroupInfo]
    )
    func usersWereRemovedFromShare(
        of: GlobalIdentifier,
        groupIdByRecipientId: [UserIdentifier: String]
    )
}

public protocol SHThreadSyncingDelegate: SHInboundAssetOperationDelegate {
    func didUpdateThreadsList(_: [ConversationThreadOutputDTO])
    
    func didReceiveMessagesFromUnauthorized(users: [any SHServerUser])
    
    func reactionsDidChange(inGroup groupId: String)
    func didReceiveMessages(_ messages: [MessageOutputDTO], 
                            inGroup groupId: String,
                            encryptionDetails: EncryptionDetailsClass)
    
    func reactionsDidChange(inThread threadId: String)
    func didReceiveMessages(_ messages: [MessageOutputDTO], 
                            inThread threadId: String,
                            encryptionDetails: EncryptionDetailsClass)
}

public protocol SHAssetDownloaderDelegate: SHInboundAssetOperationDelegate {
    
    /// The list of asset descriptors fetched from the server, filtering out what's already available locally (based on the 2 methods above)
    /// - Parameter descriptors: the descriptors
    /// - Parameter users: the `SHServerUser` objects for user ids mentioned in the descriptors
    /// - Parameter completionHandler: called when handling is complete
    func didReceiveAssetDescriptors(_ descriptors: [any SHAssetDescriptor],
                                    referencing users: [UserIdentifier: any SHServerUser])
    
    /// Notifies there are assets to download from unknown users
    /// - Parameters:
    ///   - descriptors: the descriptors to authorize
    ///   - users: the `SHServerUser` objects for user ids mentioned in the descriptors
    func didReceiveAuthorizationRequest(for descriptors: [any SHAssetDescriptor],
                                        referencing users: [UserIdentifier: any SHServerUser])
    
    /// Notifies about the start of a network download of a an asset from the CDN
    /// - Parameter downloadRequest: the request
    func didStartDownloadOfAsset(withGlobalIdentifier globalIdentifier: GlobalIdentifier,
                                 descriptor: any SHAssetDescriptor,
                                 in groupId: String)
    
    /// Notify about a failed attempt to download some assets
    /// - Parameters:
    ///   - globalIdentifier: the global identifier for the asset
    ///   - groupId: the group id of the request it belongs to
    ///   - error: the error
    func didFailDownloadOfAsset(withGlobalIdentifier: GlobalIdentifier,
                                in groupId: String,
                                with error: Error)
    
    /// Notifies about an unrecoverable error (usually an asset that couldn't be downloaded after many attempts).
    /// These assets global identifiers are blacklisted, to prevent the same failure to happen repeatedly.
    /// It's the Client's responsibility to whitelist them as needed.
    /// - Parameters:
    ///   - globalIdentifier: the global identifier of the asset
    ///   - groupId: the group id of the request it belongs to
    func didFailRepeatedlyDownloadOfAsset(withGlobalIdentifier: GlobalIdentifier,
                                          in groupId: String)
    
    /// Notifies about assets in the local library that are linked to one on the server (backed up)
    /// - Parameters:
    ///   - localToGlobal: The global identifier of the remote asset to the corresponding local `PHAsset` from the Apple Photos Library
    func didIdentify(globalToLocalAssets: [GlobalIdentifier: PHAsset])
    
    /// The low res was downloaded successfullly for some assets
    /// - Parameter _: the decrypted low res asset
    func didFetchLowResolutionAsset(_: any SHDecryptedAsset)
    
    /// The hi res was downloaded successfullly for some assets
    /// - Parameters:
    /// - Parameter _: the decrypted hi res asset
    func didFetchHighResolutionAsset(_: any SHDecryptedAsset)
    
    /// Notifies about the successful download operation for a specific asset.
    /// - Parameter globalIdentifier: the global identifier for the asset
    /// - Parameter groupId: the group id of the request it belongs to
    func didCompleteDownloadOfAsset(
        withGlobalIdentifier globalIdentifier: GlobalIdentifier,
        in groupId: String
    )
    
    /// One cycle of downloads has finished
    /// - Parameter result:  a callback returning either the descriptors for the assets downloaded,
    /// or  an error if items couldn't be dequeued, or the descriptors couldn't be fetched
    func didCompleteDownloadCycle(
        with result: Result<[(any SHDecryptedAsset, any SHAssetDescriptor)], Error>
    )
    
}

public protocol SHAssetActivityRestorationDelegate {
    
    /// Notify the delegate that the restoration started.
    func didStartRestoration()
    
    /// Provide the descriptors for items uploaded by this user, but not shared
    /// - Parameter from: the descriptors
    func restoreUploadQueueItems(from: [String: [(SHUploadHistoryItem, Date)]])
    
    /// Provide the descriptors shared by this user with other users
    /// - Parameter from: the descriptors
    func restoreShareQueueItems(from: [String: [(SHShareHistoryItem, Date)]])
    
    /// Notify the delegate that the restoration was completed.
    /// This can be used as a signal to update all the threads, so the list of user identifiers
    /// involved in the restoration of the upload/share requests is provided.
    /// 
    /// - Parameter userIdsInvolvedInRestoration: All the user identifiers involved in the restoration of the assets
    func didCompleteRestoration(userIdsInvolvedInRestoration: [String])
}

public protocol SHOutboundAssetOperationDelegate {}

public protocol SHAssetFetcherDelegate: SHOutboundAssetOperationDelegate {
    func didStartFetchingForUpload(queueItemIdentifier: String)
    func didStartFetchingForSharing(queueItemIdentifier: String)
    func didCompleteFetchingForUpload(queueItemIdentifier: String)
    func didCompleteFetchingForSharing(queueItemIdentifier: String)
    func didFailFetchingForUpload(queueItemIdentifier: String)
    func didFailFetchingForSharing(queueItemIdentifier: String)
}

public protocol SHAssetEncrypterDelegate: SHOutboundAssetOperationDelegate {
    func didStartEncryption(queueItemIdentifier: String)
    func didCompleteEncryption(queueItemIdentifier: String)
    func didFailEncryption(queueItemIdentifier: String)
}


public protocol SHAssetUploaderDelegate: SHOutboundAssetOperationDelegate {
    func didStartUpload(queueItemIdentifier: String)
    func didCompleteUpload(queueItemIdentifier: String)
    func didFailUpload(queueItemIdentifier: String, error: Error)
}


public protocol SHAssetSharerDelegate: SHOutboundAssetOperationDelegate {
    func didStartSharing(queueItemIdentifier: String)
    func didCompleteSharing(queueItemIdentifier: String)
    func didFailSharing(queueItemIdentifier: String)
}

