import Foundation

///
/// Utility class to diff descriptors coming from `LocalServer` and `RemoteServer`
///
struct AssetDescriptorsDiff {
    
    ///
    /// In-memory representation of a state difference between two asset descriptors
    ///
    struct AssetVersionState {
        let globalIdentifier: String
        let localIdentifier: String
        let quality: SHAssetQuality
        let newUploadState: SHAssetDescriptorUploadState
    }
    
    let assetsRemovedOnServer: [SHRemoteAssetIdentifier]
    let stateDifferentOnServer: [AssetVersionState]
    
    
    ///
    /// Diffs the descriptors fetched from the server from the descriptors in the local cache.
    /// Handle the following cases:
    /// 1. The asset has been encrypted but not yet downloaded (so the server doesn't know about that asset yet)
    ///     -> needs to be kept as the user encryption secret is stored there
    /// 2. The descriptor exists on the server but not locally
    ///     -> It will be created locally, created in the ShareHistory or UploadHistory queue item by any DownloadOperation
    /// 3. The descriptor exists locally but not on the server
    ///     -> remove it as long as it's not case 1
    /// 4. The local upload state doesn't match the remote state
    ///     -> inefficient solution is to verify the asset is in S3. Efficient is to trust value on server
    ///
    /// - Parameters:
    ///   - server: the server descriptors to diff against
    ///   - local: the local descriptors
    /// - Returns: the diff
    ///
    static func generateUsing(server serverDescriptors: [any SHAssetDescriptor],
                              local localDescriptors: [any SHAssetDescriptor]) -> AssetDescriptorsDiff {
        var onlyLocal = localDescriptors
            .map({
                d in SHRemoteAssetIdentifier(globalIdentifier: d.globalIdentifier,
                                             localIdentifier: d.localIdentifier)
            })
            .subtract(
                serverDescriptors.map({
                    d in SHRemoteAssetIdentifier(globalIdentifier: d.globalIdentifier,
                                                 localIdentifier: d.localIdentifier)
                })
            )
        
        if onlyLocal.count > 0 {
            for localDescriptor in localDescriptors {
                let assetRef = SHRemoteAssetIdentifier(globalIdentifier: localDescriptor.globalIdentifier,
                                                       localIdentifier: localDescriptor.localIdentifier)
                if let index = onlyLocal.firstIndex(of: assetRef) {
                    switch localDescriptor.uploadState {
                    case .notStarted:
                        ///
                        /// Assets and its details (like the secrets) are stored locally at encryption time
                        /// As this method can be called while an upload happens, all assets that are not on the server yet,
                        /// but are in the local server with state `.notStarted`, are assumed to be assets that the user is uploading
                        /// (and will make it to the server eventually).
                        /// Do not mark them as removed
                        ///
                        onlyLocal.remove(at: index)
                    case .failed:
                        ///
                        /// Assets can be recorded on device but not on server, when uploading/sharing fails.
                        /// They will actually be intentionally deleted from server when that happens,
                        /// but marked as failed locally
                        ///
                        onlyLocal.remove(at: index)
                    case .partial:
                        ///
                        /// Local assets will never be marked as partial locally.
                        /// `not_started` and `completed` are the only possible states
                        ///
                        break
                    default:
                        break
                    }
                }
            }
        }
        
        // TODO: Handle missing cases
        /// 1. Deleted users should be removed from the shares, and from the knowledge graph
        /// 2. Upload state changes?
        

        return AssetDescriptorsDiff(assetsRemovedOnServer: onlyLocal, stateDifferentOnServer: [])
    }
}
