import Foundation

public protocol AssetActivity: ReadableAssetActivity {
    
    var requestedAt: Date { get }
    
    /// Returns an copy of the activity with the assets removed.
    /// Use this method to keep assets in the activity immutable.
    /// - Parameter ids: the asset ids to remove
    /// - Returns: `nil` if no assets remain, the new activity otherwise
    func withAssetsRemoved(ids: [String]) -> (any AssetActivity)?
}
