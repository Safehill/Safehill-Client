import Foundation

public protocol AssetActivity: ReadableAssetActivity {
    
    var requestedAt: Date { get }
    
    /// Returns an copy of the activity with the assets removed.
    /// Use this method to keep assets in the activity immutable.
    /// - Parameter ids: the asset ids to remove
    /// - Returns: `nil` if no assets remain, the new activity otherwise
    func withAssetsRemoved(ids: [AssetReference]) -> (any AssetActivity)?
    
    /// Returns an copy of the activity with the invited phone numbers removed.
    /// Use this method to keep assets in the activity immutable.
    /// - Parameter phoneNumbers: the asset ids to remove
    /// - Returns: `nil` if no assets remain, the new activity otherwise
    func withPhoneNumbersRemoved(_ phoneNumbers: [String]) -> any AssetActivity
}
