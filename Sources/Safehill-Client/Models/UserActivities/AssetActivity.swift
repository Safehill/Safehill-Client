import Foundation

public protocol AssetActivity: ReadableAssetActivity {
    var requestedAt: Date { get }
    func withAssetsRemoved(ids: [String]) -> any AssetActivity
}
