import Foundation
import Photos

public struct BackedUpAsset {
    public let phAsset: PHAsset
    public let globalIdentifier: String
    
    public init(phAsset: PHAsset, globalIdentifier: String) {
        self.phAsset = phAsset
        self.globalIdentifier = globalIdentifier
    }
}
