import Foundation

/// Safehill clients store 2 versions per asset, one low resolution for the thumbnail, one full size
public enum SHAssetQuality: String {
    case lowResolution = "low",
         midResolution = "mid",
         hiResolution = "hi"
    
    public static var all: [SHAssetQuality] {
        return [
            .lowResolution,
            .midResolution,
            .hiResolution
        ]
    }
}
