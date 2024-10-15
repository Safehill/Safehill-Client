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
    
    public static func forSize(_ size: CGSize?) -> SHAssetQuality {
        let maxLowSize = SHSizeForQuality(quality: .lowResolution)
        let maxMidSize = SHSizeForQuality(quality: .midResolution)
        
        let requestedQuality: SHAssetQuality
        
        ///
        /// If lower or equal `.lowResolution` size, `.lowResolution`
        /// If lower or equal `.midResolution` size, `.midResolution`
        /// Otherwise retrieve the `.hiResolution` size
        ///
        if let size = size {
            if size.width <= maxLowSize.width,
               size.height <= maxLowSize.height {
                requestedQuality = .lowResolution
            }
            else if size.width <= maxMidSize.width,
                    size.height <= maxMidSize.height {
                requestedQuality = .midResolution
            }
            else {
                requestedQuality = .hiResolution
            }
        } else {
            requestedQuality = .hiResolution
        }
        
        return requestedQuality
    }
}
