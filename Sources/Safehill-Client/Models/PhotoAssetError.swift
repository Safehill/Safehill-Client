import Foundation


public enum SHPhotoAssetError : Error, CustomStringConvertible, LocalizedError {
    case unsupportedMediaType
    case applePhotosAssetRetrievalError(String)
    
    public var description: String {
        switch self {
        case .unsupportedMediaType:
            return "the asset media type is not supported"
        case .applePhotosAssetRetrievalError(let reason):
            return "error while retrieving asset from Apple photos library: \(reason)"
        }
    }
    
    public var errorDescription: String? {
        return description
    }
}
