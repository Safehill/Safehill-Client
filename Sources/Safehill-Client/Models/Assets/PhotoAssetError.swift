import Foundation


public enum SHPhotoAssetError : Error, CustomStringConvertible, LocalizedError {
    case unsupportedMediaType
    case applePhotosAssetRetrievalError(String)
    case photoResizingError
    
    public var description: String {
        switch self {
        case .unsupportedMediaType:
            return "the asset media type is not supported"
        case .applePhotosAssetRetrievalError(let reason):
            return "error while retrieving asset from Apple photos library: \(reason)"
        case .photoResizingError:
            return "There was an error when resizing the image"
            
        }
    }
    
    public var errorDescription: String? {
        return description
    }
}
