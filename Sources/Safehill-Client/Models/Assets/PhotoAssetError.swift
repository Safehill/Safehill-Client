import Foundation


public enum SHPhotoAssetError : Error, CustomStringConvertible, LocalizedError {
    case unsupportedMediaType
    case applePhotoAssetFileNotOnDisk(String?)
    case applePhotosAssetRetrievalError(String)
    case photoResizingError
    
    public var description: String {
        switch self {
        case .unsupportedMediaType:
            return "the asset media type is not supported"
        case .applePhotoAssetFileNotOnDisk(let path):
            if let path {
                return "there is no photo asset at path \(path)"
            } else {
                return "photo asset url could not be retrieved"
            }
        case .applePhotosAssetRetrievalError(let reason):
            return "Some assets could not be fetch from the Apple Photos library: \(reason)"
        case .photoResizingError:
            return "There was an error when resizing the image"
        }
    }
    
    public var errorDescription: String? {
        return description
    }
}
