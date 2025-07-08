import Foundation
import Photos


public enum Asset: GenericAssetIdentifiable {
    case fromApplePhotosLibrary(PHAsset) // In the local device library only
    case fromApplePhotosLibraryBackedUp(BackedUpAsset) // Both in the local device library and in the Safehill cloud
    case downloading(any SHAssetDescriptor) // In the Safehill cloud only, no version data retrieved yet
    case downloaded(any SHAssetDescriptor, any SHDecryptedAsset) // In the Safehill cloud only, some version data retrieved and held in the SHDecryptedAsset object
    
    public var debugType: String {
        switch self {
        case .fromApplePhotosLibrary:
            return "fromApplePhotosLibrary"
        case .fromApplePhotosLibraryBackedUp:
            return "fromApplePhotosLibraryBackedUp"
        case .downloading:
            return "downloading"
        case .downloaded:
            return "downloaded"
        }
    }
    
    public var identifier: String {
        switch self {
        case .fromApplePhotosLibrary(let asset):
            return asset.localIdentifier
        case .fromApplePhotosLibraryBackedUp(let asset):
            return asset.globalIdentifier
        case .downloading(let descriptor):
            return descriptor.globalIdentifier
        case .downloaded(let descriptor, _):
            return descriptor.globalIdentifier
        }
    }
    
    public var localIdentifier: String? {
        switch self {
        case .fromApplePhotosLibrary(let phAsset):
            return phAsset.localIdentifier
        case .fromApplePhotosLibraryBackedUp(let asset):
            return asset.phAsset.localIdentifier
        case .downloading(let descriptor):
            return descriptor.localIdentifier
        case .downloaded(let descriptor, _):
            return descriptor.localIdentifier
        }
    }
    
    public var globalIdentifier: String? {
        switch self {
        case .fromApplePhotosLibrary(_):
            return nil
        case .fromApplePhotosLibraryBackedUp(let asset):
            return asset.globalIdentifier
        case .downloading(let descriptor):
            return descriptor.globalIdentifier
        case .downloaded(let descriptor, _):
            return descriptor.globalIdentifier
        }
    }
    
    public var creationDate: Date? {
        switch self {
        case .fromApplePhotosLibrary(let asset):
            return asset.creationDate
        case .fromApplePhotosLibraryBackedUp(let asset):
            return asset.phAsset.creationDate
        case .downloading(let descriptor):
            return descriptor.creationDate
        case .downloaded(let descriptor, _):
            return descriptor.creationDate
        }
    }
    
    public var isFromLocalLibrary: Bool {
        switch self {
        case .fromApplePhotosLibrary(_), .fromApplePhotosLibraryBackedUp(_):
            return true
        default:
            return false
        }
    }
    
    public var isDownloading: Bool {
        if case .downloading(_) = self {
            return true
        }
        return false
    }
    
    public var isFromRemoteLibrary: Bool {
        switch self {
        case .downloaded(_, _), .fromApplePhotosLibraryBackedUp(_), .downloading(_):
            return true
        default:
            return false
        }
    }
    
    public var width: Int? {
        switch self {
        case .fromApplePhotosLibrary(let phAsset):
            return phAsset.pixelWidth
        case .fromApplePhotosLibraryBackedUp(let backedUpAsset):
            let phAsset = backedUpAsset.phAsset
            return phAsset.pixelWidth
        default:
            return nil
        }
    }
    
    public var height: Int? {
        switch self {
        case .fromApplePhotosLibrary(let phAsset):
            return phAsset.pixelHeight
        case .fromApplePhotosLibraryBackedUp(let backedUpAsset):
            let phAsset = backedUpAsset.phAsset
            return phAsset.pixelHeight
        default:
            return nil
        }
    }
}
