import Photos
import Safehill_Crypto
#if os(iOS)
import UIKit
#elseif os(macOS)
import AppKit

public extension NSBitmapImageRep {
    var png: Data? { representation(using: .png, properties: [:]) }
}
public extension Data {
    var bitmap: NSBitmapImageRep? { NSBitmapImageRep(data: self) }
}
public extension NSImage {
    var png: Data? { tiffRepresentation?.bitmap?.png }
}
#endif

let imageSizeForGlobalIdCalculation = CGSize(width: 320.0, height: 320.0)

public enum NSUIImage {
#if os(iOS)
    case uiKit(UIImage)
#else
    case appKit(NSImage)
#endif
}

public extension PHAsset {
    
    func globalIdentifier(using imageManager: PHImageManager? = nil) throws -> String{
        var error: Error? = nil
        var data: Data? = nil
        
        ///
        /// Do not call `data(forSize:usingImageManager:synchronousFetch:deliveryMode:)`
        /// for global id calculation as that method optimizes cache hits over size precision.
        /// That means that if a higher image size is cached, that will be returned, which will result in an unstable globalidentifier.
        /// We need to fetch the exact image size based on `imageSizeForGlobalIdCalculation`, regardless of what's in the cache
        ///
        self.image(forSize: imageSizeForGlobalIdCalculation,
                   usingImageManager: imageManager ?? PHImageManager(),
                   synchronousFetch: true,
                   deliveryMode: .highQualityFormat) { (result: Result<NSUIImage, Error>) in
            switch result {
            case .success(let nsuiimage):
#if os(iOS)
                if case .uiKit(let image) = nsuiimage,
                   let d = image.pngData() {
                    data = d
                } else {
                    error = SHBackgroundOperationError.unexpectedData(nsuiimage)
                }
#else
                if case .appKit(let image) = nsuiimage,
                   let d = image.png {
                    data = d
                } else {
                    error = SHBackgroundOperationError.unexpectedData(nsuiimage)
                }
#endif
            case .failure(let err):
                error = err
            }
        }
        
        guard error == nil else {
            throw error!
        }
        guard let data = data else {
            throw SHBackgroundOperationError.unexpectedData(nil)
        }
        let hash = SHHash.stringDigest(for: data)
        return hash
    }
    
    /// Get the asset data from a lazy-loaded PHAsset object
    /// - Parameters:
    ///   - asset: the PHAsset object
    ///   - size: the size of the asset. If nil, gets the original asset size (high quality), and also saves it to the `localPHAssetHighQualityDataCache`
    ///   - imageManager: the image manager to use (useful in case of a PHCachedImage manager)
    ///   - synchronousFetch: determines how many times the completionHandler is called. Asynchronous fetching may call the completion handler multiple times with lower resolution version of the requested asset as soon as it's ready
    ///   - completionHandler: the completion handler
    func data(forSize size: CGSize? = nil,
              usingImageManager imageManager: PHImageManager,
              synchronousFetch: Bool,
              deliveryMode: PHImageRequestOptionsDeliveryMode = .opportunistic,
              shouldCache: Bool = false,
              completionHandler: @escaping (Swift.Result<Data, Error>) -> ()) {
        if let data = SHLocalPHAssetHighQualityDataCache.data(forAssetId: self.localIdentifier) {
            completionHandler(.success(data))
            return
        }
        
        self.image(forSize: size,
                   usingImageManager: imageManager,
                   synchronousFetch: synchronousFetch,
                   deliveryMode: deliveryMode) { (result: Result<NSUIImage, Error>) in
            switch result {
            case .success(let nsuiimage):
#if os(iOS)
                guard case .uiKit(let image) = nsuiimage else {
                    completionHandler(.failure(SHBackgroundOperationError.unexpectedData(nsuiimage)))
                    return
                }
                if let data = image.pngData() {
                    completionHandler(.success(data))
                    if shouldCache {
                        SHLocalPHAssetHighQualityDataCache.add(data, forAssetId: self.localIdentifier)
                    }
                } else {
                    completionHandler(.failure(SHBackgroundOperationError.unexpectedData(image)))
                }
#else
                guard case .appKit(let image) = nsuiimage else {
                    completionHandler(.failure(SHBackgroundOperationError.unexpectedData(nsuiimage)))
                    return
                }

                if let data = image.png {
                    completionHandler(.success(data))
                    if shouldCache {
                        SHLocalPHAssetHighQualityDataCache.add(data, forAssetId: self.localIdentifier)
                    }
                } else {
                    completionHandler(.failure(SHBackgroundOperationError.unexpectedData(image)))
                }
#endif
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    func image(forSize size: CGSize? = nil,
               usingImageManager imageManager: PHImageManager,
               synchronousFetch: Bool,
               deliveryMode: PHImageRequestOptionsDeliveryMode = .opportunistic,
               // TODO: Implement iCloud progress handler when downloading the image
               progressHandler: ((Double, Error?, UnsafeMutablePointer<ObjCBool>, [AnyHashable : Any]?) -> Void)? = nil,
               completionHandler: @escaping (Swift.Result<NSUIImage, Error>) -> ()) {
        
        let options = PHImageRequestOptions()
        options.isSynchronous = synchronousFetch
        options.isNetworkAccessAllowed = true
        options.deliveryMode = deliveryMode
        options.progressHandler = progressHandler

        let targetSize = CGSize(width: min(size?.width ?? CGFloat(self.pixelWidth), CGFloat(self.pixelWidth)),
                                height: min(size?.width ?? CGFloat(self.pixelHeight), CGFloat(self.pixelHeight)))

        switch self.mediaType {
        case .image:
            imageManager.requestImage(for: self, targetSize: targetSize, contentMode: PHImageContentMode.default, options: options) {
                image, _ in
                if let image = image {
#if os(iOS)
                    completionHandler(.success(NSUIImage.uiKit(image)))
#else
                    completionHandler(.success(NSUIImage.appKit(image)))
#endif
                    return
                }
                completionHandler(.failure(SHBackgroundOperationError.unexpectedData(image)))
            }
//        case .video:
//            imageManager.requestAVAsset(forVideo: self, options: nil) { asset, audioMix, info in
//                if let asset = asset as? AVURLAsset,
//                   let data = NSData(contentsOf: asset.url) as Data? {
//                    completionHandler(.success(data))
//                } else {
//                    completionHandler(.failure(SHAssetFetchError.unexpectedData(asset)))
//                }
//            }
        default:
            completionHandler(.failure(SHBackgroundOperationError.fatalError("PHAsset mediaType not supported \(self.mediaType)")))
        }
    }
}


