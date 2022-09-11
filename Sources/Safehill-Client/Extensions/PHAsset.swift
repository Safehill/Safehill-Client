import Photos
#if os(iOS)
import UIKit
#endif

public extension PHAsset {
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
              shouldCache: Bool = false,
              completionHandler: @escaping (Swift.Result<Data, Error>) -> ()) {
        if let data = SHLocalPHAssetHighQualityDataCache.data(forAssetId: self.localIdentifier) {
            completionHandler(.success(data))
            return
        }
        
        self.image(forSize: size, usingImageManager: imageManager, synchronousFetch: synchronousFetch) {
            result in
            switch result {
            case .success(let image):
#if os(iOS)
                if let data = image.pngData() {
                    completionHandler(.success(data))
                    if shouldCache {
                        SHLocalPHAssetHighQualityDataCache.add(data, forAssetId: self.localIdentifier)
                    }
                } else {
                    completionHandler(.failure(SHBackgroundOperationError.unexpectedData(image)))
                }
#else
                if let data = image.png {
                    completionHandler(.success(data))
                    if shouldCache {
                        localPHAssetHighQualityDataCache.add(data, forAssetId: self.localIdentifier)
                    }
                } else {
                    completionHandler(.failure(SHAssetFetchError.unexpectedData(image)))
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
               // TODO: Implement iCloud progress handler when downloading the image
               progressHandler: ((Double, Error?, UnsafeMutablePointer<ObjCBool>, [AnyHashable : Any]?) -> Void)? = nil,
               completionHandler: @escaping (Swift.Result<UIImage, Error>) -> ()) {
        
        let options = PHImageRequestOptions()
        options.isSynchronous = synchronousFetch
        options.isNetworkAccessAllowed = true
        options.deliveryMode = .opportunistic
        options.progressHandler = progressHandler

        let targetSize = CGSize(width: min(size?.width ?? CGFloat(self.pixelWidth), CGFloat(self.pixelWidth)),
                                height: min(size?.width ?? CGFloat(self.pixelHeight), CGFloat(self.pixelHeight)))

        switch self.mediaType {
        case .image:
            imageManager.requestImage(for: self, targetSize: targetSize, contentMode: PHImageContentMode.default, options: options) {
                image, _ in
                if let image = image {
                    completionHandler(.success(image))
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


