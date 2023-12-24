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
    
    var platformImage: UIImage {
        guard case .uiKit(let uiImage) = self else {
            fatalError("platform inconsistency")
        }
        return uiImage
    }
#elseif os(macOS)
    case appKit(NSImage)
    
    var platformImage: NSImage {
        guard case .appKit(let nsImage) = self else {
            fatalError("platform inconsistency")
        }
        return nsImage
    }
#endif
}

#if os(macOS)
extension NSImage {
    func resized(to newSize: NSSize) -> NSImage? {
        if let bitmapRep = NSBitmapImageRep(
            bitmapDataPlanes: nil, pixelsWide: Int(newSize.width), pixelsHigh: Int(newSize.height),
            bitsPerSample: 8, samplesPerPixel: 4, hasAlpha: true, isPlanar: false,
            colorSpaceName: .calibratedRGB, bytesPerRow: 0, bitsPerPixel: 0
        ) {
            let sizeKeepingRatio: CGSize
            if self.size.width > self.size.height {
                ///
                /// **Landscape**
                /// `self.size.width : newSize.width = self.size.height : x`
                /// `x = newSize.width * self.size.height / self.size.width`
                ///
                let ratio = self.size.height / self.size.width
                sizeKeepingRatio = CGSize(width: newSize.width, height: newSize.width * ratio)
            } else {
                ///
                /// **Portrait**
                /// `self.size.width : x = self.size.height : newSize.height
                /// `x = newSize.height * self.size.width / self.size.height
                ///
                let ratio = self.size.width / self.size.height
                sizeKeepingRatio = CGSize(width: newSize.height * ratio, height: newSize.height)
            }
            
            bitmapRep.size = sizeKeepingRatio
            NSGraphicsContext.saveGraphicsState()
            NSGraphicsContext.current = NSGraphicsContext(bitmapImageRep: bitmapRep)
            draw(in: NSRect(x: 0, y: 0, width: sizeKeepingRatio.width, height: sizeKeepingRatio.height), from: .zero, operation: .copy, fraction: 1.0)
            NSGraphicsContext.restoreGraphicsState()

            let resizedImage = NSImage(size: sizeKeepingRatio)
            resizedImage.addRepresentation(bitmapRep)
            return resizedImage
        }

        return nil
    }
}
#endif

#if os(iOS)
extension UIImage {
    func resized(to newSize: CGSize) -> UIImage? {
        let sizeKeepingRatio: CGSize
        if self.size.width > self.size.height {
            ///
            /// **Landscape**
            /// `self.size.width : newSize.width = self.size.height : x`
            /// `x = newSize.width * self.size.height / self.size.width`
            ///
            let ratio = self.size.height / self.size.width
            sizeKeepingRatio = CGSize(width: newSize.width, height: newSize.width * ratio)
        } else {
            ///
            /// **Portrait**
            /// `self.size.width : x = self.size.height : newSize.height
            /// `x = newSize.height * self.size.width / self.size.height
            ///
            let ratio = self.size.width / self.size.height
            sizeKeepingRatio = CGSize(width: newSize.height * ratio, height: newSize.height)
        }
        UIGraphicsBeginImageContextWithOptions(sizeKeepingRatio, false, 0.0);
        self.draw(in: CGRectMake(0, 0, sizeKeepingRatio.width, sizeKeepingRatio.height))
        let newImage: UIImage? = UIGraphicsGetImageFromCurrentImageContext()
        UIGraphicsEndImageContext()
        return newImage
    }
}
#endif


public extension PHAsset {
    
    /// Get the asset data from a lazy-loaded PHAsset object
    /// - Parameters:
    ///   - asset: the PHAsset object
    ///   - size: the size of the asset. If nil, gets the original asset size (high quality), and also saves it to the `localPHAssetHighQualityDataCache`
    ///   - imageManager: the image manager to use (useful in case of a PHCachedImage manager)
    ///   - synchronousFetch: determines how many times the completionHandler is called. Asynchronous fetching may call the completion handler multiple times with lower resolution version of the requested asset as soon as it's ready
    ///   - deliveryMode: the `PHImageRequestOptionsDeliveryMode`
    ///   - shouldCache: whether or not should cache it in `SHLocalPHAssetHighQualityDataCache`
    ///   - exactSize: whether or not the image should be resized to the requested size (in case a higher resolution is available)
    ///   - completionHandler: the completion handler
    func data(forSize size: CGSize? = nil,
              usingImageManager imageManager: PHImageManager,
              synchronousFetch: Bool,
              deliveryMode: PHImageRequestOptionsDeliveryMode = .opportunistic,
              resizeMode: PHImageRequestOptionsResizeMode = .fast,
              shouldCache: Bool = false,
              exactSize: Bool = false,
              completionHandler: @escaping (Swift.Result<Data, Error>) -> ()) {
        if let data = SHLocalPHAssetHighQualityDataCache.data(forAssetId: self.localIdentifier) {
            completionHandler(.success(data))
            return
        }
        
        switch self.mediaType {
        case .image:
            break
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
            completionHandler(.failure(SHPhotoAssetError.unsupportedMediaType))
        }
        
        self.image(forSize: size,
                   usingImageManager: imageManager,
                   synchronousFetch: synchronousFetch,
                   deliveryMode: deliveryMode,
                   resizeMode: resizeMode
        ) { (result: Result<NSUIImage, Error>) in
            switch result {
            case .success(let nsuiimage):
#if os(iOS)
                guard case .uiKit(var image) = nsuiimage else {
                    completionHandler(.failure(SHBackgroundOperationError.unexpectedData(nsuiimage)))
                    return
                }
                
                if let size = size, // A size was specified
                   (image.size.width > size.width || image.size.height > size.height), // the retrieved size doesn't match the requested size
                   exactSize, // the exact size was requested
                   let newSizeImage = image.resized(to: size) // resizing was possible
                {
                    image = newSizeImage
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
                guard case .appKit(var image) = nsuiimage else {
                    completionHandler(.failure(SHBackgroundOperationError.unexpectedData(nsuiimage)))
                    return
                }
                
                if let size = size, // A size was specified
                   image.size != size, // the retrieved size doesn't match the requested size
                   exactSize, // the exact size was requested
                   let newSizeImage = image.resized(to: size) // resizing was possible
                {
                    image = newSizeImage
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
               resizeMode: PHImageRequestOptionsResizeMode = .fast,
               // TODO: Implement iCloud progress handler when downloading the image
               progressHandler: ((Double, Error?, UnsafeMutablePointer<ObjCBool>, [AnyHashable : Any]?) -> Void)? = nil,
               completionHandler: @escaping (Swift.Result<NSUIImage, Error>) -> ()) {
        
        guard self.mediaType == .image else {
            completionHandler(.failure(SHPhotoAssetError.unsupportedMediaType))
            return
        }
        
        let options = PHImageRequestOptions()
        options.isSynchronous = synchronousFetch
        options.isNetworkAccessAllowed = true
        options.deliveryMode = deliveryMode
        options.progressHandler = progressHandler
        options.resizeMode = resizeMode

        let targetSize = CGSize(
            width: min(size?.width ?? CGFloat(self.pixelWidth), CGFloat(self.pixelWidth)),
            height: min(size?.width ?? CGFloat(self.pixelHeight), CGFloat(self.pixelHeight))
        )

        imageManager.requestImage(
            for: self,
            targetSize: targetSize,
            contentMode: PHImageContentMode.default,
            options: options
        ) {
            image, _ in
            if let image = image {
                
                var exactSizeImage = image
                if resizeMode == .exact,
                   image.size.width != targetSize.width || image.size.height != targetSize.height {
                    log.warning("Even a resize=exact was requested, Photos returned an asset whose size is \(image.size.width)x\(image.size.height) (requested size \(targetSize.width)x\(targetSize.height)")
                    if let resized = image.resized(to: targetSize) {
                        exactSizeImage = resized
                    } else {
                        completionHandler(.failure(SHPhotoAssetError.applePhotosAssetRetrievalError("")))
                        return
                    }
                }
                
#if os(iOS)
                completionHandler(.success(NSUIImage.uiKit(exactSizeImage)))
#else
                completionHandler(.success(NSUIImage.appKit(exactSizeImage)))
#endif
                return

            } else {
                completionHandler(.failure(SHBackgroundOperationError.unexpectedData(image)))
                return
            }
        }
    }
}


