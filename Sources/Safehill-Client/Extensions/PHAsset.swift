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

let imageSizeForGlobalIdCalculation = CGSize(width: 240.0, height: 240.0)


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
public extension NSImage {
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
public extension UIImage {
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
        
        guard sizeKeepingRatio.width > 0, sizeKeepingRatio.height > 0 else {
            log.error("Invalid size when resizing image to \(String(describing: newSize)): \(String(describing: sizeKeepingRatio))")
            return nil
        }
        
        // Use UIGraphicsImageRenderer for efficient image rendering
        let renderer = UIGraphicsImageRenderer(size: sizeKeepingRatio)
        let resizedImage = renderer.image { _ in
            self.draw(in: CGRect(origin: .zero, size: sizeKeepingRatio))
        }
        
        // Ensure the resized image is valid
        guard let finalImage = resizedImage.cgImage else {
            log.error("Failed to create resized image")
            return nil
        }
        
        return UIImage(cgImage: finalImage, scale: self.scale, orientation: self.imageOrientation)
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
              exactSize: Bool = false
    ) async throws -> Data {
        if let data = await LocalPHAssetHighQualityDataCache.data(forAssetId: self.localIdentifier) {
            return data
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
            throw SHPhotoAssetError.unsupportedMediaType
        }
        
        let nsuiimage = try await self.image(
            forSize: size,
            usingImageManager: imageManager,
            synchronousFetch: synchronousFetch,
            deliveryMode: deliveryMode,
            resizeMode: resizeMode
        )
        
#if os(iOS)
        guard case .uiKit(var image) = nsuiimage else {
            throw SHBackgroundOperationError.unexpectedData(nsuiimage)
        }
        
        if let size = size, // A size was specified
           (image.size.width > size.width || image.size.height > size.height), // the retrieved size doesn't match the requested size
           exactSize // the exact size was requested
        {
            if let newSizeImage = image.resized(to: size) { // resizing was possible
                image = newSizeImage
            } else {
                throw SHBackgroundOperationError.fatalError("failed to fetch exact size")
            }
        }
        
        if let data = image.pngData() {
            if shouldCache {
                await LocalPHAssetHighQualityDataCache.add(data, forAssetId: self.localIdentifier)
            }
            return data
        } else {
            throw SHBackgroundOperationError.unexpectedData(image)
        }
#else
        guard case .appKit(var image) = nsuiimage else {
            throw SHBackgroundOperationError.unexpectedData(nsuiimage)
        }
        
        if let size = size, // A size was specified
           image.size != size, // the retrieved size doesn't match the requested size
           exactSize // the exact size was requested
        {
            if let newSizeImage = image.resized(to: size) { // resizing was possible
                image = newSizeImage
            } else {
                throw SHBackgroundOperationError.fatalError("failed to fetch exact size")
            }
        }

        if let data = image.png {
            if shouldCache {
                await LocalPHAssetHighQualityDataCache.add(data, forAssetId: self.localIdentifier)
            }
            return data
        } else {
            throw SHBackgroundOperationError.unexpectedData(image)
        }
#endif
    }
    
    func image(forSize size: CGSize? = nil,
               usingImageManager imageManager: PHImageManager,
               synchronousFetch: Bool,
               deliveryMode: PHImageRequestOptionsDeliveryMode = .opportunistic,
               resizeMode: PHImageRequestOptionsResizeMode = .fast,
               // TODO: Implement iCloud progress handler when downloading the image
               progressHandler: ((Double, Error?, UnsafeMutablePointer<ObjCBool>, [AnyHashable : Any]?) -> Void)? = nil
    ) async throws -> NSUIImage {
        
        guard self.mediaType == .image else {
            throw SHPhotoAssetError.unsupportedMediaType
        }
        
        let options = PHImageRequestOptions()
        options.isSynchronous = synchronousFetch
        options.isNetworkAccessAllowed = true
        options.deliveryMode = deliveryMode
        options.progressHandler = progressHandler
        options.resizeMode = resizeMode

        let targetSize = CGSize(
            width: min(size?.width ?? CGFloat(self.pixelWidth), CGFloat(self.pixelWidth)),
            height: min(size?.height ?? CGFloat(self.pixelHeight), CGFloat(self.pixelHeight))
        )

        return try await withUnsafeThrowingContinuation { continuation in
            
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
                        log.warning("Although a resize=exact was requested, Photos returned an asset whose size (\(image.size.width)x\(image.size.height)) is not the one requested (\(targetSize.width)x\(targetSize.height))")
                        if let resized = image.resized(to: targetSize) {
                            exactSizeImage = resized
                        } else {
                            continuation.resume(throwing: SHPhotoAssetError.applePhotosAssetRetrievalError(""))
                            return
                        }
                    }
                    
#if os(iOS)
                    continuation.resume(returning: NSUIImage.uiKit(exactSizeImage))
#else
                    continuation.resume(returning: NSUIImage.appKit(exactSizeImage))
#endif
                } else {
                    continuation.resume(throwing: SHBackgroundOperationError.unexpectedData(image))
                }
            }
        }
    }
}


