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


public extension PHAsset {
    
    private func mediaTypeCheck() throws {
        
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
    }
    
    /// Get the asset data from a lazy-loaded PHAsset object
    /// - Parameters:
    ///   - asset: the PHAsset object
    ///   - size: the size of the asset. If nil, gets the original asset size (high quality), and also saves it to the `localPHAssetHighQualityDataCache`
    ///   - imageManager: the image manager to use (useful in case of a PHCachedImage manager)
    ///   - synchronousFetch: determines how many times the completionHandler is called. Asynchronous fetching may call the completion handler multiple times with lower resolution version of the requested asset as soon as it's ready
    ///   - deliveryMode: the `PHImageRequestOptionsDeliveryMode`
    ///   - completionHandler: the completion handler
    func dataSynchronous(
        forSize size: CGSize? = nil,
        usingImageManager imageManager: PHImageManager,
        deliveryMode: PHImageRequestOptionsDeliveryMode = .opportunistic,
        resizeMode: PHImageRequestOptionsResizeMode = .fast
    ) async throws -> Data {
        
        try self.mediaTypeCheck()
        
        let nsuiimage = try await self.imageSynchronous(
            forSize: size,
            usingImageManager: imageManager,
            deliveryMode: deliveryMode,
            resizeMode: resizeMode
        )
        
        return try nsuiimage.data()
    }
    
    func dataAsynchronous(
        forSize size: CGSize? = nil,
        usingImageManager imageManager: PHImageManager,
        deliveryMode: PHImageRequestOptionsDeliveryMode = .opportunistic,
        resizeMode: PHImageRequestOptionsResizeMode = .fast,
        completionHandler: @escaping (Swift.Result<Data, Error>) -> ()
    ) {
        do { try self.mediaTypeCheck() }
        catch {
            completionHandler(.failure(error))
            return
        }
        
        self.imageAsynchronous(
            forSize: size,
            usingImageManager: imageManager,
            deliveryMode: deliveryMode,
            resizeMode: resizeMode
        ) { (result: Result<NSUIImage, Error>) in
            switch result {
            case .success(let nsuiimage):
                do {
                    let data = try nsuiimage.data()
                    completionHandler(.success(data))
                } catch {
                    completionHandler(.failure(error))
                }
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    func imageSynchronous(
        forSize size: CGSize? = nil,
        usingImageManager imageManager: PHImageManager,
        deliveryMode: PHImageRequestOptionsDeliveryMode = .opportunistic,
        resizeMode: PHImageRequestOptionsResizeMode = .fast,
        // TODO: Implement iCloud progress handler when downloading the image
        progressHandler: ((Double, Error?, UnsafeMutablePointer<ObjCBool>, [AnyHashable : Any]?) -> Void)? = nil
    ) async throws -> NSUIImage {
        
        guard self.mediaType == .image else {
            throw SHPhotoAssetError.unsupportedMediaType
        }
        
        let options = PHImageRequestOptions()
        options.isSynchronous = true
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
                    log.debug("[file-size] requested \(targetSize.width)x\(targetSize.height), retrieved \(image.size.width)x\(image.size.height)")
#if os(iOS)
                    continuation.resume(returning: NSUIImage.uiKit(image))
#else
                    continuation.resume(returning: NSUIImage.appKit(image))
#endif
                } else {
                    continuation.resume(throwing: SHBackgroundOperationError.unexpectedData(image))
                }
            }
        }
    }
    
    func imageAsynchronous(
        forSize size: CGSize? = nil,
        usingImageManager imageManager: PHImageManager,
        deliveryMode: PHImageRequestOptionsDeliveryMode = .opportunistic,
        resizeMode: PHImageRequestOptionsResizeMode = .fast,
        // TODO: Implement iCloud progress handler when downloading the image
        progressHandler: ((Double, Error?, UnsafeMutablePointer<ObjCBool>, [AnyHashable : Any]?) -> Void)? = nil,
        completionHandler: @escaping (Swift.Result<NSUIImage, Error>) -> ()
    ) {
        
        guard self.mediaType == .image else {
            completionHandler(.failure(SHPhotoAssetError.unsupportedMediaType))
            return
        }
        
        let options = PHImageRequestOptions()
        options.isSynchronous = false
        options.isNetworkAccessAllowed = true
        options.deliveryMode = deliveryMode
        options.progressHandler = progressHandler
        options.resizeMode = resizeMode

        let targetSize = CGSize(
            width: min(size?.width ?? CGFloat(self.pixelWidth), CGFloat(self.pixelWidth)),
            height: min(size?.height ?? CGFloat(self.pixelHeight), CGFloat(self.pixelHeight))
        )

        imageManager.requestImage(
            for: self,
            targetSize: targetSize,
            contentMode: PHImageContentMode.default,
            options: options
        ) {
            image, _ in
            if let image = image {
#if os(iOS)
                completionHandler(.success(NSUIImage.uiKit(image)))
#else
                completionHandler(.success(NSUIImage.appKit(image)))
#endif
                return

            } else {
                completionHandler(.failure(SHBackgroundOperationError.unexpectedData(image)))
                return
            }
        }
    }
}


