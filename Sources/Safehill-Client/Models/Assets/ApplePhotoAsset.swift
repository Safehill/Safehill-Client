import Photos
import Foundation
#if os(iOS)
import UIKit
#elseif os(macOS)
import AppKit
#endif
import Safehill_Crypto

public class SHApplePhotoAsset : NSObject, NSSecureCoding {
    
    public static var supportsSecureCoding = true
    
    enum CodingKeys: String, CodingKey {
        case localIdentifier
        case globalIdentifier
    }
    
    private var calculatedGlobalIdentifier: GlobalIdentifier? = nil
    
    var imageManager: PHImageManager
    
    public let phAsset: PHAsset
    
    internal func setGlobalIdentifier(_ gid: GlobalIdentifier) throws {
        if self.calculatedGlobalIdentifier == nil {
            self.calculatedGlobalIdentifier = gid
        } else {
            if self.calculatedGlobalIdentifier != gid {
                throw SHBackgroundOperationError.fatalError("previously generated global identifier doesn't match the one provided")
            }
        }
    }
    
    public init(for asset: PHAsset,
                usingCachingImageManager manager: PHCachingImageManager? = nil) {
        self.phAsset = asset
        self.imageManager = manager ?? PHImageManager.default()
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let phAssetIdentifier = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.localIdentifier.rawValue)
        let calculatedGlobalId = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.globalIdentifier.rawValue)
        
        guard let phAssetIdentifier = phAssetIdentifier as? String else {
            log.error("unexpected value for phAssetIdentifier when decoding SHApplePhotoAsset object")
            return nil
        }
        
        let fetchResult = PHAsset.fetchAssets(withLocalIdentifiers: [phAssetIdentifier], options: nil)
        guard fetchResult.count > 0 else {
            return nil
        }
        
        let asset = fetchResult.object(at: 0)
        self.init(for: asset)
        if let calculatedGlobalId = calculatedGlobalId as? GlobalIdentifier {
            self.calculatedGlobalIdentifier = calculatedGlobalId
        }
    }
    
    var globalIdentifier: GlobalIdentifier? {
        calculatedGlobalIdentifier
    }
    
    /// Generates the global identifier of the asset based on EXIF metadata (if available),
    /// or generates a UUID.
    /// Caches the value if already calculated and retrurns the cached value if so.
    ///
    /// - Returns: the global identifier
    func generateGlobalIdentifier() async -> GlobalIdentifier {
        guard calculatedGlobalIdentifier == nil else {
            return calculatedGlobalIdentifier!
        }
        
        let identifier = await SHHashingController.globalIdentifier(for: self)
        calculatedGlobalIdentifier = identifier
        return identifier
    }
    
    /// Generate the perceptial hash of the `.lowResolution` image
    /// - Returns: the hash
    func generatePerceptualHash() async throws -> PerceptualHash {
        let size = SHSizeForQuality(quality: .lowResolution)
        let image = try await self.phAsset.imageSynchronous(forSize: size, usingImageManager: self.imageManager)
        return try SHHashingController.perceptualHash(for: image)
    }
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.phAsset.localIdentifier, forKey: CodingKeys.localIdentifier.rawValue)
        coder.encode(self.calculatedGlobalIdentifier, forKey: CodingKeys.globalIdentifier.rawValue)
    }
}

extension SHApplePhotoAsset {
    
    func getOriginalUrl(
        completion: @escaping (_ url: URL?) -> Void
    ) {
        self.phAsset.requestContentEditingInput(
            with: PHContentEditingInputRequestOptions()
        ) { (contentEditingInput, dictInfo) in
            
            if let url = contentEditingInput?.fullSizeImageURL {
                completion(url)
            } else {
                completion(nil)
            }
        }
    }
    
    func getExifData() async -> (aperture: Float, shutterSpeed: Float, iso: Int)? {
        return await withUnsafeContinuation { continuation in
            self.phAsset.requestContentEditingInput(with: nil) { (input, _) in
                
                guard let input = input else {
                    continuation.resume(returning: nil)
                    return
                }
                guard let url = input.fullSizeImageURL else {
                    continuation.resume(returning: nil)
                    return
                }
                
                let imageSource = CGImageSourceCreateWithURL(url as CFURL, nil)
                let metadata = CGImageSourceCopyPropertiesAtIndex(imageSource!, 0, nil) as? [CFString: Any]
                
                guard let exifData = metadata?[kCGImagePropertyExifDictionary] as? [CFString: Any] 
                else {
                    continuation.resume(returning: nil)
                    return
                }

                // Access EXIF metadata
                let aperture = exifData[kCGImagePropertyExifFNumber] as? Float ?? 0.0
                let shutterSpeed = exifData[kCGImagePropertyExifExposureTime] as? Float ?? 0.0
                let iso = exifData[kCGImagePropertyExifISOSpeedRatings] as? Int ?? 0
                
                continuation.resume(
                    returning: (aperture: aperture, shutterSpeed: shutterSpeed, iso: iso)
                )
            }
        }
    }
    
    func getControlPixelColor() async throws -> (r: CGFloat, g: CGFloat, b: CGFloat, a: CGFloat)? {
        let nsUIimage = try await self.phAsset.imageSynchronous(
            forSize: SHSizeForQuality(quality: SHAssetQuality.lowResolution),
            usingImageManager: self.imageManager,
            resizeMode: .exact
        )
        return Self.getControlPixelColor(image: nsUIimage)
    }
    
    static func getControlPixelColor(image: NSUIImage) -> (r: CGFloat, g: CGFloat, b: CGFloat, a: CGFloat)? {
        
        let controlPixelLocation = (x: 2, y: 3)

#if os(iOS)
        guard case .uiKit(let uiImage) = image else {
            return nil
        }
        
        guard let cgImage = uiImage.cgImage else {
            return nil
        }

        let width = cgImage.width
        let height = cgImage.height
        let bytesPerPixel = 4
        let bytesPerRow = bytesPerPixel * width
        let bitsPerComponent = 8

        var pixelData = [UInt8](repeating: 0, count: Int(height * width * 4))

        guard let context = CGContext(
            data: &pixelData,
            width: width,
            height: height,
            bitsPerComponent: bitsPerComponent,
            bytesPerRow: bytesPerRow,
            space: CGColorSpaceCreateDeviceRGB(),
            bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue
        )
        else {
            return nil
        }

        context.draw(cgImage, in: CGRect(x: 0, y: 0, width: width, height: height))

        let pixelIndex = controlPixelLocation.x * bytesPerRow + controlPixelLocation.y * bytesPerPixel
        let r = CGFloat(pixelData[pixelIndex]) / 255.0
        let g = CGFloat(pixelData[pixelIndex + 1]) / 255.0
        let b = CGFloat(pixelData[pixelIndex + 2]) / 255.0
        let a = CGFloat(pixelData[pixelIndex + 3]) / 255.0

        return (r: r, g: g, b: b, a: a)
#else
        return nil
#endif
    }
    
}


extension SHApplePhotoAsset {
    
    func url() async throws -> URL? {
        switch self.phAsset.mediaType {
        
        case .image:
            return await withUnsafeContinuation { continuation in
                self.phAsset.requestContentEditingInput(with: nil, completionHandler: {
                    (contentEditingInput: PHContentEditingInput?, info: [AnyHashable : Any]) -> Void in
                    continuation.resume(returning: contentEditingInput!.fullSizeImageURL as URL?)
                })
            }
        
//        case .video:
//            let options: PHVideoRequestOptions = PHVideoRequestOptions()
//            options.version = .original
//            
//            return await withUnsafeContinuation { continuation in
//                PHImageManager.default().requestAVAsset(
//                    forVideo: self.phAsset,
//                    options: options,
//                    resultHandler: {
//                        (asset: AVAsset?, audioMix: AVAudioMix?, info: [AnyHashable : Any]?) -> Void in
//                        if let urlAsset = asset as? AVURLAsset {
//                            let localVideoUrl: URL = urlAsset.url as URL
//                            continuation.resume(returning: localVideoUrl)
//                        } else {
//                            continuation.resume(returning: nil)
//                        }
//                    }
//                )
//            }
            
        default:
            throw SHPhotoAssetError.unsupportedMediaType
        }
    }
    
    func fileSize() -> Int64? {
        let resources = PHAssetResource.assetResources(for: self.phAsset)
                  
        if let resource = resources.first {
            let unsignedInt64 = resource.value(forKey: "fileSize") as? CLong
            return Int64(bitPattern: UInt64(unsignedInt64!))
        }
        
        return nil
    }
    
    func fileData() async throws -> Data {
        let url = try await self.url()
        if let url,
           FileManager.default.fileExists(atPath: url.relativePath) {
            return try Data(contentsOf: url, options: .mappedIfSafe)
        } else {
            throw SHPhotoAssetError.applePhotoAssetFileNotOnDisk(url?.absoluteString)
        }
    }
    
    func data(for versions: [SHAssetQuality]) async throws -> [SHAssetQuality: Data] {
        var dict = [SHAssetQuality: Data]()
        
        for version in versions {
            let size = SHSizeForQuality(quality: version)
            let resizedData = try await self.phAsset.dataSynchronous(
                forSize: size,
                usingImageManager: self.imageManager,
                deliveryMode: .highQualityFormat,
                resizeMode: .exact
            )
            
            if let originalSize = self.fileSize(),
               originalSize < resizedData.count,
               let originalData = try? await self.fileData() {
                dict[version] = originalData
            } else {
                dict[version] = resizedData
            }
            
            log.debug("[file-size] \(version.rawValue): data size \(dict[version]?.count ?? 0)")
        }

        return dict
    }
}

extension SHApplePhotoAsset {
    
    public func toUploadableAsset(
        for versions: [SHAssetQuality],
        globalIdentifier: GlobalIdentifier? = nil,
        perceptualHash: PerceptualHash? = nil
    ) async throws -> SHUploadableAsset {
        let globalId: GlobalIdentifier
        if let globalIdentifier {
            globalId = globalIdentifier
        } else {
            globalId = await self.generateGlobalIdentifier()
        }
        
        let fingerprint: PerceptualHash
        if let perceptualHash {
            fingerprint = perceptualHash
        } else {
            fingerprint = try await self.generatePerceptualHash()
        }
        
        return SHUploadableAsset(
            localIdentifier: self.phAsset.localIdentifier,
            globalIdentifier: globalId,
            fingerprint: fingerprint,
            creationDate: self.phAsset.creationDate,
            data: try await data(for: versions)
        )
    }
}
