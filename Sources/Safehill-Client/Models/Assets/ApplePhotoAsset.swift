import Photos
import Foundation
#if os(iOS)
import UIKit
#elseif os(macOS)
import AppKit
#endif
import Safehill_Crypto


private let PHAssetIdentifierKey = "phAssetLocalIdentifier"
private let CalculatedGlobalIdentifier = "calculatedGlobalIdentifier"


public class SHApplePhotoAsset : NSObject, NSSecureCoding {
    public static var supportsSecureCoding = true
    
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
        let phAssetIdentifier = decoder.decodeObject(of: NSString.self, forKey: PHAssetIdentifierKey)
        let calculatedGlobalId = decoder.decodeObject(of: NSString.self, forKey: CalculatedGlobalIdentifier)
        
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
    
    func generateGlobalIdentifier() async -> GlobalIdentifier {
        guard calculatedGlobalIdentifier == nil else {
            return calculatedGlobalIdentifier!
        }
        
        let exifData = await self.getExifData()
        
        /// Global identifier is generated based on a combination of:
        /// - media type and subtype (metadata)
        /// - width and height (metadata)
        /// - datetime it was taken (metadata)
        /// - location (metadata)
        /// - EXIF data, like aperture, shutterspeed, and iso (metadata)
        ///
        /// If any metadata such as datetime, location and EXIF data of the photo is missing
        /// a UUID generated each time the identifier is generated is added for randomness.
        /// If all the metadata values are present, then photos are equal when
        /// - taken at a specific time (with milliseconds)
        /// - in a specific location (with exact coordinates)
        /// - with the specific settings (width, height, media type, EXIF data)
        ///
        
        var seed = [
            "\(self.phAsset.mediaType.rawValue)",
            "\(self.phAsset.mediaSubtypes.rawValue)",
            "\(self.phAsset.pixelWidth)",
            "\(self.phAsset.pixelHeight)"
        ]
        
        if self.phAsset.creationDate == nil || self.phAsset.location == nil || exifData == nil {
            seed.append(UUID().uuidString)
        } else {
            seed.append(self.phAsset.creationDate!.iso8601withFractionalSeconds)
            seed.append("\(self.phAsset.location!.coordinate.latitude)")
            seed.append("\(self.phAsset.location!.coordinate.longitude)")
            seed.append("\(exifData!.aperture)+\(exifData!.shutterSpeed)+\(exifData!.iso)")
        }
        
        let globalIdentifier = SHHash.stringDigest(for: seed.joined(separator: ":").data(using: .utf8)!)
        self.calculatedGlobalIdentifier = globalIdentifier
        return globalIdentifier
    }
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.phAsset.localIdentifier, forKey: PHAssetIdentifierKey)
        coder.encode(self.calculatedGlobalIdentifier, forKey: CalculatedGlobalIdentifier)
    }
    
}

extension SHApplePhotoAsset {
    
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
        return try await withUnsafeThrowingContinuation { continuation in
            self.phAsset.image(
                forSize: kSHSizeForQuality(quality: SHAssetQuality.lowResolution),
                usingImageManager: self.imageManager,
                synchronousFetch: false,
                resizeMode: .exact
            )
            { result in
                switch result {
                case .success(let nsUIimage):
                    continuation.resume(returning: Self.getControlPixelColor(image: nsUIimage))
                case .failure(let error):
                    continuation.resume(throwing: error)
                }
            }
        }
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


