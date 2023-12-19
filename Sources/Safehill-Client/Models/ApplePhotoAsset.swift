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
    
    internal static var imageRequestOptions: PHImageRequestOptions {
        let options = PHImageRequestOptions()
        options.isSynchronous = true
        options.isNetworkAccessAllowed = true
        options.resizeMode = .none
        return options
    }
    
    internal func setGlobalIdentifier(_ gid: GlobalIdentifier) throws {
        if self.calculatedGlobalIdentifier == nil {
            self.calculatedGlobalIdentifier = gid
        } else {
            if self.calculatedGlobalIdentifier != gid {
                throw SHBackgroundOperationError.fatalError("previously generated global identifier doesn't match the one provided")
            }
        }
    }
    
#if os(iOS)
    
    public init(for asset: PHAsset,
                usingCachingImageManager manager: PHCachingImageManager? = nil) {
        self.phAsset = asset
        self.imageManager = manager ?? PHImageManager.default()
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let phAssetIdentifier = decoder.decodeObject(of: NSString.self, forKey: PHAssetIdentifierKey)
        let calculatedGlobalId = decoder.decodeObject(of: NSString.self, forKey: CalculatedGlobalIdentifier)
        
        guard let phAssetIdentifier = phAssetIdentifier as String? else {
            log.error("unexpected value for phAssetIdentifier when decoding SHApplePhotoAsset object")
            return nil
        }
        
        guard let calculatedGlobalId = calculatedGlobalId as GlobalIdentifier? else {
            log.error("unexpected value for calculatedGlobalIdentifier when decoding SHApplePhotoAsset object")
            return nil
        }
        
        let fetchResult = PHAsset.fetchAssets(withLocalIdentifiers: [phAssetIdentifier], options: nil)
        guard fetchResult.count > 0 else {
            return nil
        }
        
        let asset = fetchResult.object(at: 0)
        self.init(for: asset)
        self.calculatedGlobalIdentifier = calculatedGlobalId
    }
    
    public func fetchOriginalSizeImage() throws -> UIImage {
        var image: UIImage? = nil
        
        PHImageManager().requestImage(
            for: self.phAsset,
            targetSize: PHImageManagerMaximumSize,
            contentMode: .default,
            options: SHApplePhotoAsset.imageRequestOptions
        ) {
            img, _ in
            image = img
        }
        
        guard let image = image else {
            throw SHBackgroundOperationError.applePhotosAssetRetrievalError("could not fetch full size image")
        }
        
        return image
    }
    
    public static func resizeImage(_ image: UIImage, to targetSize: CGSize) throws -> UIImage {
        let rect = CGRect(x: 0, y: 0, width: targetSize.width, height: targetSize.height)
        
        UIGraphicsBeginImageContextWithOptions(targetSize, false, 1.0)
        image.draw(in: rect)
        let newImage = UIGraphicsGetImageFromCurrentImageContext()
        UIGraphicsEndImageContext()
        
        guard let ni = newImage else {
            throw SHBackgroundOperationError.fatalError("Photos returned an image size different than the one requested and resizing failed. A global identifier can't be calculated")
        }
        return ni
    }
    
#else
    
    public init(for asset: PHAsset,
                usingCachingImageManager manager: PHCachingImageManager? = nil) {
        self.phAsset = asset
        self.imageManager = manager ?? PHImageManager.default()
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let phAssetIdentifier = decoder.decodeObject(of: NSString.self, forKey: PHAssetIdentifierKey)
        let calculatedGlobalId = decoder.decodeObject(of: NSString.self, forKey: CalculatedGlobalIdentifier)
        
        guard let phAssetIdentifier = phAssetIdentifier as String? else {
            log.error("unexpected value for phAssetIdentifier when decoding SHApplePhotoAsset object")
            return nil
        }
        
        guard let calculatedGlobalId = calculatedGlobalId as GlobalIdentifier? else {
            log.error("unexpected value for calculatedGlobalIdentifier when decoding SHApplePhotoAsset object")
            return nil
        }
        
        let fetchResult = PHAsset.fetchAssets(withLocalIdentifiers: [phAssetIdentifier], options: nil)
        guard fetchResult.count > 0 else {
            return nil
        }
        
        let asset = fetchResult.object(at: 0)
        self.init(for: asset)
        self.calculatedGlobalIdentifier = calculatedGlobalId
    }
    
    public func fetchOriginalSizeImage() throws -> NSImage {
        var image: NSImage? = nil
        
        PHImageManager().requestImage(
            for: self.phAsset,
            targetSize: PHImageManagerMaximumSize,
            contentMode: .default,
            options: SHApplePhotoAsset.imageRequestOptions
        ) {
            img, _ in
            image = img
        }
        
        guard let image = image else {
            throw SHBackgroundOperationError.applePhotosAssetRetrievalError("could not fetch full size image")
        }
        
        return image
    }
#endif
    
    
    ///
    ///  **Use it carefully!!**
    ///  This operation can take time, as it needs to retrieve the full resolution asset from the Apple Photos library.
    ///
    /// - Returns:
    ///   - a hash representing the image fingerprint, that can be used as a unique global identifier
    func retrieveOrGenerateGlobalIdentifier() throws -> GlobalIdentifier {
        guard calculatedGlobalIdentifier == nil else {
            return calculatedGlobalIdentifier!
        }
        
        let start = CFAbsoluteTimeGetCurrent()
        
        let targetSize: CGSize
        if self.phAsset.pixelWidth > self.phAsset.pixelHeight {
            targetSize = CGSize(width: imageSizeForGlobalIdCalculation.width,
                                height: floor(imageSizeForGlobalIdCalculation.width * Double(self.phAsset.pixelHeight)/Double(self.phAsset.pixelWidth)))
        } else {
            targetSize = CGSize(width: floor(imageSizeForGlobalIdCalculation.height * Double(self.phAsset.pixelWidth)/Double(self.phAsset.pixelHeight)),
                                height: imageSizeForGlobalIdCalculation.height)
        }
        
        let fullSizeImage = try self.fetchOriginalSizeImage()
        
        let cgImage: CGImage?
#if os(iOS)
        let resizedImage = try SHApplePhotoAsset.resizeImage(fullSizeImage, to: targetSize)
        cgImage = resizedImage.cgImage
#else
        var imageRect = CGRect(x: 0, y: 0, width: targetSize.width, height: targetSize.height)
        cgImage = fullSizeImage.cgImage(forProposedRect: &imageRect, context: nil, hints: nil)
#endif
        if let data = cgImage?.dataProvider?.data as? Data {
            let hash = SHHash.stringDigest(for: data)
            
            let end = CFAbsoluteTimeGetCurrent()
            log.debug("[PERF] it took \(CFAbsoluteTime(end - start)) to generate an asset global identifier")
            
            self.calculatedGlobalIdentifier = hash
            return hash
        } else {
            throw SHBackgroundOperationError.unexpectedData(cgImage)
        }
    }
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.phAsset.localIdentifier, forKey: PHAssetIdentifierKey)
        coder.encode(self.calculatedGlobalIdentifier, forKey: CalculatedGlobalIdentifier)
    }
    
}
