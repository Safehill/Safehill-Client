import Photos
import Foundation
#if os(iOS)
import UIKit
#elseif os(macOS)
import AppKit
#endif


private let PHAssetIdentifierKey = "phAssetLocalIdentifier"
private let CachedImageKey = "cachedImage"
private let CacheUpdatedAtKey = "cacheUpdatedAt"

public class SHApplePhotoAsset : NSObject, NSSecureCoding {
    public static var supportsSecureCoding = true
    
    var imageManager: PHImageManager
    
    public let phAsset: PHAsset
    var cachedImage: NSUIImage?
    public var cacheUpdatedAt: Date?
    
#if os(iOS)
    public init(for asset: PHAsset,
                cachedImage: UIImage? = nil,
                cacheUpdatedAt: Date? = nil,
                usingCachingImageManager manager: PHCachingImageManager? = nil) {
        self.phAsset = asset
        if let cachedImage = cachedImage {
            self.cachedImage = .uiKit(cachedImage)
            self.cacheUpdatedAt = cacheUpdatedAt ?? Date()
        } else {
            self.cachedImage = nil
            self.cacheUpdatedAt = nil
        }
        self.imageManager = manager ?? PHImageManager.default()
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let phAssetIdentifier = decoder.decodeObject(of: NSString.self, forKey: PHAssetIdentifierKey)
        let cachedImage = decoder.decodeObject(of: UIImage.self, forKey: CachedImageKey)
        let cacheUpdatedAt = decoder.decodeObject(of: NSDate.self, forKey: CacheUpdatedAtKey)
        
        guard let phAssetIdentifier = phAssetIdentifier as String? else {
            log.error("unexpected value for phAssetIdentifier when decoding SHApplePhotoAsset object")
            return nil
        }
        
        let fetchResult = PHAsset.fetchAssets(withLocalIdentifiers: [phAssetIdentifier], options: nil)
        guard fetchResult.count > 0 else {
            return nil
        }
        
        let asset = fetchResult.object(at: 0)
        
        guard let cachedImage = cachedImage as UIImage?,
              let cacheUpdatedAt = cacheUpdatedAt as Date? else {
                  self.init(for: asset)
                  return
        }
        self.init(for: asset, cachedImage: cachedImage, cacheUpdatedAt: cacheUpdatedAt)
    }
    
    
    public func cachedData(forSize size: CGSize?) throws -> Data? {
        guard let cachedImage = cachedImage else {
            return nil
        }
        guard let size = size else {
            return cachedImage.platformImage.pngData()
        }
        
        return cachedImage.platformImage.resized(to: size)?.pngData()
    }
#endif
    
#if os(macOS)
    public init(for asset: PHAsset,
                cachedImage: NSImage? = nil,
                cacheUpdatedAt: Date? = nil,
                usingCachingImageManager manager: PHCachingImageManager? = nil) {
        self.phAsset = asset
        if let cachedImage = cachedImage {
            self.cachedImage = .appKit(cachedImage)
        } else {
            self.cachedImage = nil
        }
        self.cacheUpdatedAt = cacheUpdatedAt ?? Date()
        self.imageManager = manager ?? PHImageManager.default()
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let phAssetIdentifier = decoder.decodeObject(of: NSString.self, forKey: PHAssetIdentifierKey)
        let cachedImage = decoder.decodeObject(of: NSImage.self, forKey: CachedImageKey)
        let cacheUpdatedAt = decoder.decodeObject(of: NSDate.self, forKey: CacheUpdatedAtKey)
        
        guard let phAssetIdentifier = phAssetIdentifier as String? else {
            log.error("unexpected value for phAssetIdentifier when decoding SHApplePhotoAsset object")
            return nil
        }
        
        let fetchResult = PHAsset.fetchAssets(withLocalIdentifiers: [phAssetIdentifier], options: nil)
        guard fetchResult.count > 0 else {
            return nil
        }
        
        let asset = fetchResult.object(at: 0)
        
        guard let cachedImage = cachedImage as NSImage?,
              let cacheUpdatedAt = cacheUpdatedAt as Date? else {
                  self.init(for: asset)
                  return
        }
        self.init(for: asset, cachedImage: cachedImage, cacheUpdatedAt: cacheUpdatedAt)
    }
    
    
    public func cachedData(forSize size: CGSize?) throws -> Data? {
        guard let cachedImage = cachedImage else {
            return nil
        }
        guard let size = size else {
            return cachedImage.platformImage.png
        }
        
        return cachedImage.platformImage.resized(to: size)?.png
    }
#endif
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.phAsset.localIdentifier, forKey: PHAssetIdentifierKey)
        coder.encode(self.cachedImage?.platformImage, forKey: CachedImageKey)
        coder.encode(self.cacheUpdatedAt, forKey: CacheUpdatedAtKey)
    }
    
}
