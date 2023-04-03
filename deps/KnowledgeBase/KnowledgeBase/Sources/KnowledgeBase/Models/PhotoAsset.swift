//
//  PhotoAsset.swift
//  
//
//  Created by Gennaro Frazzingaro on 8/19/21.
//

import Photos
import Foundation

private let PHAssetIdentifierKey = "phAssetLocalIdentifier"
private let CachedDataKey = "cachedData"
private let CacheUpdatedAtKey = "cacheUpdatedAt"

public class KBPhotoAsset : NSObject, NSSecureCoding {
    public static var supportsSecureCoding = true
    
    var imageManager: PHImageManager
    
    public let phAsset: PHAsset
    public var cachedData: Data?
    public var cacheUpdatedAt: Date?
    
    public init(for asset: PHAsset,
                cachedData: Data? = nil,
                cacheUpdatedAt: Date? = nil,
                usingCachingImageManager manager: PHCachingImageManager? = nil) {
        self.phAsset = asset
        self.cachedData = cachedData
        self.cacheUpdatedAt = cacheUpdatedAt ?? Date()
        self.imageManager = manager ?? PHImageManager.default()
    }
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.phAsset.localIdentifier, forKey: PHAssetIdentifierKey)
        coder.encode(self.cachedData, forKey: CachedDataKey)
        coder.encode(self.cacheUpdatedAt, forKey: CacheUpdatedAtKey)
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let phAssetIdentifier = decoder.decodeObject(of: NSString.self, forKey: PHAssetIdentifierKey)
        let cachedData = decoder.decodeObject(of: NSData.self, forKey: CachedDataKey)
        let cacheUpdatedAt = decoder.decodeObject(of: NSDate.self, forKey: CacheUpdatedAtKey)
        
        guard let phAssetIdentifier = phAssetIdentifier as String? else {
            log.error("unexpected value for phAssetIdentifier when decoding KBPhotoAsset object")
            return nil
        }
        
        let fetchResult = PHAsset.fetchAssets(withLocalIdentifiers: [phAssetIdentifier], options: nil)
        guard fetchResult.count > 0 else {
            return nil
        }
        
        let asset = fetchResult.object(at: 0)
        
        guard let cachedData = cachedData as Data?,
              let cacheUpdatedAt = cacheUpdatedAt as Date? else {
                  self.init(for: asset)
                  return
        }
        self.init(for: asset, cachedData: cachedData, cacheUpdatedAt: cacheUpdatedAt)
    }

}
