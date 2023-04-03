//
//  File.swift
//  
//
//  Created by Gennaro Frazzingaro on 8/10/21.
//

import Foundation
import Photos

public let kKBPhotosAssetCacheStoreName = "com.gf.knowledgebase.PhotosAssetCache"
let kKBPhotosAuthorizationStatusKey = "com.gf.knowledgebase.indexer.photos.authorizationStatus"


public protocol KBPhotoAssetChangeDelegate {
    func authorizationChanged()
    func didAddToCameraRoll(assets: [PHAsset])
    func didRemoveFromCameraRoll(assets: [PHAsset])
}

public enum KBPhotosFilter {
    case withLocalIdentifiers([String]), after(Date), before(Date), afterOrOn(Date), beforeOrOn(Date)
}

public class KBPhotosIndexer : NSObject, PHPhotoLibraryChangeObserver {
    
    // TODO: Maybe hashing can be handled better by overriding Hashable/Equatable? That would also make it unnecessarily complex though :(
    public var identifier: String {
        "\(self.hashValue)"
    }
    
    private let photosIndexerDefaults = KBKVStore.userDefaultsStore()
    private var delegates = [String: KBPhotoAssetChangeDelegate]()
    
    /// The index of `KBPhotoAsset`s
    public let index: KBKVStore?
    public let imageManager: PHCachingImageManager
    
    private var cameraRollFetchResult: PHFetchResult<PHAsset>? = nil
    
    public var indexedAssets: [PHAsset] {
        var indexedAssets = [PHAsset]()
        guard let cameraRollFetchResult = cameraRollFetchResult else {
            return indexedAssets
        }
        cameraRollFetchResult.enumerateObjects { phAsset, _, _ in
            indexedAssets.append(phAsset)
        }
        return indexedAssets
    }
    
    private var authorizationStatus: PHAuthorizationStatus = .notDetermined {
        willSet {
            if newValue == .authorized {
                PHPhotoLibrary.shared().register(self)
            }
        }
    }
    private let ingestionQueue = DispatchQueue(label: "com.gf.knowledgebase.indexer.photos.ingestion", qos: .userInitiated)
    private let processingQueue = DispatchQueue(label: "com.gf.knowledgebase.indexer.photos.processing", qos: .background)
    
    public init(withIndex index: KBKVStore? = nil) {
        self.index = index
        self.imageManager = PHCachingImageManager()
        self.imageManager.allowsCachingHighQualityImages = false
        super.init()
        self.requestAuthorization()
    }
    
    public func requestAuthorization() {
        do {
            let savedAuthStatus = try self.photosIndexerDefaults.value(for: kKBPhotosAuthorizationStatusKey)
            if let savedAuthStatus = savedAuthStatus as? Int {
                self.authorizationStatus = PHAuthorizationStatus(rawValue: savedAuthStatus) ?? .notDetermined
            }
        } catch {}
        
        if [.notDetermined, .denied].contains(self.authorizationStatus) {
            PHPhotoLibrary.requestAuthorization(for: .readWrite) { status in
                self.authorizationStatus = status
                do {
                    try self.photosIndexerDefaults.set(value: status.rawValue, for: kKBPhotosAuthorizationStatusKey)
                } catch {
                    log.warning("Unable to record authorization status in UserDefaults KBKVStore")
                }
            }
        }
    }
    
    public func addDelegate<T: KBPhotoAssetChangeDelegate>(_ delegate: T) {
        self.delegates[String(describing: delegate)] = delegate
    }
    public func removeDelegate<T: KBPhotoAssetChangeDelegate>(_ delegate: T) {
        self.delegates.removeValue(forKey: String(describing: delegate))
    }
    
    private static func cameraRollPredicate() -> NSPredicate {
        return NSPredicate(format: "(mediaType = %d || mediaType = %d) && NOT (mediaSubtype & %d) != 0",
                           PHAssetMediaType.image.rawValue,
                           PHAssetMediaType.image.rawValue,
//                           PHAssetMediaType.video.rawValue,
                           PHAssetMediaSubtype.photoScreenshot.rawValue
        )
    }
    
    /// Fetches the latest assets in the Camera Roll using the Photos Framework in the background and returns a `PHFetchResult`.
    /// If an `index` is available, it also stores the`KBPhotoAsset`s corresponding to the assets in the fetch result.
    /// The first operation is executed on the`ingestionQueue`, while the latter on the `processingQueue`.
    /// - Parameters:
    ///   - filters: filters to apply to the search
    ///   - completionHandler: the completion handler
    public func fetchCameraRollAssets(withFilters filters: [KBPhotosFilter],
                                      completionHandler: @escaping (Swift.Result<PHFetchResult<PHAsset>?, Error>) -> ()) {
        self.ingestionQueue.async { [weak self] in
            guard let self = self else {
                return completionHandler(.failure(KBError.fatalError("self not available after executing block on the serial queue")))
            }
            
            // Get all the camera roll photos and videos
            let albumFetchResult = PHAssetCollection.fetchAssetCollections(with: .smartAlbum, subtype: .smartAlbumUserLibrary, options: nil)
            albumFetchResult.enumerateObjects { collection, count, stop in
                let assetsFetchOptions = PHFetchOptions()
                assetsFetchOptions.sortDescriptors = [NSSortDescriptor(key: "creationDate", ascending: false)]
                
                var predicate = KBPhotosIndexer.cameraRollPredicate()
                
                for filter in filters {
                    switch filter {
                    case .withLocalIdentifiers(let localIdentifiers):
                        if localIdentifiers.count > 0 {
                            let onlyIdsPredicate = NSPredicate(format: "(localIdentifier IN %@)", localIdentifiers)
                            predicate = NSCompoundPredicate(andPredicateWithSubpredicates: [predicate, onlyIdsPredicate])
                        }
                    case .before(let date):
                        let beforePredicate = NSPredicate(format: "creationDate < %@", date as NSDate)
                        predicate = NSCompoundPredicate(andPredicateWithSubpredicates: [predicate, beforePredicate])
                    case .beforeOrOn(let date):
                        let beforePredicate = NSPredicate(format: "creationDate <= %@", date as NSDate)
                        predicate = NSCompoundPredicate(andPredicateWithSubpredicates: [predicate, beforePredicate])
                    case .after(let date):
                        let afterPredicate = NSPredicate(format: "creationDate > %@", date as NSDate)
                        predicate = NSCompoundPredicate(andPredicateWithSubpredicates: [predicate, afterPredicate])
                    case .afterOrOn(let date):
                        let afterPredicate = NSPredicate(format: "creationDate => %@", date as NSDate)
                        predicate = NSCompoundPredicate(andPredicateWithSubpredicates: [predicate, afterPredicate])
                    }
                }
                
                assetsFetchOptions.predicate = predicate
                
                self.cameraRollFetchResult = PHAsset.fetchAssets(in: collection, options: assetsFetchOptions)
                
                if let _ = self.index {
                    self.updateIndex(with: self.cameraRollFetchResult!) { result in
                        switch result {
                        case .success():
                            completionHandler(.success(self.cameraRollFetchResult!))
                        case .failure(let error):
                            completionHandler(.failure(error))
                        }
                    }
                } else {
                    completionHandler(.success(self.cameraRollFetchResult!))
                }
            }
            
            if albumFetchResult.count == 0 {
                self.cameraRollFetchResult = PHFetchResult<PHAsset>()
                completionHandler(.success(nil))
            }
        }
    }
    
    /// Update the cache with the latest fetch result
    /// - Parameters:
    ///   - fetchResult: the fresh Photos fetch result
    private func updateIndex(with fetchResult: PHFetchResult<PHAsset>, completionHandler: @escaping KBActionCompletion) {
        guard let index = self.index else {
            completionHandler(.failure(KBError.notSupported))
            return
        }
        
        self.processingQueue.async {
            var cachedAssetIdsToInvalidate = [String]()
            let writeBatch = index.writeBatch()
            
            fetchResult.enumerateObjects { asset, count, stop in
                if let cacheHit = try? index.value(for: asset.localIdentifier) as? KBPhotoAsset {
                    if let whenCached = cacheHit.cacheUpdatedAt,
                       whenCached.compare(asset.modificationDate ?? .distantPast) == .orderedAscending {
                        // Asset was modified since cached -> remove from the persistent cache
                        cachedAssetIdsToInvalidate.append(asset.localIdentifier)
                    }
                } else {
                    let kvsAssetValue = KBPhotoAsset(for: asset, usingCachingImageManager: self.imageManager)
                    writeBatch.set(value: kvsAssetValue, for: asset.localIdentifier)
                }
            }
            
            do {
                try writeBatch.write()
                try index.removeValues(for: cachedAssetIdsToInvalidate)
                completionHandler(.success(()))
            } catch {
                completionHandler(.failure(error))
            }
        }
    }
    
    
    // MARK: PHPhotoLibraryChangeObserver protocol
    
    public func photoLibraryDidChange(_ changeInstance: PHChange) {
        guard let cameraRoll = self.cameraRollFetchResult else {
            log.warning("No assets were ever fetched. Ignoring change notification")
            return
        }
        
        self.ingestionQueue.async {
            let changeDetails = changeInstance.changeDetails(for: cameraRoll)
            if let changeDetails = changeDetails {
                self.cameraRollFetchResult = changeDetails.fetchResultAfterChanges
                let writeBatch = self.index?.writeBatch()
                
                for asset in changeDetails.insertedObjects {
                    writeBatch?.set(value: KBPhotoAsset(for: asset), for: asset.localIdentifier)
                }
                for delegate in self.delegates.values {
                    delegate.didAddToCameraRoll(assets: changeDetails.insertedObjects)
                }
                for delegate in self.delegates.values {
                    delegate.didRemoveFromCameraRoll(assets: changeDetails.removedObjects)
                }
                
                if let index = self.index {
                    do {
                        try writeBatch!.write()
                        try index.removeValues(for: changeDetails.removedObjects.map { $0.localIdentifier })
                    } catch {
                        log.error("Failed to update cache on library change notification: \(error.localizedDescription)")
                    }
                }
            } else {
                let _ = self.delegates.map { $0.value.authorizationChanged() }
                log.warning("No changes in camera roll")
            }
        }
    }
}

