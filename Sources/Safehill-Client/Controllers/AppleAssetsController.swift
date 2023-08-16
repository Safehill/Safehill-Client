import Foundation
import Photos
import KnowledgeBase

public let kSHPhotosAssetCacheStoreName = "com.gf.safehill.PhotosAssetCache"
let kSHPhotosAuthorizationStatusKey = "com.gf.safehill.indexer.photos.authorizationStatus"


public protocol SHPhotoAssetChangeDelegate {
    func authorizationChanged()
    func didAddToCameraRoll(assets: [PHAsset])
    func didRemoveFromCameraRoll(assets: [PHAsset])
}

public enum SHPhotosFilter {
    case withLocalIdentifiers([String]), after(Date), before(Date), afterOrOn(Date), beforeOrOn(Date), limit(Int)
}

public class SHPhotosIndexer : NSObject, PHPhotoLibraryChangeObserver {
    
    // TODO: Maybe hashing can be handled better by overriding Hashable/Equatable? That would also make it unnecessarily complex though :(
    public var identifier: String {
        "\(self.hashValue)"
    }
    
    private let photosIndexerDefaults = KBKVStore.userDefaultsStore()!
    private var delegates = [String: SHPhotoAssetChangeDelegate]()
    
    /// The index of `SHApplePhotoAsset`s
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
    
    public var authorizationStatus: PHAuthorizationStatus {
        get {
            do {
                let savedAuthStatus = try self.photosIndexerDefaults.value(for: kSHPhotosAuthorizationStatusKey)
                if let savedAuthStatus = savedAuthStatus as? Int {
                    return PHAuthorizationStatus(rawValue: savedAuthStatus) ?? .notDetermined
                }
            } catch {}
            return .notDetermined
        }
        set {
            do {
                try self.photosIndexerDefaults.set(value: newValue.rawValue,
                                                   for: kSHPhotosAuthorizationStatusKey)
            } catch {
                log.warning("Unable to record kSHPhotosAuthorizationStatusKey status in UserDefaults KBKVStore")
            }
            if [.authorized, .limited].contains(newValue) {
                PHPhotoLibrary.shared().register(self)
            }
        }
    }
    private let ingestionQueue = DispatchQueue(label: "com.gf.knowledgebase.indexer.photos.ingestion", qos: .userInitiated)
    private let processingQueue = DispatchQueue(label: "com.gf.knowledgebase.indexer.photos.processing", qos: .background)
    /// Time-based cache invalidation
    private var cacheInvalidateTimer: Timer? = nil
    
    public init(withIndex index: KBKVStore? = nil) {
        self.index = index
        self.imageManager = PHCachingImageManager()
        self.imageManager.allowsCachingHighQualityImages = false
        super.init()
        self.requestAuthorization { _ in }
    }
    
    public func requestAuthorization(completionHandler: @escaping (PHAuthorizationStatus) -> Void) {
        PHPhotoLibrary.requestAuthorization(for: .readWrite) { status in
            self.authorizationStatus = status
            completionHandler(status)
        }
    }
    
    public func addDelegate<T: SHPhotoAssetChangeDelegate>(_ delegate: T) {
        self.delegates[String(describing: delegate)] = delegate
    }
    public func removeDelegate<T: SHPhotoAssetChangeDelegate>(_ delegate: T) {
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
    
    public static func fetchResult(using filters: [SHPhotosFilter],
                                   completionHandler: @escaping (Swift.Result<PHFetchResult<PHAsset>, Error>) -> ()) {
        // Get all the camera roll photos and videos
        let albumFetchResult = PHAssetCollection.fetchAssetCollections(with: .smartAlbum, subtype: .smartAlbumUserLibrary, options: nil)
        albumFetchResult.enumerateObjects { collection, count, stop in
            let assetsFetchOptions = PHFetchOptions()
            assetsFetchOptions.sortDescriptors = [NSSortDescriptor(key: "creationDate", ascending: false)]
            
            var predicate = SHPhotosIndexer.cameraRollPredicate()
            
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
                case .limit(let limit):
                    assetsFetchOptions.fetchLimit = limit
                }
            }
            
            assetsFetchOptions.predicate = predicate
            completionHandler(.success(PHAsset.fetchAssets(in: collection, options: assetsFetchOptions)))
            return
        }
        
        if albumFetchResult.count == 0 {
            completionHandler(.success(PHFetchResult<PHAsset>()))
        }
    }
    
    /// Fetches one asset from the cache, if available, or from the photo library
    /// - Parameters:
    ///   - localIdentifier: the PHAsset local identifier to search
    ///   - completionHandler: the completion handler
    public func fetchCameraRollAsset(withLocalIdentifier localIdentifier: String,
                                     completionHandler: @escaping (Swift.Result<PHAsset?, Error>) -> ()) {
        var retrievedAsset: PHAsset? = nil
        
        if let previousResult = self.cameraRollFetchResult {
            previousResult.enumerateObjects { phAsset, _, stop in
                if phAsset.localIdentifier == localIdentifier {
                    retrievedAsset = phAsset
                    stop.pointee = true
                }
            }
        }
        
        guard retrievedAsset == nil else {
            completionHandler(.success(retrievedAsset))
            return
        }
        
        let filters: [SHPhotosFilter] = [
            .limit(1),
            .withLocalIdentifiers([localIdentifier])
        ]
        SHPhotosIndexer.fetchResult(using: filters, completionHandler: { result in
            switch result {
            case .success(let fetchResult):
                if fetchResult.count == 1 {
                    fetchResult.enumerateObjects { phAsset, count, stop in
                        retrievedAsset = phAsset
                        stop.pointee = true
                    }
                }
                completionHandler(.success(retrievedAsset))
            case .failure(let error):
                completionHandler(.failure(error))
            }
        })
    }
    
    /// Fetches the latest assets in the Camera Roll using the Photos Framework in the background and returns a `PHFetchResult`.
    /// If an `index` is available, it also stores the`SHApplePhotoAsset`s corresponding to the assets in the fetch result.
    /// The first operation is executed on the`ingestionQueue`, while the latter on the `processingQueue`.
    /// - Parameters:
    ///   - filters: filters to apply to the search
    ///   - completionHandler: the completion handler
    public func fetchCameraRollAssets(withFilters filters: [SHPhotosFilter],
                                      completionHandler: @escaping (Swift.Result<PHFetchResult<PHAsset>?, Error>) -> ()) {
        let refreshCache = {
            (completionHandler: @escaping (Swift.Result<PHFetchResult<PHAsset>?, Error>) -> ()) in
            self.ingestionQueue.async { [weak self] in
                guard let self = self else {
                    return completionHandler(.failure(SHBackgroundOperationError.fatalError("self not available after executing block on the serial queue")))
                }
                
                SHPhotosIndexer.fetchResult(using: filters, completionHandler: { result in
                    switch result {
                    case .success(let fetchResult):
                        self.cameraRollFetchResult = fetchResult
                        
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
                    case .failure(let error):
                        completionHandler(.failure(error))
                    }
                })
            }
        }
        
        ///
        /// Cache full library results and retrieve from the cache when the full library is requested.
        /// Invalidate the cache after 30 seconds of setting it.
        ///
        
        if let previousResult = self.cameraRollFetchResult,
           filters.count == 0
        {
            completionHandler(.success(previousResult))
        } else {
            refreshCache { result in
                if case .success(_) = result {
                    DispatchQueue.main.async { [weak self] in
                        guard let sself = self else {
                            return
                        }
                        /// If a timer is already running, keep the current one
                        guard sself.cacheInvalidateTimer?.isValid ?? false == false else {
                            return
                        }
                        sself.cacheInvalidateTimer?.invalidate()
                        sself.cacheInvalidateTimer = Timer.scheduledTimer(
                            withTimeInterval: 30.0,
                            repeats: false,
                            block: { [weak self] (timer) in
                                guard timer.isValid else {
                                    return
                                }
                                self?.ingestionQueue.async { [weak self] in
                                    self?.cameraRollFetchResult = nil
                                }
                            }
                        )
                    }
                }
                completionHandler(result)
            }
        }
    }
    
    /// Update the cache with the latest fetch result
    /// - Parameters:
    ///   - fetchResult: the fresh Photos fetch result
    private func updateIndex(with fetchResult: PHFetchResult<PHAsset>, completionHandler: @escaping (Result<Void, Error>) -> Void) {
        guard let index = self.index else {
            completionHandler(.failure(SHBackgroundOperationError.fatalError("not supported")))
            return
        }
        
        self.processingQueue.async {
            var cachedAssetIdsToInvalidate = [String]()
            let writeBatch = index.writeBatch()
            
            fetchResult.enumerateObjects { asset, count, stop in
                if let cacheHit = try? index.value(for: asset.localIdentifier) as? SHApplePhotoAsset {
                    if let whenCached = cacheHit.cacheUpdatedAt,
                       whenCached.compare(asset.modificationDate ?? .distantPast) == .orderedAscending {
                        // Asset was modified since cached -> remove from the persistent cache
                        cachedAssetIdsToInvalidate.append(asset.localIdentifier)
                    }
                }
                let kvsAssetValue = SHApplePhotoAsset(for: asset, usingCachingImageManager: self.imageManager)
                writeBatch.set(value: kvsAssetValue, for: asset.localIdentifier)
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
        
        self.processingQueue.async {
            let changeDetails = changeInstance.changeDetails(for: cameraRoll)
            let totalCount = (changeDetails?.insertedObjects.count ?? 0) + (changeDetails?.removedObjects.count ?? 0) + (changeDetails?.changedObjects.count ?? 0)
            if let changeDetails = changeDetails, totalCount > 0 {
                self.cameraRollFetchResult = changeDetails.fetchResultAfterChanges
                let writeBatch = self.index?.writeBatch()
                
                // Inserted
                for asset in changeDetails.insertedObjects {
                    writeBatch?.set(value: SHApplePhotoAsset(for: asset), for: asset.localIdentifier)
                }
                for delegate in self.delegates.values {
                    delegate.didAddToCameraRoll(assets: changeDetails.insertedObjects)
                }
                // Removed
                for asset in changeDetails.removedObjects {
                    writeBatch?.set(value: nil, for: asset.localIdentifier)
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
                log.warning("No changes in camera roll. Assuming authorization changed")
                let _ = self.delegates.map { $0.value.authorizationChanged() }
            }
        }
    }
}

