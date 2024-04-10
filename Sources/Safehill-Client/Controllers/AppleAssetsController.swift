import Foundation
import Photos
import KnowledgeBase

public let kSHPhotosPreferencesUserDefaults = "com.gf.safehill.PhotosAssetCache"
public let kSHPhotosAssetCacheStoreName = "com.gf.safehill.PhotosAssetCache"
let kSHPhotosAuthorizationStatusKey = "com.gf.safehill.indexer.photos.authorizationStatus"


public protocol SHPhotoAssetChangeDelegate {
    func authorizationChanged()
    func didAddToCameraRoll(assets: [PHAsset])
    func didRemoveFromCameraRoll(assets: [PHAsset])
    func needsToFetchWholeLibrary()
}

public enum SHPhotosFilter {
    case withLocalIdentifiers([String]), after(Date), before(Date), afterOrOn(Date), beforeOrOn(Date), limit(Int)
}

public class SHPhotosIndexer : NSObject, PHPhotoLibraryChangeObserver, PHPhotoLibraryAvailabilityObserver {
    
    // TODO: Maybe hashing can be handled better by overriding Hashable/Equatable? That would also make it unnecessarily complex though :(
    public var identifier: String {
        "\(self.hashValue)"
    }
    
    private let preferencesUserDefaults = UserDefaults(suiteName: kSHPhotosPreferencesUserDefaults)!
    private var delegates = [String: SHPhotoAssetChangeDelegate]()
    
    /// The index of `SHApplePhotoAsset`s
    public let index: KBKVStore?
    public let imageManager: PHCachingImageManager
    
    public var lastFullFetchResult: PHFetchResult<PHAsset>? = nil
    
    public var authorizationStatus: PHAuthorizationStatus {
        get {
            let savedAuthStatus = self.preferencesUserDefaults.value(forKey: kSHPhotosAuthorizationStatusKey)
            if let savedAuthStatus = savedAuthStatus as? Int {
                return PHAuthorizationStatus(rawValue: savedAuthStatus) ?? .notDetermined
            }
            return .notDetermined
        }
        set {
            self.preferencesUserDefaults.set(
                newValue.rawValue,
                forKey: kSHPhotosAuthorizationStatusKey
            )
            if [.authorized, .limited].contains(newValue) {
                PHPhotoLibrary.shared().register(self as PHPhotoLibraryChangeObserver)
                PHPhotoLibrary.shared().register(self as PHPhotoLibraryAvailabilityObserver)
            }
        }
    }
    private let ingestionQueue = DispatchQueue(label: "com.safehill.indexer.photos.ingestion", qos: .userInitiated)
    private let processingQueue = DispatchQueue(label: "com.safehill.indexer.photos.processing", qos: .background)
    private let delegatesQueue = DispatchQueue(label: "com.safehill.indexer.delegates")
    
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
    
    private static func predicate(for filters: [SHPhotosFilter]) -> NSPredicate {
        var predicate = NSPredicate(format: "(mediaType = %d || mediaType = %d)",
                                    PHAssetMediaType.image.rawValue,
                                    PHAssetMediaType.unknown.rawValue)
        
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
            case .limit:
                /// This is a setting of PHFetchOptions
                break
            }
        }
        
        return predicate
    }
    
    private static func fetchResult(
        using filters: [SHPhotosFilter],
        completionHandler: @escaping (Swift.Result<PHFetchResult<PHAsset>, Error>) -> ()
    ) {
        let assetsFetchOptions = PHFetchOptions()
        assetsFetchOptions.sortDescriptors = [NSSortDescriptor(key: "creationDate", ascending: false)]
        
        let limit: Int? = filters.compactMap({
            if case .limit(let limit) = $0 {
                return limit
            }
            return nil
        }).first
        
        if let limit {
            assetsFetchOptions.fetchLimit = limit
        }
        
        assetsFetchOptions.predicate = self.predicate(for: filters)
        completionHandler(.success(PHAsset.fetchAssets(with: .image, options: assetsFetchOptions)))
    }
    
    private static func fetchResultFromCameraRoll(
        using filters: [SHPhotosFilter],
        completionHandler: @escaping (Swift.Result<PHFetchResult<PHAsset>, Error>) -> ()
    ) {
        var fetchResult = PHFetchResult<PHAsset>()
        
        // Get all the camera roll photos and videos
        let albumFetchResult = PHAssetCollection.fetchAssetCollections(with: .smartAlbum, subtype: .smartAlbumUserLibrary, options: nil)
        
        albumFetchResult.enumerateObjects { collection, count, stop in
            let assetsFetchOptions = PHFetchOptions()
            assetsFetchOptions.sortDescriptors = [NSSortDescriptor(key: "creationDate", ascending: false)]
            
            assetsFetchOptions.predicate = self.predicate(for: filters)
            fetchResult = PHAsset.fetchAssets(in: collection, options: assetsFetchOptions)
            stop.pointee = true
        }
        
        completionHandler(.success(fetchResult))
    }
    
    /// Fetches one asset from the cache, if available, or from the photo library
    /// - Parameters:
    ///   - localIdentifier: the PHAsset local identifier to search
    ///   - completionHandler: the completion handler
    public func fetchAsset(withLocalIdentifier localIdentifier: String,
                           completionHandler: @escaping (Swift.Result<PHAsset?, Error>) -> ()) {
        var retrievedAsset: PHAsset? = nil
        
        if let previousResult = self.lastFullFetchResult {
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
    
    /// Fetches all Photo Library assets using the Photos Framework in the background and returns a `PHFetchResult`.
    /// If an `index` is available, it also stores the`SHApplePhotoAsset`s corresponding to the assets in the fetch result.
    /// The first operation is executed on the`ingestionQueue`, while the latter on the `processingQueue`.
    /// - Parameters:
    ///   - filters: filters to apply to the search
    ///   - completionHandler: the completion handler
    public func fetchAllAssets(withFilters filters: [SHPhotosFilter],
                               completionHandler: @escaping (Swift.Result<PHFetchResult<PHAsset>?, Error>) -> ()) {
        self.ingestionQueue.async { [weak self] in
            guard let self = self else {
                return completionHandler(.failure(SHBackgroundOperationError.fatalError("self not available after executing block on the serial queue")))
            }
            
            SHPhotosIndexer.fetchResult(using: filters, completionHandler: { result in
                switch result {
                case .success(let fetchResult):
                    if filters.count == 0 {
                        self.lastFullFetchResult = fetchResult
                    }
                    
                    if let _ = self.index {
                        self.updateIndex(with: fetchResult) { result in
                            switch result {
                            case .success():
                                completionHandler(.success(fetchResult))
                            case .failure(let error):
                                completionHandler(.failure(error))
                            }
                        }
                    } else {
                        completionHandler(.success(fetchResult))
                    }
                case .failure(let error):
                    completionHandler(.failure(error))
                }
            })
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
                let kvsAssetValue = SHApplePhotoAsset(for: asset, usingCachingImageManager: self.imageManager)
                writeBatch.set(value: kvsAssetValue, for: asset.localIdentifier)
                cachedAssetIdsToInvalidate.append(asset.localIdentifier)
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
        guard let lastFetch = self.lastFullFetchResult else {
            log.warning("No assets were ever fetched. Ignoring the change notification")
            return
        }
        
        guard lastFetch.count > 0 else {
            /// If the previous fetch returned 0 results, and we get a notification, it's likely that the authorization status changed.
            /// In fact, this happens on first launch when the user allow photos access for the first time.
            let delegates = self.delegates
            self.delegatesQueue.async {
                delegates.forEach { $0.value.authorizationChanged() }
            }
            return
        }
        
        let changeDetails = changeInstance.changeDetails(for: lastFetch)
        guard let changeDetails = changeDetails else {
            /// If the previous fetch is not empty, and we get a notification with no details about the change
            /// then we ignore it.
            /// It seems like this is happening when a photo is deleted in the app.
            /// Ignoring it guarantees that we don't re-fetch the library on deletion.
            log.warning("Notified about change but no change object. Ignoring the notification")
            return
        }
        
        self.processingQueue.async {
            if changeDetails.hasIncrementalChanges {
                let totalCount = (
                    changeDetails.insertedObjects.count
                    + changeDetails.removedObjects.count
                    + changeDetails.changedObjects.count
                )
                
                guard totalCount > 0 else {
                    log.warning("Notified of changes but no change detected")
                    return
                }
                
                self.lastFullFetchResult = changeDetails.fetchResultAfterChanges
                let writeBatch = self.index?.writeBatch()
                
                // Inserted
                if changeDetails.insertedObjects.count > 0 {
                    for asset in changeDetails.insertedObjects {
                        writeBatch?.set(value: SHApplePhotoAsset(for: asset), for: asset.localIdentifier)
                    }
                    let delegates = self.delegates
                    self.delegatesQueue.async {
                        for delegate in delegates.values {
                            delegate.didAddToCameraRoll(assets: changeDetails.insertedObjects)
                        }
                    }
                }
                // Removed
                if changeDetails.removedObjects.count > 0 {
                    for asset in changeDetails.removedObjects {
                        writeBatch?.set(value: nil, for: asset.localIdentifier)
                    }
                    let delegates = self.delegates
                    self.delegatesQueue.async {
                        for delegate in delegates.values {
                            delegate.didRemoveFromCameraRoll(assets: changeDetails.removedObjects)
                        }
                    }
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
                let delegates = self.delegates
                self.delegatesQueue.async {
                    delegates.forEach { $0.value.needsToFetchWholeLibrary() }
                }
            }
        }
    }
    
    public func photoLibraryDidBecomeUnavailable(_ photoLibrary: PHPhotoLibrary) {
        let delegates = self.delegates
        self.delegatesQueue.async {
            delegates.forEach { $0.value.authorizationChanged() }
        }
    }
}

