import Foundation

public final actor DataRingBufferCache {
    private var cache = [String: Data]()
    private var keys: [String?]
    fileprivate var index = 0
      
    init(count: Int) {
        keys = [String?](repeating: nil, count: count)
    }
    
    public func add(_ data: Data, forAssetId key: String) {
        let oldKey = keys[index % keys.count]
        keys[index % keys.count] = key
        if let oldKey = oldKey {
            cache.removeValue(forKey: oldKey)
        }
        cache[key] = data
        index += 1
    }
    
    public func removeData(forAssetId key: String) {
        cache.removeValue(forKey: key)
    }
    
    public func data(forAssetId key: String) -> Data? {
        return cache[key]
    }
}

///
/// Custom management of PHAsset high quality assets (5 max in memory)
/// Low quality asset cache is managed by PHCachingImageManager
///
public var LocalPHAssetHighQualityDataCache = DataRingBufferCache(count: 5)
///
/// Custom management of SHDecryptedAsset high quality assets (5 max in memory)
/// Low quality asset cache are in the inMemoryCache
///
public var RemoteAssetHighQualityDataCache = DataRingBufferCache(count: 5)
