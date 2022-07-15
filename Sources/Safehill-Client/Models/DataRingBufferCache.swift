import Foundation

public final class SHDataRingBufferCache {
    private var cache = [String: Data]()
    private var keys: [String?]
    fileprivate var index = 0
    let readWriteQueue = DispatchQueue(label: "com.gf.enkey.DataRingBufferCache", attributes: .concurrent)
      
    init(count: Int) {
        keys = [String?](repeating: nil, count: count)
    }
    
    public func add(_ data: Data, forAssetId key: String) {
        readWriteQueue.async(flags: .barrier) { [self] in
            let oldKey = keys[index % keys.count]
            keys[index % keys.count] = key
            if let oldKey = oldKey {
                cache.removeValue(forKey: oldKey)
            }
            cache[key] = data
            index += 1
        }
    }
    
    public func removeData(forAssetId key: String) {
        readWriteQueue.async(flags: .barrier) { [self] in
            cache.removeValue(forKey: key)
        }
    }
    
    public func data(forAssetId key: String) -> Data? {
        var cached: Data?
        readWriteQueue.sync {
            cached = cache[key]
        }
        return cached
    }
}

// Custom management of high quality assets (30 max in memory)
// Low quality asset cache is managed by PHCachingImageManager
public var SHLocalPHAssetHighQualityDataCache = SHDataRingBufferCache(count: 30)
