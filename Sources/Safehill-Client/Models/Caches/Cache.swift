import Foundation

class CacheItem<Value> {
    let value: Value
    let expirationDate: Date
    
    init(value: Value, expiration: TimeInterval) {
        self.value = value
        self.expirationDate = Date().addingTimeInterval(expiration)
    }
    
    var isExpired: Bool {
        return Date() > expirationDate
    }
}

public class ThreadSafeCache<Key: Hashable, Value: AnyObject> {
    private let cache = NSCache<WrappedKey, Entry>()
    
    /// The time after which the value is considered stale
    private let expirationInterval: TimeInterval
    /// The time after expiration until the value is still in the cache
    /// This is useful to keep expired values in the cache instead of immediately remove them after expiration.
    private let evictionInterval: TimeInterval
    
    /// The timer that runs every 60 seconds to evict items
    private var evictionTimer: Timer?
    
    public init(
        expirationInterval: TimeInterval = 300, // (5 minutes)
        evictionInterval: TimeInterval = 0
    ) {
        self.expirationInterval = expirationInterval
        self.evictionInterval = evictionInterval
        startEvictionTimer()
    }
    
    public func set(_ value: Value, forKey key: Key) {
        let entry = Entry(value: value, expiration: expirationInterval)
        cache.setObject(entry, forKey: WrappedKey(key))
    }
    
    public func value(
        forKey key: Key,
        ignoreExpiration: Bool = false
    ) -> AnyObject? {
        guard let entry = cache.object(forKey: WrappedKey(key)) else {
            return nil
        }
        
        if entry.isExpired {
            if shouldEvictItem(entry) {
                removeValue(forKey: key)
            }
            if !ignoreExpiration {
                return nil
            }
        }
        
        return entry.value
    }
    
    public func removeValue(forKey key: Key) {
        cache.removeObject(forKey: WrappedKey(key))
    }
    
    public func removeAll() {
        for key in cache.allKeys {
            cache.removeObject(forKey: WrappedKey(key))
        }
    }
    
    private func startEvictionTimer() {
        evictionTimer?.invalidate()
        evictionTimer = Timer.scheduledTimer(withTimeInterval: 60, repeats: true) { [weak self] _ in
            self?.evictItems()
        }
    }
    
    private func evictItems() {
        for key in cache.allKeys {
            if let entry = cache.object(forKey: key), entry.isExpired {
                if shouldEvictItem(entry) {
                    cache.removeObject(forKey: key)
                }
            }
        }
    }
    
    private func shouldEvictItem(_ item: CacheItem<AnyObject>) -> Bool {
        let evictionDate = item.expirationDate.addingTimeInterval(self.evictionInterval)
        return Date() > evictionDate
    }
    
    deinit {
        evictionTimer?.invalidate()
    }
}

private extension NSCache where KeyType == WrappedKey, ObjectType == Entry {
    var allKeys: [WrappedKey] {
        return (name.map { $0 }).compactMap { key in
            WrappedKey(key)
        }
    }
}

private class WrappedKey: NSObject {
    let key: AnyHashable
    init(_ key: AnyHashable) { self.key = key }
    override var hash: Int { return key.hashValue }
    override func isEqual(_ object: Any?) -> Bool {
        guard let object = object as? WrappedKey else { return false }
        return object.key == key
    }
}

private class Entry: CacheItem<AnyObject> {}
