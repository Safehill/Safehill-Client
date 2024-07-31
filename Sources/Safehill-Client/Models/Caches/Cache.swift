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

class ThreadSafeCache<Key: Hashable, Value: AnyObject> {
    private let cache = NSCache<WrappedKey, Entry>()
    private let expirationTime: TimeInterval
    private let evictionInterval: TimeInterval
    private var evictionTimer: Timer?
    
    init(
        expirationTime: TimeInterval = 300,
        evictionInterval: TimeInterval = 60
    ) {
        self.expirationTime = expirationTime
        self.evictionInterval = evictionInterval
        startEvictionTimer()
    }
    
    func set(_ value: Value, forKey key: Key) {
        let entry = Entry(value: value, expiration: expirationTime)
        cache.setObject(entry, forKey: WrappedKey(key))
    }
    
    func value(forKey key: Key) -> AnyObject? {
        guard let entry = cache.object(forKey: WrappedKey(key)) else {
            return nil
        }
        
        if entry.isExpired {
            removeValue(forKey: key)
            return nil
        }
        
        return entry.value
    }
    
    func removeValue(forKey key: Key) {
        cache.removeObject(forKey: WrappedKey(key))
    }
    
    func removeAll() {
        for key in cache.allKeys {
            cache.removeObject(forKey: WrappedKey(key))
        }
    }
    
    private func startEvictionTimer() {
        evictionTimer?.invalidate()
        evictionTimer = Timer.scheduledTimer(withTimeInterval: evictionInterval, repeats: true) { [weak self] _ in
            self?.evictExpiredItems()
        }
    }
    
    private func evictExpiredItems() {
        for key in cache.allKeys {
            if let entry = cache.object(forKey: key), entry.isExpired {
                cache.removeObject(forKey: key)
            }
        }
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
