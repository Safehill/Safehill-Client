import Foundation

internal class ServerUserCache {
    
    static var shared = ServerUserCache()
    
    private var cache = ThreadSafeCache<String, SHRemoteUserClass>()
    
    private init() {}
    
    func user(with identifier: String) -> (any SHServerUser)? {
        if let cacheObj = cache.value(forKey: identifier) as? SHRemoteUserClass {
            return SHRemoteUser(
                identifier: cacheObj.identifier,
                name: cacheObj.name,
                phoneNumber: cacheObj.phoneNumber,
                publicKeyData: cacheObj.publicKeyData,
                publicSignatureData: cacheObj.publicSignatureData
            )
        }
        return nil
    }
    
    func cache(users: [any SHServerUser]) {
        for user in users {
            let cacheObject = SHRemoteUserClass(
                identifier: user.identifier,
                name: user.name,
                phoneNumber: user.phoneNumber,
                publicKeyData: user.publicKeyData,
                publicSignatureData: user.publicSignatureData
            )
            self.cache.set(cacheObject, forKey: user.identifier)
        }
    }
    
    func evict(usersWithIdentifiers userIdentifiers: [String]) {
        userIdentifiers.forEach {
            self.cache.removeValue(forKey: $0)
        }
    }
    
    func clear() {
        self.cache.removeAll()
    }
}
