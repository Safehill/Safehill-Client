import Foundation

public typealias UserIdentifier = String

internal struct ServerUserCache {
    
    static var shared = ServerUserCache()
    
    private var cache = NSCache<NSString, SHRemoteUserClass>()
    private var evictors = [String: Timer]()
    
    private init() {}
    
    func user(with identifier: String) -> (any SHServerUser)? {
        if let cacheObj = cache.object(forKey: NSString(string: identifier)) {
            return SHRemoteUser(
                identifier: cacheObj.identifier,
                name: cacheObj.name,
                publicKeyData: cacheObj.publicKeyData,
                publicSignatureData: cacheObj.publicSignatureData
            )
        }
        return nil
    }
    
    mutating func cache(_ user: any SHServerUser) {
        let cacheObject = SHRemoteUserClass(identifier: user.identifier, name: user.name, publicKeyData: user.publicKeyData, publicSignatureData: user.publicSignatureData)
        cache.setObject(cacheObject, forKey: NSString(string: user.identifier))
        
        evictors[user.identifier]?.invalidate()
        
        // Cache retention policy: TTL = 5 minutes
        evictors[user.identifier] = Timer.scheduledTimer(withTimeInterval: 60 * 5, repeats: false, block: { [self] (timer) in
            cache.removeObject(forKey: NSString(string: user.identifier))
        })
    }
}

public class SHUsersController {
    
    public let localUser: SHLocalUser
    
    init(localUser: SHLocalUser) {
        self.localUser = localUser
    }
    
    private var serverProxy: SHServerProxy {
        SHServerProxy(user: self.localUser)
    }
    
    public func getUsers(withIdentifiers userIdentifiers: [UserIdentifier]) throws -> [SHServerUser] {
        
        var shouldFetch = false
        var users = [any SHServerUser]()
        
        for userIdentifier in userIdentifiers {
            if let user = ServerUserCache.shared.user(with: userIdentifier) {
                users.append(user)
            } else {
                shouldFetch = true
            }
        }
        
        guard shouldFetch else {
            return users
        }
        
        var error: Error? = nil
        let group = DispatchGroup()
        
        group.enter()
        serverProxy.getUsers(
            withIdentifiers: userIdentifiers
        ) { result in
            switch result {
            case .success(let serverUsers):
                users = serverUsers
            case .failure(let err):
                error = err
            }
            group.leave()
        }
        
        let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultNetworkTimeoutInMilliseconds))
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
        guard error == nil else {
            throw error!
        }
        
        for user in users {
            ServerUserCache.shared.cache(user)
        }
        
        return users
    }
}
