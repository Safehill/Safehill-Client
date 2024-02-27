import Foundation

public typealias UserIdentifier = String

internal class ServerUserCache {
    
    private let writeQueue = DispatchQueue(label: "ServerUserCache.write")
    
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
    
    func cache(users: [any SHServerUser]) {
        writeQueue.sync {
            for user in users {
                let cacheObject = SHRemoteUserClass(identifier: user.identifier, name: user.name, publicKeyData: user.publicKeyData, publicSignatureData: user.publicSignatureData)
                self.cache.setObject(cacheObject, forKey: NSString(string: user.identifier))
                self.evictors[user.identifier]?.invalidate()
                self.evictors.removeValue(forKey: user.identifier)
            }
            
            DispatchQueue.main.async {
                for (i, user) in users.enumerated() {
                    // Cache retention policy: TTL = 2 minutes
                    self.evictors[user.identifier] = Timer.scheduledTimer(withTimeInterval: TimeInterval(60 * 2 + (i/100)),
                                                                          repeats: false,
                                                                          block: { (timer) in
                        self.writeQueue.sync(flags: .barrier) {
                            self.cache.removeObject(forKey: NSString(string: user.identifier))
                            timer.invalidate()
                        }
                    })
                }
            }
        }
    }
    
    func evict(usersWithIdentifiers userIdentifiers: [String]) {
        writeQueue.sync {
            for userIdentifier in userIdentifiers {
                self.cache.removeObject(forKey: NSString(string: userIdentifier))
                self.evictors[userIdentifier]?.invalidate()
            }
        }
    }
}

public class SHUsersController {
    
    public let localUser: SHLocalUserProtocol
    
    public init(localUser: SHLocalUserProtocol) {
        self.localUser = localUser
    }
    
    private var serverProxy: SHServerProxy {
        self.localUser.serverProxy
    }
    
    /// Retrieve from either the in-memory or the on-disk cache only.
    /// **Best effort** to retrieve users locally (maybe when there's no connection to get them from the server?)
    /// Doesn't cache the result in memory.
    ///
    /// - Parameter userIdentifiers: the user identifiers to fetch
    /// - Returns: the best effort to retrieve the requested identifiers from cache
    public func getCachedUsers(
        withIdentifiers userIdentifiers: [UserIdentifier]
    ) throws -> [UserIdentifier: any SHServerUser] {
        guard userIdentifiers.count > 0 else {
            return [:]
        }
        
        var users = [UserIdentifier: any SHServerUser]()
        var missingUserIds = [UserIdentifier]()
        
        for userIdentifier in Set(userIdentifiers) {
            if let user = ServerUserCache.shared.user(with: userIdentifier) {
                users[userIdentifier] = user
            } else if missingUserIds.contains(userIdentifier) == false {
                missingUserIds.append(userIdentifier)
            }
        }
        
        if missingUserIds.isEmpty {
            return users
        }
        
        let group = DispatchGroup()
        
        group.enter()
        serverProxy.getLocalUsers(withIdentifiers: missingUserIds) {
            result in
            switch result {
            case .success(let serverUsers):
                for serverUser in serverUsers {
                    users[serverUser.identifier] = serverUser
                }
            case .failure(let err):
                log.warning("failed to retrieve users from the local server: \(err.localizedDescription)")
                break
            }
            group.leave()
        }
        
        let _ = group.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds))
        return users
    }
    
    /// Retrieve the users from either the in-memory cache or the server.
    /// Fallback to the on-disk cache only if the server returns an error.
    /// Also caches the result in memory.
    ///
    /// - Parameter userIdentifiers: the user identifiers to fetch
    /// - Returns: the users requested or throws an error
    public func getUsers(
        withIdentifiers userIdentifiers: [UserIdentifier]
    ) throws -> [UserIdentifier: any SHServerUser] {
        
        guard userIdentifiers.count > 0 else {
            return [:]
        }
        
        var users = [UserIdentifier: any SHServerUser]()
        var missingUserIds = [UserIdentifier]()
        
        for userIdentifier in Set(userIdentifiers) {
            if let user = ServerUserCache.shared.user(with: userIdentifier) {
                users[userIdentifier] = user
            } else if missingUserIds.contains(userIdentifier) == false {
                missingUserIds.append(userIdentifier)
            }
        }
        
        if missingUserIds.isEmpty {
            return users
        }
        
        users = [:]
        
        var error: Error? = nil
        let group = DispatchGroup()
        
        group.enter()
        serverProxy.getUsers(
            withIdentifiers: userIdentifiers
        ) { result in
            switch result {
            case .success(let serverUsers):
                for serverUser in serverUsers {
                    users[serverUser.identifier] = serverUser
                }
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
        
        ServerUserCache.shared.cache(users: Array(users.values))
        
        return users
    }
    
    internal func deleteUsers(withIdentifiers userIdentifiers: [UserIdentifier]) throws {
        var error: Error? = nil
        let group = DispatchGroup()
        
        ServerUserCache.shared.evict(usersWithIdentifiers: userIdentifiers)
        
        group.enter()
        serverProxy.localServer.deleteUsers(withIdentifiers: userIdentifiers) { result in
            if case .failure(let err) = result {
                error = err
            }
            group.leave()
        }
        
        group.enter()
        serverProxy.localServer.unshareAll(with: userIdentifiers) { result in
            if case .failure(let err) = result {
                error = err
            }
            group.leave()
        }
        
        let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultNetworkTimeoutInMilliseconds * 2))
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
        guard error == nil else {
            throw error!
        }
    }
}
