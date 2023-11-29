import Foundation

public typealias UserIdentifier = String

internal class ServerUserCache {
    
    private let writeQueue = DispatchQueue(label: "ServerUserCache.write", attributes: .concurrent)
    
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
        writeQueue.async(flags: .barrier) { [weak self] in
            guard let sself = self else {
                return
            }
            for user in users {
                let cacheObject = SHRemoteUserClass(identifier: user.identifier, name: user.name, publicKeyData: user.publicKeyData, publicSignatureData: user.publicSignatureData)
                sself.cache.setObject(cacheObject, forKey: NSString(string: user.identifier))
                sself.evictors[user.identifier]?.invalidate()
                   
                DispatchQueue.main.async { [weak self] in
                    // Cache retention policy: TTL = 2 minutes
                    self?.evictors[user.identifier] = Timer.scheduledTimer(withTimeInterval: 60 * 2, repeats: false, block: { [weak self] (timer) in
                        self?.evict(usersWithIdentifiers: [user.identifier])
                    })
                }
            }
        }
    }
    
    func evict(usersWithIdentifiers userIdentifiers: [String]) {
        writeQueue.async(flags: .barrier) { [weak self] in
            for userIdentifier in userIdentifiers {
                self?.cache.removeObject(forKey: NSString(string: userIdentifier))
                self?.evictors[userIdentifier]?.invalidate()
            }
        }
    }
}

public class SHUsersController {
    
    public let localUser: SHLocalUser
    
    public init(localUser: SHLocalUser) {
        self.localUser = localUser
    }
    
    private var serverProxy: SHServerProxy {
        SHServerProxy(user: self.localUser)
    }
    
    public func getUsers(withIdentifiers userIdentifiers: [UserIdentifier]) throws -> [SHServerUser] {
        
        var shouldFetch = false
        var users = [any SHServerUser]()
        var missingUserIds = [String]()
        
        for userIdentifier in userIdentifiers {
            if let user = ServerUserCache.shared.user(with: userIdentifier) {
                users.append(user)
            } else {
                missingUserIds.append(userIdentifier)
            }
        }
        
        guard missingUserIds.isEmpty == false else {
            return users
        }
        
        var error: Error? = nil
        let group = DispatchGroup()
        
        group.enter()
        serverProxy.getUsers(
            withIdentifiers: missingUserIds
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
        
        ServerUserCache.shared.cache(users: users)
        
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
