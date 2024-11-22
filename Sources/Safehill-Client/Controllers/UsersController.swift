import Foundation

public typealias UserIdentifier = String

public class SHUsersController {
    
    public let localUser: SHLocalUserProtocol
    
    public init(localUser: SHLocalUserProtocol) {
        self.localUser = localUser
    }
    
    private var serverProxy: SHServerProxy {
        self.localUser.serverProxy
    }
    
    /// Retrieve from either the in-memory or the on-disk cache only.
    /// Best effort to retrieve users **locally** (maybe when there's no connection to get them from the server?)
    /// Doesn't cache the result in memory.
    ///
    /// - Parameter userIdentifiers: the user identifiers to fetch
    /// - Returns: the best effort to retrieve the requested identifiers from cache
    public func getCachedUsers(
        withIdentifiers userIdentifiers: [UserIdentifier]
    ) async throws -> [UserIdentifier: any SHServerUser] {
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
        
        guard missingUserIds.isEmpty == false else {
            return users
        }
        
        return try await withUnsafeThrowingContinuation { continuation in
            serverProxy.getLocalUsers(withIdentifiers: missingUserIds) {
                result in
                switch result {
                case .success(let serverUsers):
                    for serverUser in serverUsers {
                        users[serverUser.identifier] = serverUser
                    }
                    continuation.resume(returning: users)
                case .failure(let err):
                    log.warning("failed to retrieve users from the local server: \(err.localizedDescription)")
                    continuation.resume(throwing: err)
                }
            }
        }
    }
    
    /// Best effort to retrieve users from **either local or remote server**.
    /// The `getUsers` call fails if not all users can be retrieved
    /// (i.e. not all are in local, and the user is not yet authenticated to talk to the remote server).
    ///
    /// - Parameters:
    ///   - identifiers: the user identifiers
    ///   - completionHandler: the map identifier to user
    public func getUsersOrCached(
        with identifiers: [UserIdentifier]
    ) async throws -> [UserIdentifier: any SHServerUser] {
        do {
            return try await self.getUsers(withIdentifiers: identifiers)
        } catch {
            switch error {
            case SHLocalUserError.notAuthenticated:
                break
            default:
                log.warning("[\(type(of: self))] failed fetch users from server, falling back to **best effort** user cache: \(error.localizedDescription)")
            }
            
            var cached = try await self.getCachedUsers(withIdentifiers: identifiers)
            if cached[self.localUser.identifier] == nil {
                cached[self.localUser.identifier] = self.localUser
            }
            return cached
        }
    }
    
    public func getUsers(
        withIdentifiers userIdentifiers: [UserIdentifier]
    ) async throws -> [UserIdentifier: any SHServerUser] {
        try await withUnsafeThrowingContinuation { continuation in
            self.getUsers(withIdentifiers: userIdentifiers) { result in
                continuation.resume(with: result)
            }
        }
    }
    
    /// Retrieve the users from either the in-memory cache or the server.
    /// Fallback to the on-disk cache only if the server returns an error.
    /// Also caches the result in memory.
    ///
    /// - Parameter userIdentifiers: the user identifiers to fetch
    /// - Returns: the users requested or throws an error
    public func getUsers(
        withIdentifiers userIdentifiers: [UserIdentifier],
        completionHandler: @escaping (Result<[UserIdentifier: any SHServerUser], Error>) -> Void
    ) {
        guard userIdentifiers.count > 0 else {
            completionHandler(.success([:]))
            return
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
        
        guard missingUserIds.isEmpty == false else {
            completionHandler(.success(users))
            return
        }
        
        serverProxy.getUsers(
            withIdentifiers: missingUserIds
        ) { result in
            switch result {
            case .success(let serverUsers):
                for serverUser in serverUsers {
                    users[serverUser.identifier] = serverUser
                }
                ServerUserCache.shared.cache(users: Array(users.values))
                completionHandler(.success(users))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    private func deleteUsers(withIdentifiers userIdentifiers: [UserIdentifier]) throws {
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
