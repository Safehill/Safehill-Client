import Foundation
import Contacts
import Safehill_Crypto
import CryptoKit

public struct SHServerProxy: SHServerProxyProtocol {
    
    let localServer: SHLocalServerAPI
    let remoteServer: SHRemoteServerAPI
    
    public init(user: SHLocalUserProtocol) {
        self.localServer = LocalServer(requestor: user)
        self.remoteServer = RemoteServer(requestor: user)
    }
    
    // Useful for testing
    internal init(local: SHLocalServerAPI, remote: SHRemoteServerAPI) {
        self.localServer = local
        self.remoteServer = remote
    }
}


extension SHServerProxy {
    
    // MARK: - Migrations
    
    public func runLocalMigrations(
        currentBuild: String?,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        self.localServer.runDataMigrations(
            currentBuild: currentBuild,
            completionHandler: completionHandler
        )
    }
    
}


extension SHServerProxy {
    
    // MARK: - Users & Devices
    
    public func createUser(name: String,
                           completionHandler: @escaping (Result<any SHServerUser, Error>) -> ()) {
        self.localServer.createOrUpdateUser(name: name) { result in
            switch result {
            case .success(let localUser):
                self.remoteServer.createOrUpdateUser(name: name) { result in
                    switch result {
                    case .success:
                        completionHandler(.success(localUser))
                    case .failure(let failure):
                        completionHandler(.failure(failure))
                    }
                }
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    public func sendCodeToUser(countryCode: Int,
                               phoneNumber: Int,
                               code: String,
                               medium: SendCodeToUserRequestDTO.Medium,
                               appName: String,
                               completionHandler: @escaping (Result<Void, Error>) -> ()) {
        self.remoteServer.sendCodeToUser(
            countryCode: countryCode,
            phoneNumber: phoneNumber,
            code: code,
            medium: medium,
            appName: appName,
            completionHandler: completionHandler
        )
    }
    
    public func updateUser(phoneNumber: SHPhoneNumber? = nil,
                           name: String? = nil,
                           forcePhoneNumberLinking: Bool = false,
                           completionHandler: @escaping (Result<any SHServerUser, Error>) -> ()) {
        self.remoteServer.updateUser(
            name: name,
            phoneNumber: phoneNumber,
            forcePhoneNumberLinking: forcePhoneNumberLinking
        ) { result in
            switch result {
            case .success(_):
                self.localServer.updateUser(
                    name: name,
                    phoneNumber: phoneNumber,
                    forcePhoneNumberLinking: false,
                    completionHandler: completionHandler
                )
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    public func updateLocalUser(name: String,
                                completionHandler: @escaping (Result<any SHServerUser, Error>) -> ()) {
        self.localServer.updateUser(
            name: name,
            phoneNumber: nil,
            forcePhoneNumberLinking: false,
            completionHandler: completionHandler
        )
    }
    
    internal func updateLocalUser(_ user: SHRemoteUser,
                                  phoneNumber: SHPhoneNumber,
                                  linkedSystemContact: CNContact,
                                  completionHandler: @escaping (Result<Void, Error>) -> ()) {
        self.localServer.update(user: user, 
                                phoneNumber: phoneNumber,
                                linkedSystemContact: linkedSystemContact,
                                completionHandler: completionHandler)
    }
    
    internal func removeLinkedSystemContact(from users: [SHRemoteUserLinkedToContact],
                                            completionHandler: @escaping (Result<Void, Error>) -> ()) {
        self.localServer.removeLinkedSystemContact(from: users, completionHandler: completionHandler)
    }
    
    public func signIn(clientBuild: String?, completionHandler: @escaping (Result<SHAuthResponse, Error>) -> ()) {
        self.remoteServer.signIn(clientBuild: clientBuild, completionHandler: completionHandler)
    }
    
    ///
    /// Save the users retrieved from the remote server to the local server for a file-based cache.
    /// Having a persistent cache (in addition to the in-memory one,
    /// helps when there is no connectivity or the server can not be reached
    ///
    private func updateLocalUserDB(
        remoteServerUsers serverUsers: [any SHServerUser],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        let group = DispatchGroup()
        
        for serverUserChunk in serverUsers.chunked(into: 10) {
            for serverUser in serverUserChunk {
                group.enter()
                self.localServer.createOrUpdateUser(
                    identifier: serverUser.identifier,
                    name: serverUser.name,
                    publicKeyData: serverUser.publicKeyData,
                    publicSignatureData: serverUser.publicSignatureData
                ) { result in
                    if case .failure(let failure) = result {
                        log.error("failed to create server user in local server: \(failure.localizedDescription)")
                    }
                    group.leave()
                }
            }
            
            usleep(useconds_t(10 * 1000)) // sleep 10ms
        }
        
        group.notify(queue: .global(qos: .background)) {
            completionHandler(.success(()))
        }
    }
    
    public func avatarImage(for user: any SHServerUser) async throws -> Data? {
        if let data = try await self.localServer.avatarImage(for: user) {
            return data
        }
        if let remoteData = try await self.remoteServer.avatarImage(for: user) {
            try await self.localServer.saveAvatarImage(data: remoteData, for: user)
            return remoteData
        }
        return nil
    }
    
    public func saveAvatarImage(data: Data, for user: any SHServerUser) async throws {
        if user.identifier == self.remoteServer.requestor.identifier {
            try await self.remoteServer.saveAvatarImage(data: data, for: user)
        }
        try await self.localServer.saveAvatarImage(data: data, for: user)
    }
    
    public func deleteAvatarImage(for user: any SHServerUser) async throws {
        if user.identifier == self.remoteServer.requestor.identifier {
            try await self.remoteServer.deleteAvatarImage(for: user)
        }
        try await self.localServer.deleteAvatarImage(for: user)
    }
    
    public func getUsers(
        withIdentifiers userIdentifiersToFetch: [UserIdentifier]?,
        completionHandler: @escaping (Result<[any SHServerUser], Error>) -> ()
    ) {
        guard userIdentifiersToFetch == nil || userIdentifiersToFetch!.count > 0 else {
            return completionHandler(.success([]))
        }
        
        self.remoteServer.getUsers(withIdentifiers: userIdentifiersToFetch) { result in
            switch result {
            case .success(let serverUsers):
                completionHandler(.success(serverUsers))
                
                guard serverUsers.isEmpty == false else {
                    return
                }
                
                ///
                /// Save them also to the local server for a file-based cache.
                /// Having a persistent cache (in addition to the in-memory one,
                /// helps when there is no connectivity or the server can not be reached
                ///
                DispatchQueue.global(qos: .background).async {
                    self.updateLocalUserDB(remoteServerUsers: serverUsers) { updateResult in
                        switch updateResult {
                        case .failure(let failure):
                            log.error("failed to store server users in local server: \(failure.localizedDescription)")
                        case .success:
                            break
                        }
                    }
                }
                
            case .failure(let err):
                var shouldFetchFromLocal = false /// Only try to fetch users from the local DB …
                switch err {
                case is URLError: /// when a connection to the server could not be established
                    shouldFetchFromLocal = true
                case is SHHTTPError.TransportError:
                    shouldFetchFromLocal = true
                case SHLocalUserError.notAuthenticated: /// when the user is not yet authenticated
                    shouldFetchFromLocal = true
                default:
                    break
                }
                
                if shouldFetchFromLocal {
                    ///
                    /// If can't get from the server because of a connection issue
                    /// try to get them from the local cache
                    ///
                    self.localServer.getUsers(withIdentifiers: userIdentifiersToFetch) { localResult in
                        switch localResult {
                        case .success(let serverUsers):
                            if userIdentifiersToFetch != nil,
                               serverUsers.count == userIdentifiersToFetch!.count {
                                completionHandler(localResult)
                            } else {
                                ///
                                /// If you can't get them all throw an error
                                ///
                                completionHandler(.failure(err))
                            }
                        case .failure(_):
                            completionHandler(.failure(err))
                        }
                    }
                } else {
                    completionHandler(.failure(err))
                }
            }
        }
    }
    
    public func getLocalUsers(
        withIdentifiers userIdentifiersToFetch: [UserIdentifier]?,
        completionHandler: @escaping (Result<[any SHServerUser], Error>) -> ()
    ) {
        guard userIdentifiersToFetch == nil || userIdentifiersToFetch!.count > 0 else {
            return completionHandler(.success([]))
        }
        
        self.localServer.getUsers(
            withIdentifiers: userIdentifiersToFetch,
            completionHandler: completionHandler
        )
    }
    
    public func getUsers(
        withHashedPhoneNumbers hashedPhoneNumbers: [String],
        completionHandler: @escaping (Result<[String: any SHServerUser], Error>) -> ()
    ) {
        self.remoteServer.getUsers(withHashedPhoneNumbers: hashedPhoneNumbers, completionHandler: completionHandler)
    }
    
    func getUsers(
        inAssetDescriptors descriptors: [any SHAssetDescriptor],
        completionHandler: @escaping (Result<[any SHServerUser], Error>) -> Void
    ) {
        var userIdsSet = Set<String>()
        for descriptor in descriptors {
            userIdsSet.insert(descriptor.sharingInfo.sharedByUserIdentifier)
            descriptor.sharingInfo.groupIdsByRecipientUserIdentifier.keys.forEach({ userIdsSet.insert($0) })
        }
        userIdsSet.remove(self.remoteServer.requestor.identifier)
        let userIds = Array(userIdsSet)

        self.getUsers(withIdentifiers: userIds) { result in
            switch result {
            case .success(let serverUsers):
                completionHandler(.success(serverUsers))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    public func searchUsers(query: String, completionHandler: @escaping (Result<[any SHServerUser], Error>) -> ()) {
        self.remoteServer.searchUsers(query: query, completionHandler: completionHandler)
    }
    
    /// Fetch the local user details. If fails fall back to local cache if the server is unreachable or the token is expired
    /// - Parameters:
    ///   - completionHandler: the callback method
    public func fetchUserAccount(completionHandler: @escaping (Result<any SHServerUser, Error>) -> ()) {
        self.fetchRemoteUserAccount { result in
            switch result {
            case .success(let user):
                completionHandler(.success(user))
                
                ///
                /// Save it also to the local server for a file-based cache.
                /// Having a persistent cache (in addition to the in-memory one,
                /// helps when there is no connectivity or the server can not be reached
                ///
                DispatchQueue.global(qos: .background).async {
                    self.updateLocalUserDB(remoteServerUsers: [user]) { result in
                        switch result {
                        case .failure(let failure):
                            log.error("failed to store this user's server user in local server: \(failure.localizedDescription)")
                        case .success:
                            break
                        }
                    }
                }
            case .failure(let err):
                if err is URLError || err is SHHTTPError.TransportError {
                    /// 
                    /// Can't connect to the server, get details from local cache
                    ///
                    log.error("failed to get user details from server. Using local cache. Error=\(err)")
                    self.fetchLocalUserAccount(
                        originalServerError: err,
                        completionHandler: completionHandler
                    )
                } else {
                    completionHandler(.failure(err))
                }
            }
        }
    }
    
    public func fetchRemoteUserAccount(completionHandler: @escaping (Result<any SHServerUser, Error>) -> ()) {
        self.remoteServer.getUsers(withIdentifiers: [self.remoteServer.requestor.identifier]) { result in
            switch result {
            case .success(let users):
                guard users.count == 1 else {
                    completionHandler(.failure(SHHTTPError.ServerError.unexpectedResponse("Sever sent a 200 response to user fetch with \(users.count) users")))
                    return
                }
                completionHandler(.success(users.first!))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    private func fetchLocalUserAccount(originalServerError: Error? = nil,
                                      completionHandler: @escaping (Result<any SHServerUser, Error>) -> ()) {
        self.localServer.getUsers(withIdentifiers: [self.remoteServer.requestor.identifier]) { result in
            switch result {
            case .success(let users):
                guard users.count == 0 || users.count == 1 else {
                    completionHandler(.failure(SHHTTPError.ServerError.unexpectedResponse("Local server retrieved more than one (\(users.count)) self user")))
                    return
                }
                guard let user = users.first else {
                    /// Mimic server behavior on not found
                    completionHandler(.failure(SHHTTPError.ClientError.notFound))
                    return
                }
                completionHandler(.success(user))
            case .failure(let err):
                completionHandler(.failure(originalServerError ?? err))
            }
        }
    }
    
    public func deleteAccount(force: Bool = false, completionHandler: @escaping (Result<Void, Error>) -> ()) {
        self.remoteServer.deleteAccount { result in
            if force == false, case .failure(let err) = result {
                completionHandler(.failure(err))
                return
            }
            self.localServer.deleteAccount(completionHandler: completionHandler)
        }
    }
    
    public func deleteLocalAccount(completionHandler: @escaping (Result<Void, Error>) -> ()) {
        self.localServer.deleteAccount(completionHandler: completionHandler)
    }
    
    public func deleteAccount(name: String, password: String, completionHandler: @escaping (Result<Void, Error>) -> ()) {
        self.remoteServer.deleteAccount(name: name, password: password) { result in
            if case .failure(let err) = result {
                completionHandler(.failure(err))
                return
            }
            self.localServer.deleteAccount(completionHandler: completionHandler)
        }
    }
    
    public func registerDevice(
        _ deviceId: String,
        token: String?,
        appBundleId: String? = nil,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        self.remoteServer.registerDevice(
            deviceId,
            token: token,
            appBundleId: appBundleId,
            completionHandler: completionHandler
        )
    }
}

extension SHServerProxy {
    
    // MARK: - User Connections / Authorizations
    
    public func authorizeUsers(
        with userPublicIdentifiers: [String],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        self.remoteServer.authorizeUsers(with: userPublicIdentifiers, completionHandler: completionHandler)
    }
    
    public func blockUsers(
        with userPublicIdentifiers: [String],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        self.remoteServer.blockUsers(with: userPublicIdentifiers, completionHandler: completionHandler)
    }
    
    public func pendingOrBlockedUsers(
        completionHandler: @escaping (Result<UserAuthorizationStatusDTO, Error>) -> ()
    ) {
        self.remoteServer.pendingOrBlockedUsers(completionHandler: completionHandler)
    }
}

extension SHServerProxy {
    
    // MARK: - Assets
    
    public func getCurrentUsage(
        completionHandler: @escaping (Result<Int, Error>) -> ()
    ) {
        self.remoteServer.countUploaded() { result in
            switch result {
            case .success(let count):
                completionHandler(.success(count))
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    public func getAssetDescriptor(
        for globalIdentifier: GlobalIdentifier,
        filteringGroups: [String]? = nil,
        completionHandler: @escaping (Result<(any SHAssetDescriptor)?, Error>) -> ()
    ) {
        self.getLocalAssetDescriptors(
            for: [globalIdentifier],
            after: nil,
            filteringGroups: filteringGroups
        ) { result in
            switch result {
            case .failure(let err):
                log.warning("no local asset descriptor for asset \(globalIdentifier) in local server. Trying remote. \(err.localizedDescription)")
                self.getRemoteAssetDescriptors(
                    for: [globalIdentifier],
                    after: nil
                ) { remoteResult in
                    switch remoteResult {
                    case .success(let descriptors):
                        completionHandler(.success(descriptors.first))
                    case .failure(let error):
                        completionHandler(.failure(error))
                    }
                }
            case .success(let descriptors):
                if let descriptor = descriptors.first {
                    completionHandler(.success(descriptor))
                } else {
                    self.getRemoteAssetDescriptors(
                        for: [globalIdentifier],
                        after: nil
                    ) { remoteResult in
                        switch remoteResult {
                        case .success(let descriptors):
                            completionHandler(.success(descriptors.first))
                        case .failure(let error):
                            completionHandler(.failure(error))
                        }
                    }
                }
            }
        }
    }
    
    /// Retrieve asset descriptors from the local server
    ///
    /// - Parameters:
    ///   - globalIdentifiers: `nil` or empty means all, otherwise filter assets with these identifiers
    ///   - after: only retrieve assets added after a certain date
    ///   - filteringGroups: only retrieve information about these groups
    ///   - useCache: whether or not they should be retrieved from the cache if available. Note that when requesting all descriptors (`globalIdentifiers` is `nil` or empty) this parameter is ignored
    ///   - completionHandler: the callback
    func getLocalAssetDescriptors(
        for globalIdentifiers: [GlobalIdentifier]? = nil,
        after: Date? = nil,
        filteringGroups: [String]? = nil,
        useCache: Bool = false,
        completionHandler: @escaping (Result<[any SHAssetDescriptor], Error>) -> ()
    ) {
        self.localServer.getAssetDescriptors(
            forAssetGlobalIdentifiers: globalIdentifiers ?? [],
            filteringGroupIds: filteringGroups,
            after: after,
            useCache: useCache
        ) { result in
            switch result {
            case .failure(let err):
                completionHandler(.failure(err))
            case .success(_):
                completionHandler(result)
            }
        }
    }
    
    /// Get all visible asset descriptors to this user. Fall back to local descriptor if server is unreachable
    /// - Parameter completionHandler: the callback method
    func getRemoteAssetDescriptors(
        for globalIdentifiers: [GlobalIdentifier]? = nil,
        after: Date?,
        filteringGroups: [String]? = nil,
        completionHandler: @escaping (Result<[any SHAssetDescriptor], Error>) -> ()
    ) {
        let handleServerResult = { (serverResult: Result<[any SHAssetDescriptor], Error>) in
            switch serverResult {
            case .failure(let serverError):
                completionHandler(.failure(serverError))
            case .success(let descriptors):
                completionHandler(.success(descriptors))
            }
        }
        
        if let globalIdentifiers {
            self.remoteServer.getAssetDescriptors(
                forAssetGlobalIdentifiers: globalIdentifiers,
                filteringGroupIds: filteringGroups,
                after: after
            ) {
                handleServerResult($0)
            }
        } else {
            self.remoteServer.getAssetDescriptors(after: after) {
                handleServerResult($0)
            }
        }
    }
    
    ///
    /// Retrieve asset from local server (cache) if available.
    /// Do not try to fetch from remote server if such assets don't exist on local.
    ///
    /// - Parameters:
    ///   - assetIdentifiers: the global identifier of the asset to retrieve
    ///   - versions: the versions to retrieve
    ///   - completionHandler: the callback method returning the encrypted assets keyed by global id, or the error
    internal func getLocalAssets(
        withGlobalIdentifiers assetIdentifiers: [String],
        versions: [SHAssetQuality],
        completionHandler: @escaping (Result<[String: any SHEncryptedAsset], Error>) -> ()
    ) {
        self.localServer.getAssets(
            withGlobalIdentifiers: assetIdentifiers,
            versions: versions,
            completionHandler: completionHandler
        )
    }
    
    ///
    /// Retrieves assets versions with given identifiers.
    /// Tries to fetch from local server first, then remote server if some are not present.
    /// For those not available in the local server, **it updates the local server (cache)**
    ///
    /// - Parameters:
    ///   - assetIdentifiers: the global asset identifiers to retrieve
    ///   - versions: filter asset version (retrieve just the low res asset or the hi res asset, for instance)
    ///   - synchronousFetch: whether or not we want all assets versions to be available before calling the callback (set to `true`), or have the callback called multiple times as asset versions are available (set to `false`)
    ///   - completionHandler: the callback, returning the `SHEncryptedAsset` objects keyed by asset identifier. Note that the output object might not have the same number of assets requested, as some of them might be deleted on the server
    ///
    func getAssetsAndCache(
        withGlobalIdentifiers assetIdentifiers: [GlobalIdentifier],
        versions requestedVersions: [SHAssetQuality],
        synchronousFetch: Bool = true,
        completionHandler: @escaping (Result<[GlobalIdentifier: any SHEncryptedAsset], Error>) -> ()
    ) {
        guard !assetIdentifiers.isEmpty else {
            completionHandler(.success([:]))
            return
        }
        
        self.getLocalAssets(
            withGlobalIdentifiers: assetIdentifiers,
            versions: requestedVersions
        ) { localResult in
            switch localResult {
            case .failure(let error):
                log.error("[asset-data] failed to get local assets: \(error.localizedDescription)")
                completionHandler(.failure(error))

            case .success(let localDictionary):
                Task {
                    let threadSafeToFetch = ThreadSafeDictionary<GlobalIdentifier, [SHAssetQuality]>()

                    ///
                    /// Collect assets that weren't available on local,
                    /// and need to be fetched from remote
                    ///
                    let assetIdsMissing = Set(assetIdentifiers).subtracting(localDictionary.keys)
                    for assetId in assetIdsMissing {
                        await threadSafeToFetch.set(requestedVersions, for: assetId)
                    }

                    ///
                    /// Include assets whose requested versions need to be fetched from remote
                    ///
                    for (assetId, asset) in localDictionary {
                        let missingVersions = Set(requestedVersions).subtracting(asset.encryptedVersions.keys)
                        if !missingVersions.isEmpty {
                            await threadSafeToFetch.set(Array(missingVersions), for: assetId)
                        }
                    }

                    if await threadSafeToFetch.count() == 0 {
                        log.debug("[asset-data] Cache hit for all requested assets.")
                        completionHandler(.success(localDictionary))
                        return
                    }

                    var intermediateResultReturned = false
                    ///
                    /// For asynchronous fetch, return the intermediate result from local, unless it's empty
                    ///
                    if !synchronousFetch, !localDictionary.isEmpty {
                        completionHandler(.success(localDictionary))
                        intermediateResultReturned = true
                    }

                    self.remoteServer.getAssetDescriptors(
                        forAssetGlobalIdentifiers: await threadSafeToFetch.allKeys(),
                        filteringGroupIds: nil,
                        after: nil
                    ) { descriptorResult in
                        switch descriptorResult {
                            
                        case .failure(let error):
                            log.error("[asset-data] Failed to fetch descriptors: \(error.localizedDescription)")
                            if !intermediateResultReturned {
                                completionHandler(.success(localDictionary))
                            }
                            
                        case .success(let descriptors):
                            Task {
                                let descriptorMap = Dictionary(
                                    uniqueKeysWithValues: descriptors.map { ($0.globalIdentifier, $0) }
                                )

                                ///
                                /// Reset the descriptors to fetch based on the server descriptors.
                                /// Remove the ones that don't have a server descriptor
                                ///
                                let currentKeys = await threadSafeToFetch.allKeys()
                                for key in currentKeys {
                                    if descriptorMap[key] == nil {
                                        await threadSafeToFetch.removeValue(forKey: key)
                                    }
                                }

                                let toFetch = await threadSafeToFetch.allKeyValues()
                                if toFetch.isEmpty {
                                    if !intermediateResultReturned {
                                        completionHandler(.success(localDictionary))
                                    }
                                    return
                                }

                                let threadSafeRemoteOnly = ThreadSafeDictionary<GlobalIdentifier, any SHEncryptedAsset>()

                                for (assetId, versions) in toFetch {
                                    do {
                                        let remoteAssets = try await self.getRemoteAssets(
                                            withGlobalIdentifiers: [assetId],
                                            versions: versions
                                        )

                                        if let fetched = remoteAssets[assetId] {
                                            await threadSafeRemoteOnly.set(fetched, for: assetId)

                                        } else {
                                            log.warning("[asset-data] Remote fetch missing for \(assetId) versions \(versions)")
                                        }

                                    } catch {
                                        log.warning("[asset-data] Remote fetch failed for \(assetId) versions \(versions): \(error.localizedDescription)")
                                    }
                                }

                                let remoteOnlyAssets = await threadSafeRemoteOnly.allKeyValues()

                                if !remoteOnlyAssets.values.isEmpty {
                                    ///
                                    /// CACHE only remote assets
                                    ///
                                    Task.detached(priority: .utility) {
                                        self.localServer.create(
                                            assets: Array(remoteOnlyAssets.values),
                                            descriptorsByGlobalIdentifier: descriptorMap,
                                            uploadState: .completed,
                                            overwriteFileIfExists: true
                                        ) { localCachingResult in
                                            if case .failure(let error) = localCachingResult {
                                                log.warning("[asset-data] caching failed for \(remoteOnlyAssets): \(error.localizedDescription)")
                                            }
                                        }
                                    }
                                }
                                
                                ///
                                /// Combine local + remote for return
                                ///
                                var finalAssets = localDictionary
                                for (id, asset) in remoteOnlyAssets {
                                    finalAssets[id] = asset
                                }

                                completionHandler(.success(finalAssets))
                            }
                        }
                    }
                }
            }
        }
    }

    
    func getRemoteAssets(
        withGlobalIdentifiers assetIdentifiers: [GlobalIdentifier],
        versions requestedVersions: [SHAssetQuality]
    ) async throws -> [GlobalIdentifier: any SHEncryptedAsset]
    {
        try await withUnsafeThrowingContinuation { continuation in
            self.remoteServer.getAssets(
                withGlobalIdentifiers: assetIdentifiers,
                versions: requestedVersions
            ) {
                result in
                switch result {
                case .success(let dict):
                    continuation.resume(returning: dict)
                case .failure(let error):
                    continuation.resume(throwing: error)
                }
            }
        }
    }
    
    func upload(
        serverAsset: SHServerAsset,
        asset: any SHEncryptedAsset,
        filterVersions: [SHAssetQuality]? = nil
    ) async throws {
        
        let manifest = try self.serverAssetVersionsURLDataManifest(
            serverAsset,
            asset: asset,
            filterVersions: filterVersions
        )
        
        try await withUnsafeThrowingContinuation { continuation in
            self.remoteServer.uploadAsset(
                with: serverAsset.globalIdentifier,
                versionsDataManifest: manifest
            ) {
                result in
                switch result {
                case .success:
                    self.localServer.uploadAsset(
                        with: serverAsset.globalIdentifier,
                        versionsDataManifest: manifest
                    ) { result in
                        if case .failure(let localError) = result {
                            log.warning("asset was uploaded on remote server, but local asset wasn't marked as completed. This inconsistency will be resolved by assets sync. \(localError.localizedDescription)")
                        }
                    }
                    
                    continuation.resume(returning: ())
                    
                case .failure(let error):
                    log.error("asset could not be uploaded to remote server: \(error.localizedDescription)")
                    
                    continuation.resume(throwing: error)
                }
            }
        }
    }
    
    private func serverAssetVersionsURLDataManifest(
        _ serverAsset: SHServerAsset,
        asset: any SHEncryptedAsset,
        filterVersions: [SHAssetQuality]?
    ) throws -> [SHAssetQuality: (URL, Data)] {
        var manifest = [SHAssetQuality: (URL, Data)]()
        
        for serverAssetVersion in serverAsset.versions {
            guard let serverAssetVersionQuality = SHAssetQuality(rawValue: serverAssetVersion.versionName) else {
                throw SHHTTPError.ServerError.unexpectedResponse("Unknown version enum \(serverAssetVersion.versionName)")
            }
            guard filterVersions == nil || filterVersions!.contains(serverAssetVersionQuality) else {
                continue
            }
            
            log.info("[S3] uploading to CDN asset version \(serverAssetVersion.versionName) for asset \(serverAsset.globalIdentifier) (localId=\(serverAsset.localIdentifier ?? ""))")
            
            guard let encryptedVersion = asset.encryptedVersions[serverAssetVersionQuality] else {
                throw SHHTTPError.ServerError.unexpectedResponse("invalid upload payload. Mismatched local and server asset versions. server=\(serverAsset), local=\(asset)")
            }
            
            guard let url = URL(string: serverAssetVersion.presignedURL) else {
                throw SHHTTPError.ServerError.unexpectedResponse("[S3] presigned URL is invalid")
            }
            
            if encryptedVersion.encryptedSecret == serverAssetVersion.encryptedSecret,
               encryptedVersion.publicKeyData == serverAssetVersion.publicKeyData,
               encryptedVersion.publicSignatureData == serverAssetVersion.publicSignatureData {
                manifest[serverAssetVersionQuality] = (url, encryptedVersion.encryptedData)
            } else {
                let decyptedAssetVersion = try self.remoteServer.requestor.decrypt(
                    asset,
                    versions: [serverAssetVersionQuality],
                    receivedFrom: self.remoteServer.requestor
                ).decryptedVersions[serverAssetVersionQuality]
                
                guard let decyptedAssetVersion else {
                    throw SHHTTPError.ClientError.badRequest("Encrypted asset provided could not be decrypted by self")
                }
                
                let encryptedSecret = SHShareablePayload(
                    ephemeralPublicKeyData: serverAssetVersion.publicKeyData,
                    cyphertext: serverAssetVersion.encryptedSecret,
                    signature: serverAssetVersion.publicSignatureData
                )
                let privateSecretData = try SHUserContext(user: self.remoteServer.requestor.shUser)
                    .decryptSecret(
                        usingEncryptedSecret: encryptedSecret,
                        protocolSalt: self.remoteServer.requestor.maybeEncryptionProtocolSalt!,
                        signedWith: self.remoteServer.requestor.publicSignatureData
                    )
                
                let privateSecret = try SymmetricKey(rawRepresentation: privateSecretData)
                
                let encryptedData = try SHEncryptedData(
                    privateSecret: privateSecret,
                    clearData: decyptedAssetVersion
                )
                
                manifest[serverAssetVersionQuality] = (url, encryptedData.encryptedData)
            }
        }
        
        return manifest
    }
    
    public func deleteAssets(withGlobalIdentifiers globalIdentifiers: [GlobalIdentifier],
                             completionHandler: @escaping (Result<[GlobalIdentifier], Error>) -> ()) {
        self.remoteServer.deleteAssets(withGlobalIdentifiers: globalIdentifiers) { result in
            switch result {
            case .success(_):
                self.localServer.deleteAssets(withGlobalIdentifiers: globalIdentifiers) { result in
                    if case .failure(let err) = result {
                        log.critical("asset was deleted on server but not from the local cache. As the two servers are out of sync this can fail other operations downstream. error=\(err.localizedDescription)")
                    }
                }
                completionHandler(result)
                
            case .failure(let err):
                log.error("asset deletion failed. Error: \(err.localizedDescription)")
                completionHandler(.failure(err))
            }
        }
    }
    
    public func deleteAllLocalAssets(completionHandler: @escaping (Result<[String], Error>) -> ()) {
        self.localServer.deleteAllAssets(completionHandler: completionHandler)
    }
    
    func shareAssetLocally(
        _ asset: SHShareableEncryptedAsset,
        asPhotoMessageInThreadId: String?,
        permissions: Int?,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        self.localServer.share(
            asset: asset,
            asPhotoMessageInThreadId: asPhotoMessageInThreadId,
            permissions: permissions,
            suppressNotification: true
        ) {
            result in
            switch result {
            case .success(_):
                completionHandler(.success(()))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    func share(_ asset: SHShareableEncryptedAsset,
               asPhotoMessageInThreadId: String?,
               permissions: Int?,
               suppressNotification: Bool = false,
               completionHandler: @escaping (Result<Void, Error>) -> ()) {
        self.remoteServer.share(
            asset: asset,
            asPhotoMessageInThreadId: asPhotoMessageInThreadId,
            permissions: permissions,
            suppressNotification: suppressNotification,
            completionHandler: completionHandler
        )
    }
    
    public func unshare(
        assetIdsWithUsers: [GlobalIdentifier: [UserIdentifier]],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        let userIdsReferenced = Set(assetIdsWithUsers.values.flatMap({ $0 }))
        guard userIdsReferenced.contains(self.remoteServer.requestor.identifier) == false else {
            completionHandler(.failure(SHHTTPError.ClientError.badRequest("unshare should not be used for the requestor user, aka the owner of the asset. Use delete instead")))
            return
        }
        
        self.remoteServer.unshare(assetIdsWithUsers: assetIdsWithUsers) {
            remoteResult in
            switch remoteResult {
            case .success:
                self.localServer.unshare(assetIdsWithUsers: assetIdsWithUsers) { localResult in
                    if case .failure(let failure) = localResult {
                        log.warning("failed to unshare locally after successfully sharing remotely \(assetIdsWithUsers): \(failure.localizedDescription)")
                    }
                    completionHandler(.success(()))
                }
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    public func changeGroupPermission(
        groupId: String,
        permission: Int,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        self.remoteServer.changeGroupPermission(groupId: groupId, permission: permission) {
            remoteResult in
            switch remoteResult {
            case .success:
                self.localServer.changeGroupPermission(groupId: groupId, permission: permission) {
                    localResult in
                    if case .failure(let failure) = localResult {
                        log.warning("failed to change permissions locally after successfully changing them remotely \(groupId) \(permission): \(failure.localizedDescription)")
                    }
                    completionHandler(.success(()))
                }
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
}


extension SHServerProxy {
    
    // - MARK: Groups, Threads and Interactions
    
    func setupGroup(
        groupId: String,
        encryptedTitle: String?,
        recipientsEncryptionDetails: [RecipientEncryptionDetailsDTO],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        log.trace("saving encryption details for group \(groupId) to local server")
        log.debug("[setupGroup] \(recipientsEncryptionDetails.map({ ($0.encryptedSecret, $0.ephemeralPublicKey, $0.secretPublicSignature) }))")
        /// Save the encryption details for this user on local
        self.remoteServer.setupGroup(
            groupId: groupId,
            encryptedTitle: encryptedTitle,
            recipientsEncryptionDetails: recipientsEncryptionDetails
        ) { remoteResult in
            switch remoteResult {
            case .success:
                log.trace("encryption details for group \(groupId) saved to remote server. Updating local server")
                /// Save the encryption details for all users on server
                self.localServer.setupGroup(
                    groupId: groupId,
                    encryptedTitle: encryptedTitle,
                    recipientsEncryptionDetails: recipientsEncryptionDetails,
                    completionHandler: completionHandler
                )
            case .failure(let error):
                log.error("failed to create group with encryption details locally: \(error.localizedDescription)")
                completionHandler(.failure(error))
            }
        }
    }
    
    func setLocalGroupTitle(
        encryptedTitle: String,
        groupId: String,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        self.localServer.setGroupTitle(encryptedTitle: encryptedTitle,
                                       groupId: groupId,
                                       completionHandler: completionHandler)
    }
    
    public func deleteGroup(
        groupId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        self.localServer.deleteGroup(groupId: groupId) { localResult in
            switch localResult {
            case .success():
                self.remoteServer.deleteGroup(groupId: groupId, completionHandler: completionHandler)
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    internal func createOrUpdateThread(
        name: String?,
        recipientsEncryptionDetails: [RecipientEncryptionDetailsDTO]?,
        invitedPhoneNumbers: [String]?,
        completionHandler: @escaping (Result<ConversationThreadOutputDTO, Error>) -> ()
    ) {
        if let recipientsEncryptionDetails {
            log.trace("creating or updating thread with with users with ids \(recipientsEncryptionDetails.map({ $0.recipientUserIdentifier }))")
            log.debug("[setupThread] \(recipientsEncryptionDetails.map({ ($0.encryptedSecret, $0.ephemeralPublicKey, $0.secretPublicSignature, $0.senderPublicSignature) }))")
        }
        self.remoteServer.createOrUpdateThread(
            name: name,
            recipientsEncryptionDetails: recipientsEncryptionDetails,
            invitedPhoneNumbers: invitedPhoneNumbers
        ) {
            remoteResult in
            switch remoteResult {
            case .success(let thread):
                log.debug("thread created on server. Server returned encryptionDetails R=\(thread.encryptionDetails.recipientUserIdentifier) ES=\(thread.encryptionDetails.encryptedSecret), EPK=\(thread.encryptionDetails.ephemeralPublicKey) SSig=\(thread.encryptionDetails.secretPublicSignature) USig=\(thread.encryptionDetails.senderPublicSignature)")
                self.localServer.createOrUpdateThread(
                    serverThread: thread,
                    completionHandler: completionHandler
                )
            case .failure(let error):
                log.error("failed to create or update thread with encryption details: \(error.localizedDescription)")
                completionHandler(.failure(error))
            }
        }
    }
    
    internal func updateThread(
        _ threadId: String,
        newName: String?,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        self.remoteServer.updateThread(threadId, newName: newName) { remoteResult in
            switch remoteResult {
            case .failure(let error):
                completionHandler(.failure(error))
            case .success:
                self.localServer.updateThread(threadId, newName: newName) { _ in
                    completionHandler(.success(()))
                }
            }
        }
    }
    
    internal func updateThreadMembers(
        for threadId: String,
        _ update: ConversationThreadMembersUpdateDTO,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        self.remoteServer.updateThreadMembers(for: threadId, update, completionHandler: completionHandler)
    }
    
    internal func listThreads() async throws -> [ConversationThreadOutputDTO] {
        return try await withUnsafeThrowingContinuation { continuation in
            self.remoteServer.listThreads { remoteResult in
                switch remoteResult {
                case .success(let serverThreads):
                    continuation.resume(returning: serverThreads)
                case .failure(let error):
                    log.warning("failed to fetch threads from server. Returning local version. \(error.localizedDescription)")
                    self.localServer.listThreads {
                        localResult in
                        switch localResult {
                        case .success(let localThreads):
                            continuation.resume(returning: localThreads)
                        case .failure(let error):
                            continuation.resume(throwing: error)
                        }
                    }
                }
            }
        }
    }
    
    internal func listLocalThreads(
        withIdentifiers threadIds: [String]? = nil
    ) async throws -> [ConversationThreadOutputDTO]{
        return try await withUnsafeThrowingContinuation { continuation in
            self.localServer.listThreads(
                withIdentifiers: threadIds
            ) { result in
                switch result {
                case .success(let list):
                    continuation.resume(returning: list)
                case .failure(let err):
                    continuation.resume(throwing: err)
                }
            }
        }
    }
    
    internal func getThread(
        withId threadId: String,
        completionHandler: @escaping (Result<ConversationThreadOutputDTO?, Error>) -> ()
    ) {
        self.localServer.getThread(withId: threadId) { localResult in
            switch localResult {
            case .failure:
                self.remoteServer.getThread(withId: threadId, completionHandler: completionHandler)
            case .success(let maybeThread):
                if let maybeThread {
                    completionHandler(.success(maybeThread))
                } else {
                    self.remoteServer.getThread(
                        withId: threadId
                    ) { remoteResult in
                        completionHandler(remoteResult)
                    }
                }
            }
        }
    }
    
    internal func getThread(
        withUserIds userIds: [UserIdentifier],
        and phoneNumbers: [String],
        completionHandler: @escaping (Result<ConversationThreadOutputDTO?, Error>) -> ()
    ) {
        self.localServer.getThread(withUserIds: userIds, and: phoneNumbers) { localResult in
            switch localResult {
            case .failure:
                self.remoteServer.getThread(withUserIds: userIds, and: phoneNumbers, completionHandler: completionHandler)
            case .success(let maybeThread):
                if let maybeThread {
                    completionHandler(.success(maybeThread))
                } else {
                    self.remoteServer.getThread(withUserIds: userIds, and: phoneNumbers, completionHandler: completionHandler)
                }
            }
        }
    }
    
    /// Get them from local server, and rely on the thread asset sync operation to retrieve fresh information.
    /// If none is found or an error occurs, retrieve them from the remote server
    /// - Parameters:
    ///   - threadId: the thread identifier
    ///   - completionHandler: the callback method
    internal func getAssets(
        inThread threadId: String,
        completionHandler: @escaping (Result<ConversationThreadAssetsDTO, Error>) -> ()
    ) {
        self.remoteServer.getAssets(inThread: threadId) { remoteResult in
            switch remoteResult {
                
            case .success(let threadAssets):
                completionHandler(.success(threadAssets))
                
            case .failure(let failure):
                log.error("failed to get assets in thread \(threadId) from remote server, trying local. \(failure.localizedDescription)")
                self.localServer.getAssets(inThread: threadId, completionHandler: completionHandler)
            }
        }
    }
    
    internal func deleteThread(
        withId threadId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        self.remoteServer.deleteThread(withId: threadId) { remoteResult in
            switch remoteResult {
            case .success:
                self.localServer.deleteThread(withId: threadId, completionHandler: { res in
                    if case .failure(let failure) = res {
                        log.warning("thread \(threadId) was deleted on the server but not locally. Thread syncing will attempt this again. \(failure.localizedDescription)")
                    }
                })
                completionHandler(.success(()))
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    internal func deleteLocalThread(
        withId threadId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        self.localServer.deleteThread(
            withId: threadId,
            completionHandler: completionHandler
        )
    }
    
    internal func retrieveUserEncryptionDetails(
        forGroup groupId: String,
        completionHandler: @escaping (Result<RecipientEncryptionDetailsDTO?, Error>) -> ()
    ) {
        self.localServer.retrieveUserEncryptionDetails(forGroup: groupId) { localResult in
            if case .success(let localDetails) = localResult, let localDetails {
                completionHandler(.success(localDetails))
            } else {
                if case .failure(let error) = localResult {
                    log.warning("failed to retrieve details for group \(groupId) from local: \(error.localizedDescription)")
                }
                self.remoteServer.retrieveGroupDetails(forGroup: groupId) { remoteResult in
                    switch remoteResult {
                    case .success(let remoteDetails):
                        if let remoteDetails {
                            self.localServer.setupGroup(
                                groupId: groupId,
                                encryptedTitle: remoteDetails.encryptedTitle,
                                recipientsEncryptionDetails: [remoteDetails.encryptionDetails]
                            ) { _ in
                                completionHandler(.success(remoteDetails.encryptionDetails))
                            }
                        } else {
                            completionHandler(.success(nil))
                        }
                    case .failure(let error):
                        completionHandler(.failure(error))
                    }
                }
            }
        }
    }
    
    internal func retrieveUserEncryptionDetails(
        forThread threadId: String,
        completionHandler: @escaping (Result<RecipientEncryptionDetailsDTO?, Error>) -> ()
    ) {
        self.localServer.getThread(withId: threadId) { localResult in
            if case .success(let localThread) = localResult, let localThread {
                completionHandler(.success(localThread.encryptionDetails))
            } else {
                if case .failure(let error) = localResult {
                    log.warning("failed to retrieve <SELF> E2EE details for thread \(threadId) from local: \(error.localizedDescription)")
                }
                self.remoteServer.getThread(withId: threadId) { remoteResult in
                    switch remoteResult {
                    case .success(let remoteThread):
                        completionHandler(.success(remoteThread?.encryptionDetails))
                    case .failure(let error):
                        completionHandler(.failure(error))
                    }
                }
            }
        }
    }
    
    internal func addLocalReactions(
        _ reactions: [ReactionInput],
        toGroup groupId: String,
        completionHandler: @escaping (Result<[ReactionOutputDTO], Error>) -> ()
    ) {
        self.localServer.addReactions(
            reactions,
            toGroup: groupId,
            completionHandler: completionHandler
        )
    }
    
    internal func addLocalReactions(
        _ reactions: [ReactionInput],
        toThread threadId: String,
        completionHandler: @escaping (Result<[ReactionOutputDTO], Error>) -> ()
    ) {
        self.localServer.addReactions(
            reactions,
            toThread: threadId,
            completionHandler: completionHandler
        )
    }
    
    internal func addReactions(
        _ reactions: [ReactionInput],
        toGroup groupId: String,
        completionHandler: @escaping (Result<[ReactionOutputDTO], Error>) -> ()
    ) {
        self.remoteServer.addReactions(reactions, toGroup: groupId) { remoteResult in
            switch remoteResult {
            case .success(let reactionsOutput):
                ///
                /// Pass the output of the reaction creation on the server to the local server
                /// The output (rather than the input) is required, as an interaction identifier needs to be stored
                ///
                self.addLocalReactions(
                    reactionsOutput,
                    toGroup: groupId
                ) { localResult in
                    if case .failure(let failure) = localResult {
                        log.critical("The reaction could not be recorded on the local server. This will lead to incosistent results until a syncing mechanism is implemented. error=\(failure.localizedDescription)")
                    }
                    completionHandler(.success(reactionsOutput))
                }
            case .failure(let failure):
                completionHandler(.failure(failure))
            }
        }
    }
    
    internal func addReactions(
        _ reactions: [ReactionInput],
        toThread threadId: String,
        completionHandler: @escaping (Result<[ReactionOutputDTO], Error>) -> ()
    ) {
        self.remoteServer.addReactions(reactions, toThread: threadId) { remoteResult in
            switch remoteResult {
            case .success(let reactionsOutput):
                ///
                /// Pass the output of the reaction creation on the server to the local server
                /// The output (rather than the input) is required, as an interaction identifier needs to be stored
                ///
                self.addLocalReactions(
                    reactionsOutput,
                    toThread: threadId
                ) { localResult in
                    if case .failure(let failure) = localResult {
                        log.critical("The reaction could not be recorded on the local server. This will lead to incosistent results until a syncing mechanism is implemented. error=\(failure.localizedDescription)")
                    }
                    completionHandler(.success(reactionsOutput))
                }
            case .failure(let failure):
                completionHandler(.failure(failure))
            }
        }
    }
    
    internal func removeReaction(
        _ reactionType: ReactionType,
        inReplyToAssetGlobalIdentifier: GlobalIdentifier?,
        inReplyToInteractionId: String?,
        fromGroup groupId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        self.remoteServer.removeReaction(
            reactionType,
            senderPublicIdentifier: self.remoteServer.requestor.identifier,
            inReplyToAssetGlobalIdentifier: inReplyToAssetGlobalIdentifier,
            inReplyToInteractionId: inReplyToInteractionId,
            fromGroup: groupId
        ) { remoteResult in
            switch remoteResult {
            case .success():
                self.localServer.removeReaction(
                    reactionType,
                    senderPublicIdentifier: self.localServer.requestor.identifier,
                    inReplyToAssetGlobalIdentifier: inReplyToAssetGlobalIdentifier,
                    inReplyToInteractionId: inReplyToInteractionId,
                    fromGroup: groupId
                ) { localResult in
                    if case .failure(let failure) = localResult {
                        log.critical("The reaction was removed on the server but not locally. This will lead to inconsistent results until a syncing mechanism is implemented. error=\(failure.localizedDescription)")
                    }
                    completionHandler(.success(()))
                }
            case .failure(let failure):
                completionHandler(.failure(failure))
            }
        }
    }
    
    internal func removeReaction(
        _ reactionType: ReactionType,
        inReplyToAssetGlobalIdentifier: GlobalIdentifier?,
        inReplyToInteractionId: String?,
        fromThread threadId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        self.remoteServer.removeReaction(
            reactionType,
            senderPublicIdentifier: self.remoteServer.requestor.identifier,
            inReplyToAssetGlobalIdentifier: inReplyToAssetGlobalIdentifier,
            inReplyToInteractionId: inReplyToInteractionId,
            fromThread: threadId
        ) { remoteResult in
            switch remoteResult {
            case .success():
                self.localServer.removeReaction(
                    reactionType,
                    senderPublicIdentifier: self.localServer.requestor.identifier,
                    inReplyToAssetGlobalIdentifier: inReplyToAssetGlobalIdentifier,
                    inReplyToInteractionId: inReplyToInteractionId,
                    fromThread: threadId
                ) { localResult in
                    if case .failure(let failure) = localResult {
                        log.critical("The reaction was removed on the server but not locally. This will lead to inconsistent results until a syncing mechanism is implemented. error=\(failure.localizedDescription)")
                    }
                    completionHandler(.success(()))
                }
            case .failure(let failure):
                completionHandler(.failure(failure))
            }
        }
    }
    
    internal func addLocalMessages(
        _ messages: [MessageInput],
        toGroup groupId: String,
        completionHandler: @escaping (Result<[MessageOutputDTO], Error>) -> ()
    ) {
        self.localServer.addMessages(
            messages,
            toGroup: groupId,
            completionHandler: completionHandler
        )
    }
    
    internal func addLocalMessages(
        _ messages: [MessageInput],
        toThread threadId: String,
        completionHandler: @escaping (Result<[MessageOutputDTO], Error>) -> ()
    ) {
        self.localServer.addMessages(
            messages,
            toThread: threadId,
            completionHandler: completionHandler
        )
    }
    
    internal func addMessage(
        _ message: MessageInputDTO,
        toGroup groupId: String,
        completionHandler: @escaping (Result<MessageOutputDTO, Error>) -> ()
    ) {
        self.remoteServer.addMessages([message], toGroup: groupId) { remoteResult in
            switch remoteResult {
            case .success(let messageOutputs):
                guard let messageOutput = messageOutputs.first else {
                    completionHandler(.failure(SHHTTPError.ServerError.unexpectedResponse("empty result")))
                    return
                }
                completionHandler(.success(messageOutput))
                self.addLocalMessages([messageOutput], toGroup: groupId) { localResult in
                    if case .failure(let failure) = localResult {
                        log.critical("The message could not be recorded on the local server. This will lead to inconsistent results until a syncing mechanism is implemented. error=\(failure.localizedDescription)")
                    }
                }
            case .failure(let failure):
                completionHandler(.failure(failure))
            }
        }
    }
    
    internal func addMessage(
        _ message: MessageInputDTO,
        toThread threadId: String,
        completionHandler: @escaping (Result<MessageOutputDTO, Error>) -> ()
    ) {
        self.remoteServer.addMessages([message], toThread: threadId) { remoteResult in
            switch remoteResult {
            case .success(let messageOutputs):
                guard let messageOutput = messageOutputs.first else {
                    completionHandler(.failure(SHHTTPError.ServerError.unexpectedResponse("empty result")))
                    return
                }
                completionHandler(.success(messageOutput))
                self.addLocalMessages([messageOutput], toThread: threadId) { localResult in
                    if case .failure(let failure) = localResult {
                        log.critical("The message could not be recorded on the local server. This will lead to inconsistent results until a syncing mechanism is implemented. error=\(failure.localizedDescription)")
                    }
                }
            case .failure(let failure):
                completionHandler(.failure(failure))
            }
        }
    }
    
    internal func topLevelInteractionsSummaryFromRemote() async throws -> InteractionsSummaryDTO {
        try await withUnsafeThrowingContinuation { continuation in
            self.remoteServer.topLevelInteractionsSummary { result in
                switch result {
                case .failure(let error):
                    continuation.resume(throwing: error)
                case .success(let summary):
                    continuation.resume(returning: summary)
                }
            }
        }
    }
    
    internal func topLevelInteractionsSummaryFromLocal() async throws -> InteractionsSummaryDTO {
        try await withUnsafeThrowingContinuation { continuation in
            self.localServer.topLevelInteractionsSummary { result in
                switch result {
                case .failure(let error):
                    continuation.resume(throwing: error)
                case .success(let summary):
                    continuation.resume(returning: summary)
                }
            }
        }
    }
    
    internal func topLevelInteractionsSummary() async throws -> InteractionsSummaryDTO {
        try await withUnsafeThrowingContinuation { continuation in
            self.remoteServer.topLevelInteractionsSummary { result in
                switch result {
                    
                case .failure(let error):
                    log.warning("failed to get interactions summary from server. Fetching local summary. \(error.localizedDescription)")
                    self.localServer.topLevelInteractionsSummary {
                        localResult in
                        switch localResult {
                        case .success(let success):
                            continuation.resume(returning: success)
                        case .failure(let failure):
                            continuation.resume(throwing: failure)
                        }
                    }
                    
                case .success(let summary):
                    continuation.resume(returning: summary)
                }
            }
        }
    }
    
    internal func topLevelThreadsInteractionsSummary() async throws -> [String: InteractionsThreadSummaryDTO] {
        try await withUnsafeThrowingContinuation { continuation in
            self.remoteServer.topLevelThreadsInteractionsSummary {
                result in
                switch result {
                    
                case .failure(let error):
                    log.warning("failed to get threads interactions summary from server. Fetching local summary. \(error.localizedDescription)")
                    self.localServer.topLevelThreadsInteractionsSummary {
                        localResult in
                        switch localResult {
                        case .success(let success):
                            continuation.resume(returning: success)
                        case .failure(let failure):
                            continuation.resume(throwing: failure)
                        }
                    }
                    
                case .success(let summaryByThreadId):
                    continuation.resume(returning: summaryByThreadId)
                }
            }
        }
    }
    
    internal func topLevelGroupsInteractionsSummary() async throws -> [String: InteractionsGroupSummaryDTO] {
        try await withUnsafeThrowingContinuation { continuation in
            self.remoteServer.topLevelGroupsInteractionsSummary {
                result in
                switch result {
                    
                case .failure(let error):
                    if error is URLError || error is SHHTTPError.TransportError {
                    } else {
                        log.warning("failed to get groups interactions summary from server. Fetching local summary. \(error.localizedDescription)")
                    }
                    self.localServer.topLevelGroupsInteractionsSummary {
                        localResult in
                        switch localResult {
                        case .success(let success):
                            continuation.resume(returning: success)
                        case .failure(let failure):
                            continuation.resume(throwing: failure)
                        }
                    }
                    
                case .success(let summaryByGroupId):
                    continuation.resume(returning: summaryByGroupId)
                }
            }
        }
    }
    
    public func topLevelLocalInteractionsSummary(
        for groupId: String
    ) async throws -> InteractionsGroupSummaryDTO {
        try await withUnsafeThrowingContinuation { continuation in
            self.localServer.topLevelInteractionsSummary(inGroup: groupId) { result in
                switch result {
                case .success(let success):
                    continuation.resume(returning: success)
                case .failure(let failure):
                    continuation.resume(throwing: failure)
                }
            }
        }
    }
    
    /// Create in the local server the threads provided. If they exist they will be overwritten
    /// - Parameters:
    ///   - threadsToCreate: the list of threads to create locally
    /// - Returns: the list of threads created
    internal func createThreadsLocally(
        _ threadsToCreate: [ConversationThreadOutputDTO]
    ) async -> [ConversationThreadOutputDTO] {
        
        guard threadsToCreate.isEmpty == false else {
            return []
        }
        
        return await withUnsafeContinuation { continuation in
            let dispatchGroup = DispatchGroup()
            for threadToCreateLocally in threadsToCreate {
                dispatchGroup.enter()
                self.localServer.createOrUpdateThread(
                    serverThread: threadToCreateLocally
                ) { createResult in
                    if case .failure(let error) = createResult {
                        log.error("failed to create thread locally. \(error.localizedDescription)")
                    }
                    dispatchGroup.leave()
                }
            }
            
            dispatchGroup.notify(queue: .global()) {
                continuation.resume(returning: threadsToCreate)
            }
        }
    }
    
    /// Update the last updated at based on the value in the provided threads
    /// - Parameter threads: the threads
    internal func updateLocalThreads(
        from threads: [ConversationThreadOutputDTO]
    ) async throws {
        guard threads.isEmpty == false else {
            return
        }
        
        return try await withUnsafeThrowingContinuation { continuation in
            self.localServer.updateThreads(
                from: threads
            ) { result in
                switch result {
                case .failure(let error):
                    continuation.resume(throwing: error)
                case .success:
                    continuation.resume(returning: ())
                }
            }
        }
    }
    
    /// Update the last updated at based on the value in the provided threads
    /// - Parameter threads: the threads
    internal func updateLocalThread(
        from threadUpdate: WebSocketMessage.ThreadUpdate
    ) async throws {
        
        return try await withUnsafeThrowingContinuation { continuation in
            self.localServer.updateThreads(
                from: [threadUpdate]
            ) { result in
                switch result {
                case .failure(let error):
                    continuation.resume(throwing: error)
                case .success:
                    continuation.resume(returning: ())
                }
            }
        }
    }
    
    func addLocalInteractions(
        _ remoteInteractions: InteractionsGroupDTO,
        inAnchor anchor: SHInteractionAnchor,
        anchorId: String
    ) {
        let messagesCompletionBlock = { (result: Result<[MessageOutputDTO], Error>) -> Void in
            switch result {
            case .success(let array):
                log.info("cached \(array.count) messages in \(anchor.rawValue) \(anchorId)")
            case .failure(let error):
                log.error("failed to cache messages in \(anchor.rawValue) \(anchorId): \(error.localizedDescription)")
            }
        }
        
        let reactionsCompletionBlock = { (result: Result<[ReactionOutputDTO], Error>) -> Void in
            switch result {
            case .success(let array):
                log.info("cached \(array.count) reactions in \(anchor.rawValue) \(anchorId)")
            case .failure(let error):
                log.error("failed to cache reactions in \(anchor.rawValue) \(anchorId): \(error.localizedDescription)")
            }
        }
        
        switch anchor {
        case .thread:
            if remoteInteractions.messages.isEmpty == false {
                self.addLocalMessages(
                    remoteInteractions.messages,
                    toThread: anchorId,
                    completionHandler: messagesCompletionBlock
                )
            }
            
            if remoteInteractions.reactions.isEmpty == false {
                self.addLocalReactions(
                    remoteInteractions.reactions,
                    toThread: anchorId,
                    completionHandler: reactionsCompletionBlock
                )
            }
        case .group:
            if remoteInteractions.messages.isEmpty == false {
                self.addLocalMessages(
                    remoteInteractions.messages,
                    toGroup: anchorId,
                    completionHandler: messagesCompletionBlock
                )
            }
            
            if remoteInteractions.reactions.isEmpty == false {
                self.addLocalReactions(
                    remoteInteractions.reactions,
                    toGroup: anchorId,
                    completionHandler: reactionsCompletionBlock
                )
            }
        }
    }
    
    func retrieveInteractions(
        inGroup groupId: String,
        ofType type: InteractionType?,
        underMessage messageId: String?,
        before: Date?,
        limit: Int,
        completionHandler: @escaping (Result<InteractionsGroupDTO, Error>) -> ()
    ) {
        self.retrieveRemoteInteractions(
            inGroup: groupId,
            ofType: type,
            underMessage: messageId,
            before: before,
            limit: limit
        ) {
            result in
            switch result {
            case .failure(let serverError):
                self.retrieveLocalInteractions(
                    inGroup: groupId,
                    ofType: type,
                    underMessage: messageId,
                    before: before,
                    limit: limit
                ) { localResult in
                    switch localResult {
                    case .failure:
                        completionHandler(.failure(serverError))
                    case .success(let localInteractions):
                        completionHandler(.success(localInteractions))
                    }
                }
            case .success(let remoteInteractions):
                completionHandler(.success(remoteInteractions))
                self.addLocalInteractions(
                    remoteInteractions, 
                    inAnchor: .group,
                    anchorId: groupId
                )
            }
        }
    }
    
    func retrieveInteractions(
        inThread threadId: String,
        ofType type: InteractionType?,
        underMessage messageId: String?,
        before: Date?,
        limit: Int,
        completionHandler: @escaping (Result<InteractionsGroupDTO, Error>) -> ()
    ) {
        self.retrieveRemoteInteractions(
            inThread: threadId,
            ofType: type,
            underMessage: messageId,
            before: before,
            limit: limit
        ) {
            result in
            switch result {
            case .failure(let serverError):
                self.retrieveLocalInteractions(
                    inThread: threadId,
                    ofType: type,
                    underMessage: messageId,
                    before: before,
                    limit: limit
                ) { localResult in
                    switch localResult {
                    case .failure:
                        completionHandler(.failure(serverError))
                    case .success(let localInteractions):
                        completionHandler(.success(localInteractions))
                    }
                }
            case .success(let remoteInteractions):
                completionHandler(.success(remoteInteractions))
                self.addLocalInteractions(
                    remoteInteractions,
                    inAnchor: .thread,
                    anchorId: threadId
                )
            }
        }
    }
    
    func retrieveLocalInteractions(
        inGroup groupId: String,
        ofType type: InteractionType?,
        underMessage messageId: String?,
        before: Date?,
        limit: Int,
        completionHandler: @escaping (Result<InteractionsGroupDTO, Error>) -> ()
    ) {
        self.localServer.retrieveInteractions(
            inGroup: groupId,
            ofType: type,
            underMessage: messageId,
            before: before,
            limit: limit,
            completionHandler: completionHandler
        )
    }
    
    func retrieveLocalInteractions(
        inThread threadId: String,
        ofType type: InteractionType?,
        underMessage messageId: String?,
        before: Date?,
        limit: Int,
        completionHandler: @escaping (Result<InteractionsGroupDTO, Error>) -> ()
    ) {
        self.localServer.retrieveInteractions(
            inThread: threadId,
            ofType: type,
            underMessage: messageId,
            before: before,
            limit: limit,
            completionHandler: completionHandler
        )
    }
    
    func retrieveRemoteInteractions(
        inGroup groupId: String,
        ofType type: InteractionType?,
        underMessage messageId: String?,
        before: Date?,
        limit: Int,
        completionHandler: @escaping (Result<InteractionsGroupDTO, Error>) -> ()
    ) {
        self.remoteServer.retrieveInteractions(
            inGroup: groupId,
            ofType: type,
            underMessage: messageId,
            before: before,
            limit: limit
        ) { remoteResult in
            switch remoteResult {
            case .success(let remoteInteractions):
                completionHandler(.success(remoteInteractions))
            case .failure(let failure):
                completionHandler(.failure(failure))
            }
        }
    }
    
    func retrieveRemoteInteractions(
        inThread threadId: String,
        ofType type: InteractionType?,
        underMessage messageId: String?,
        before: Date?,
        limit: Int,
        completionHandler: @escaping (Result<InteractionsGroupDTO, Error>) -> ()
    ) {
        self.remoteServer.retrieveInteractions(
            inThread: threadId,
            ofType: type,
            underMessage: messageId,
            before: before,
            limit: limit
        ) { remoteResult in
            switch remoteResult {
            case .success(let remoteInteractions):
                completionHandler(.success(remoteInteractions))
            case .failure(let failure):
                completionHandler(.failure(failure))
            }
        }
    }
    
    func retrieveLocalInteraction(
        inThread threadId: String,
        withId interactionIdentifier: String,
        completionHandler: @escaping (Result<InteractionsGroupDTO, Error>) -> ()
    ) {
        self.localServer.retrieveInteraction(
            anchorType: .thread,
            anchorId: threadId,
            withId: interactionIdentifier,
            completionHandler: completionHandler
        )
    }
    
    func retrieveLocalInteraction(
        inGroup groupId: String,
        withId interactionIdentifier: String,
        completionHandler: @escaping (Result<InteractionsGroupDTO, Error>) -> ()
    ) {
        self.localServer.retrieveInteraction(
            anchorType: .group,
            anchorId: groupId,
            withId: interactionIdentifier,
            completionHandler: completionHandler
        )
    }
}

extension SHServerProxy {
    
    // MARK: - Subscriptions
    
    public func validateTransaction(
        originalTransactionId: String,
        receipt: String,
        productId: String,
        completionHandler: @escaping (Result<SHReceiptValidationResponse, Error>) -> ()
    ) {
        let group = DispatchGroup()
        var localResult: Result<SHReceiptValidationResponse, Error>? = nil
        var serverResult: Result<SHReceiptValidationResponse, Error>? = nil
        
        group.enter()
        self.localServer.validateTransaction(
            originalTransactionId: originalTransactionId,
            receipt: receipt,
            productId: productId
        ) { result in
            localResult = result
            group.leave()
        }
        
        group.enter()
        self.remoteServer.validateTransaction(
            originalTransactionId: originalTransactionId,
            receipt: receipt,
            productId: productId
        ) { result in
            serverResult = result
            group.leave()
        }
        
        let dispatchResult = group.wait(timeout: .now() + .seconds(SHDefaultNetworkTimeoutInMilliseconds * 2))
        guard dispatchResult == .success else {
            return completionHandler(.failure(SHHTTPError.TransportError.timedOut))
        }
        
        guard let localResult = localResult, let serverResult = serverResult else {
            return completionHandler(.failure(SHHTTPError.ServerError.noData))
        }
        
        switch localResult {
        case .failure(let localErr):
            completionHandler(.failure(localErr))
        case .success(let localResponse):
            switch serverResult {
            case .success(let serverRespose):
                // TODO: After Safehill server notifications are implemented make sure values from server and local invocation of StoreKit API agree
                // Currently we only validate the receipt with the StoreKit server API, and on Safehill Server that this receipt has been granted to the user
                
                completionHandler(.success(localResponse))
            case .failure(let serverErr):
                log.critical("receipt server validation failed with error: \(serverErr.localizedDescription)")
                completionHandler(.failure(serverErr))
            }
        }
    }
}

extension SHServerProxy {
    
    // MARK: - Phone Number Invitations
    
    func invite(
        _ phoneNumbers: [String],
        to groupId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        self.remoteServer.invite(phoneNumbers, to: groupId) { remoteResult in
            switch remoteResult {
            case .success:
                self.localServer.invite(phoneNumbers, to: groupId) { _ in
                    completionHandler(.success(()))
                }
                
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    public func uninvite(
        _ phoneNumbers: [String],
        from groupId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        self.remoteServer.uninvite(phoneNumbers, from: groupId) { remoteResult in
            switch remoteResult {
            case .success:
                self.localServer.uninvite(phoneNumbers, from: groupId) { _ in
                    completionHandler(.success(()))
                }
                
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    public func requestAccess(
        toGroupId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        self.remoteServer.requestAccess(toGroupId: toGroupId, completionHandler: completionHandler)
    }
    
    public func requestAccess(
        toThreadId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        self.remoteServer.requestAccess(toThreadId: toThreadId, completionHandler: completionHandler)
    }
    
    public func updateAssetFingerprint(
        for globalIdentifier: GlobalIdentifier,
        _ fingerprint: PerceptualHash
    ) async throws {
        try await self.remoteServer.updateAssetFingerprint(for: globalIdentifier, fingerprint)
    }
    
    public func sendEncryptedKeysToWebClient(
        sessionId: String,
        requestorIp: String,
        encryptedPrivateKeyData: Data,
        encryptedPrivateSignatureData: Data
    ) async throws -> Void
    {
        try await self.remoteServer.sendEncryptedKeysToWebClient(
            sessionId: sessionId,
            requestorIp: requestorIp,
            encryptedPrivateKeyData: encryptedPrivateKeyData,
            encryptedPrivateSignatureData: encryptedPrivateSignatureData
        )
    }
}
