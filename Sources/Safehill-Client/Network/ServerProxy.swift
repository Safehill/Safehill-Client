import Foundation
import Yams
import Contacts

public struct SHServerProxy: SHServerProxyProtocol {
    
    let localServer: LocalServer
    let remoteServer: RemoteServer
    
    public init(user: SHLocalUserProtocol) {
        self.localServer = LocalServer(requestor: user)
        self.remoteServer = RemoteServer(requestor: user)
    }
}


extension SHServerProxy {
    
    // MARK: - Migrations
    
    public func runLocalMigrations(
        currentBuild: Int?,
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
                               completionHandler: @escaping (Result<Void, Error>) -> ()) {
        self.remoteServer.sendCodeToUser(
            countryCode: countryCode,
            phoneNumber: phoneNumber,
            code: code,
            medium: medium,
            completionHandler: completionHandler
        )
    }
    
    public func updateUser(phoneNumber: SHPhoneNumber? = nil,
                           name: String? = nil,
                           completionHandler: @escaping (Result<any SHServerUser, Error>) -> ()) {
        self.remoteServer.updateUser(name: name, phoneNumber: phoneNumber) { result in
            switch result {
            case .success(_):
                self.localServer.updateUser(name: name, phoneNumber: phoneNumber, completionHandler: completionHandler)
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    public func updateLocalUser(name: String,
                                completionHandler: @escaping (Result<any SHServerUser, Error>) -> ()) {
        self.localServer.updateUser(name: name, completionHandler: completionHandler)
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
    
    public func signIn(clientBuild: Int?, completionHandler: @escaping (Result<SHAuthResponse, Error>) -> ()) {
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
            descriptor.sharingInfo.sharedWithUserIdentifiersInGroup.keys.forEach({ userIdsSet.insert($0) })
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
    
    public func registerDevice(_ deviceId: String, token: String, completionHandler: @escaping (Result<Void, Error>) -> ()) {
        self.remoteServer.registerDevice(deviceId, token: token, completionHandler: completionHandler)
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
    
    func getLocalAssetDescriptors(
        for globalIdentifiers: [GlobalIdentifier]? = nil,
        after: Date? = nil,
        filteringGroups: [String]? = nil,
        completionHandler: @escaping (Result<[any SHAssetDescriptor], Error>) -> ()
    ) {
        self.localServer.getAssetDescriptors(
            forAssetGlobalIdentifiers: globalIdentifiers ?? [],
            filteringGroupIds: filteringGroups,
            after: after
        ) { result in
            switch result {
            case .failure(let err):
                completionHandler(.failure(err))
            case .success(let descriptors):
#if DEBUG
                if descriptors.count > 0 {
//                    let encoder = YAMLEncoder()
//                    let encoded = (try? encoder.encode(descriptors as! [SHGenericAssetDescriptor])) ?? ""
//                    log.debug("[DESCRIPTORS] from local server:\n\(encoded)")
//                    log.debug("[DESCRIPTORS] from local server: \(descriptors.count)")
                } else {
//                    log.debug("[DESCRIPTORS] from local server: empty")
                }
#endif
                completionHandler(result)
            }
        }
    }
    
    /// Get all visible asset descriptors to this user. Fall back to local descriptor if server is unreachable
    /// - Parameter completionHandler: the callback method
    func getRemoteAssetDescriptors(
        for globalIdentifiers: [GlobalIdentifier]? = nil,
        filteringGroups: [String]? = nil,
        after: Date?,
        completionHandler: @escaping (Result<[any SHAssetDescriptor], Error>) -> ()
    ) {
        let handleServerResult = { (serverResult: Result<[any SHAssetDescriptor], Error>) in
            switch serverResult {
            case .failure(let serverError):
                completionHandler(.failure(serverError))
            case .success(let descriptors):
#if DEBUG
                if descriptors.count > 0 {
//                    let encoder = YAMLEncoder()
//                    let encoded = (try? encoder.encode(descriptors as! [SHGenericAssetDescriptor])) ?? ""
//                    log.debug("[DESCRIPTORS] from remote server:\n\(encoded)")
//                    log.debug("[DESCRIPTORS] from remote server: \(descriptors.count)")
                } else {
//                    log.debug("[DESCRIPTORS] from remote server: empty")
                }
#endif
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
    
    /// Fill the specified version of the requested assets in the local server (cache)
    /// - Parameter descriptorsKeyedByGlobalIdentifier: the assets' descriptors keyed by their global identifier
    /// - Parameter quality: the quality of the asset to cache
    private func cacheAssets(
        for descriptorsKeyedByGlobalIdentifier: [GlobalIdentifier: any SHAssetDescriptor],
        quality: SHAssetQuality
    ) {
        log.trace("[CACHING] Attempting to cache \(quality.rawValue) for assets \(descriptorsKeyedByGlobalIdentifier)")
        
        let globalIdentifiers = Array(descriptorsKeyedByGlobalIdentifier.keys)
        
        ///
        /// Get the asset from the remote server (CDN)
        ///
        self.remoteServer.getAssets(
            withGlobalIdentifiers: globalIdentifiers,
            versions: [quality]
        ) { result in
            switch result {
            case .success(let encryptedDict):
                guard encryptedDict.isEmpty == false else {
                    log.error("[CACHING] No \(quality.rawValue) for assets \(globalIdentifiers) on remote server")
                    return
                }
                
                for (globalIdentifier, encryptedAsset) in encryptedDict {
                    ///
                    /// Store the asset in the local server (cache)
                    ///
                    self.localServer.create(
                        assets: [encryptedAsset],
                        descriptorsByGlobalIdentifier: descriptorsKeyedByGlobalIdentifier,
                        uploadState: .completed
                    ) {
                        result in
                        switch result {
                        case .success(_):
                            log.trace("[CACHING] Downloaded and cached \(quality.rawValue) for asset \(globalIdentifier)")
                        case .failure(let error):
                            log.error("[CACHING] Unable to save asset \(globalIdentifier) to local server: \(error.localizedDescription)")
                        }
                    }
                }
            case .failure(let error):
                log.error("[CACHING] Unable to get assets \(globalIdentifiers) from remote server. \(error.localizedDescription)")
            }
        }
    }
    
    /// Fill the specified version of the requested assets in the local server (cache)
    /// - Parameter globalIdentifiers: the assets' global identifiers
    /// - Parameter quality: the quality of the asset to cache
    private func cacheAssets(with globalIdentifiers: [String],
                             quality: SHAssetQuality) {
        log.trace("[CACHING] Attempting to cache \(quality.rawValue) for assets \(globalIdentifiers)")
        
        ///
        /// Get the remote descriptor from the remote server
        ///
        self.remoteServer.getAssetDescriptors(
            forAssetGlobalIdentifiers: globalIdentifiers,
            filteringGroupIds: nil,
            after: nil
        ) { result in
            switch result {
            case .success(let descriptors):
                let descriptorsByGlobalIdentifier = descriptors.reduce([String: any SHAssetDescriptor]()) {
                    partialResult, descriptor in
                    var result = partialResult
                    result[descriptor.globalIdentifier] = descriptor
                    return result
                }
                
                self.cacheAssets(for: descriptorsByGlobalIdentifier, quality: quality)
            case .failure(let error):
                log.error("[CACHING] Unable to get asset descriptors \(globalIdentifiers) from remote server. error=\(error.localizedDescription)")
            }
        }
    }
    
    private func organizeAssetVersions(
        _ encryptedAssetsByGlobalId: [String: any SHEncryptedAsset],
        basedOnRequested requestedVersions: [SHAssetQuality]
    ) -> [String: any SHEncryptedAsset] {
        var finalDict = encryptedAssetsByGlobalId
        for (gid, encryptedAsset) in encryptedAssetsByGlobalId {
            
            var newEncryptedVersions = [SHAssetQuality: any SHEncryptedAssetVersion]()
            
            if requestedVersions.contains(.hiResolution),
               encryptedAsset.encryptedVersions.contains(where: { (quality, _) in quality == .midResolution }),
               encryptedAsset.encryptedVersions.contains(where: { (quality, _) in quality == .hiResolution }) == false {
                ///
                /// If `.hiResolution` was requested, use the `.midResolution` version if any is available under that key
                ///
                newEncryptedVersions[SHAssetQuality.hiResolution] = encryptedAsset.encryptedVersions[.midResolution]!
                
                ///
                /// Populate the rest of the versions based on the `requestedVersions`
                ///
                for version in requestedVersions {
                    if version != .hiResolution,
                       let v = encryptedAsset.encryptedVersions[version] {
                        newEncryptedVersions[version] = v
                    }
                }
            } else {
                for version in requestedVersions {
                    if let v = encryptedAsset.encryptedVersions[version] {
                        newEncryptedVersions[version] = v
                    }
                }
            }
            
            finalDict[gid] = SHGenericEncryptedAsset(
                globalIdentifier: encryptedAsset.globalIdentifier,
                localIdentifier: encryptedAsset.localIdentifier,
                creationDate: encryptedAsset.creationDate,
                encryptedVersions: newEncryptedVersions
            )
        }
        return finalDict
    }
    
    ///
    /// Retrieve asset from local server (cache).
    ///
    /// /// If only a `.lowResolution` version is available, this method triggers the caching of the `.midResolution` in the background.
    /// In addition, when asking for a `.midResolution` or a `.hiResolution` version, and the `cacheHiResolution` parameter is set to `true`,
    /// this method triggers the caching in the background of the `.hiResolution` version, unless already availeble, replacing the `.midResolution`.
    /// Use the `cacheHiResolution` carefully, as higher resolution can take a lot of space on disk.
    ///
    /// - Parameters:
    ///   - assetIdentifiers: the global identifier of the asset to retrieve
    ///   - versions: the versions to retrieve
    ///   - cacheHiResolution: if the `.hiResolution` isn't in the local server, then fetch it and cache it in the background. `.hiResolution` is usually a big file, so this boolean lets clients control the caching strategy. Also, this parameter only makes sense when requesting `.midResolution` or `.hiResolution` versions. It's a no-op otherwise.
    ///   - completionHandler: the callback method returning the encrypted assets keyed by global id, or the error
    func getLocalAssets(withGlobalIdentifiers assetIdentifiers: [String],
                        versions: [SHAssetQuality],
                        cacheHiResolution: Bool,
                        completionHandler: @escaping (Result<[String: any SHEncryptedAsset], Error>) -> ()) {
        var versionsToRetrieve = Set(versions)
        
        ///
        /// Because `.hiResoution` might not be present in local cache, then always try to pull the `.midResolution`
        /// when `.hiResolution` is explicitly requested, and return that version instead
        ///
        if versionsToRetrieve.contains(.hiResolution),
           versionsToRetrieve.contains(.midResolution) == false {
            versionsToRetrieve.insert(.midResolution)
        }
        
        if cacheHiResolution {
            ///
            /// A `.midResolution` version for asset being requested from the local cache
            /// is a strong signal that the high resolution version needs to be downloaded, if not already.
            /// If no `.hiResolution` is returned from the local cache, then fetch that resolution in the background.
            ///
            if versionsToRetrieve.contains(.midResolution),
               versionsToRetrieve.contains(.hiResolution) == false {
                versionsToRetrieve.insert(.hiResolution)
            }
        }
        
        ///
        /// Always add the `.lowResolution`, even when not explicitly requested
        /// so that we can distinguish between assets that don't have ANY version
        /// and assets that only have a `.lowResolution`.
        /// An asset with `.lowResolution` only will trigger the loading of the next quality version in the background
        ///
        versionsToRetrieve.insert(.lowResolution)
        
        self.localServer.getAssets(withGlobalIdentifiers: assetIdentifiers,
                                   versions: Array(versionsToRetrieve)) { result in
            switch result {
            case .success(let dict):
                ///
                /// Always cache the `.midResolution` if the `.lowResolution` is the only version available
                ///
                for (globalIdentifier, encryptedAsset) in dict {
                    if versionsToRetrieve.count > 1,
                       encryptedAsset.encryptedVersions.keys.count == 1,
                       encryptedAsset.encryptedVersions.keys.first! == .lowResolution {
                        DispatchQueue.global(qos: .background).async {
                            self.cacheAssets(with: [globalIdentifier], quality: .midResolution)
                        }
                    }
                }
                
                ///
                /// Cache the `.hiResolution` if requested
                ///
                if cacheHiResolution {
                    var hiResGlobalIdentifiersToLazyLoad = [String]()
                    for (globalIdentifier, encryptedAsset) in dict {
                        ///
                        /// Determine the `.hiResolution` asset identifiers to lazy load,
                        /// as some asset identifiers might have a `.hiResolution` available and some don't
                        ///
                        if versionsToRetrieve.contains(.hiResolution),
                           encryptedAsset.encryptedVersions.keys.contains(.hiResolution) == false {
                            hiResGlobalIdentifiersToLazyLoad.append(globalIdentifier)
                        }
                    }
                    
                    if hiResGlobalIdentifiersToLazyLoad.count > 0 {
                        DispatchQueue.global(qos: .background).async {
                            self.cacheAssets(with: hiResGlobalIdentifiersToLazyLoad, quality: .hiResolution)
                        }
                    }
                }
                
                completionHandler(.success(self.organizeAssetVersions(dict, basedOnRequested: versions)))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    ///
    /// Retrieves assets versions with given identifiers.
    /// Tries to fetch from local server first, then remote server if some are not present. For those not available in the local server, **it updates the local server (cache)**
    ///
    /// - Parameters:
    ///   - assetIdentifiers: the global asset identifiers to retrieve
    ///   - versions: filter asset version (retrieve just the low res asset or the hi res asset, for instance)
    ///   - completionHandler: the callback, returning the `SHEncryptedAsset` objects keyed by asset identifier. Note that the output object might not have the same number of assets requested, as some of them might be deleted on the server
    ///
    func getAssets(withGlobalIdentifiers assetIdentifiers: [String],
                   versions: [SHAssetQuality],
                   completionHandler: @escaping (Result<[String: any SHEncryptedAsset], Error>) -> ()) {
        if assetIdentifiers.count == 0 {
            completionHandler(.success([:]))
            return
        }
        
        ///
        /// Because `.hiResoution` might not be uploaded yet, try to pull the `.midResolution`
        /// when `.hiResolution` is explicitly requested, and return that version instead
        ///
        var newVersions = Set(versions)
        if newVersions.contains(.hiResolution),
           newVersions.contains(.midResolution) == false {
            newVersions.insert(.midResolution)
        }
        
        var localDictionary: [String: any SHEncryptedAsset] = [:]
        var assetIdentifiersToFetch = assetIdentifiers
        
        let group = DispatchGroup()
        
        ///
        /// Get the asset from the local server cache
        /// Do this first to support offline access, and rely on the AssetDownloader to clean up local assets that were deleted on server.
        /// The right thing way to do this is to retrieve descriptors first and fetch local assets later, but that would not support offline.
        ///
        /// **NOTE**
        /// `cacheHiResolution` is set to `false` because this method's contract expects that the asset is retrieved and returned
        /// from local server when available.
        /// On the contrary, the contract for `getLocalAssets(withGlobalIdentifiers:versions:cacheHiResolution:)`
        /// is that these assets are not returned when not available in the local server, hence the caching would happen in the background.
        ///
        group.enter()
        self.getLocalAssets(
            withGlobalIdentifiers: assetIdentifiersToFetch,
            versions: Array(versions),
            cacheHiResolution: false
        ) { localResult in
            if case .success(let assetsDict) = localResult {
                localDictionary = assetsDict
                
            }
            group.leave()
        }
        
        group.notify(queue: .global()) {
            assetIdentifiersToFetch = assetIdentifiers.subtract(Array(localDictionary.keys))
            
            /// If all could be found locally return success
            guard assetIdentifiersToFetch.count > 0 else {
                completionHandler(.success(localDictionary))
                return
            }
            
            ///
            /// Get the asset descriptors from the remote Safehill server.
            /// This is needed to:
            /// - filter out assets that haven't been uploaded yet. The call to the CDN would otherwise fail for those.
            /// - filter out the ones that are no longer shared with this user. In fact, it is possible the client still asks for this asset, but it should not be fetched.
            /// - determine the groupId used to upload or share by/with this user. That is the groupId that should be saved with the asset sharing info by the `LocalServer`.
            ///
            
            var error: Error? = nil
            var descriptorsByAssetGlobalId: [String: any SHAssetDescriptor] = [:]
            
            group.enter()
            self.remoteServer.getAssetDescriptors(
                forAssetGlobalIdentifiers: assetIdentifiersToFetch,
                filteringGroupIds: nil,
                after: nil
            ) {
                result in
                switch result {
                case .success(let descriptors):
                    descriptorsByAssetGlobalId = descriptors
                        .reduce([:]) { partialResult, descriptor in
                            var result = partialResult
                            result[descriptor.globalIdentifier] = descriptor
                            return result
                        }
                case .failure(let err):
                    error = err
                }
                group.leave()
            }
            
            group.notify(queue: .global()) {
                
                guard error == nil else {
                    if error is URLError || error is SHHTTPError.TransportError {
                        /// Failing to establish the connection with the server very likely means the other calls will fail too.
                        /// Return the local results
                        completionHandler(.success(localDictionary))
                    } else {
                        completionHandler(.failure(error!))
                    }
                    return
                }
                
                ///
                /// Reset the descriptors to fetch based on the server descriptors
                ///
                if assetIdentifiersToFetch.count != descriptorsByAssetGlobalId.count {
                    log.warning("Some assets requested could not be found in the server manifest, shared with you. Skipping those")
                }
                assetIdentifiersToFetch = Array(descriptorsByAssetGlobalId.keys)
                guard assetIdentifiersToFetch.count > 0 else {
                    completionHandler(.success(localDictionary))
                    return
                }
                
                ///
                /// Get the asset from the remote Safehill server.
                ///
                var remoteDictionary = [String: any SHEncryptedAsset]()
                
                group.enter()
                self.remoteServer.getAssets(withGlobalIdentifiers: assetIdentifiersToFetch,
                                            versions: Array(newVersions)) { serverResult in
                    switch serverResult {
                    case .success(let assetsDict):
                        guard assetsDict.count > 0 else {
                            error = SHHTTPError.ClientError.notFound
                            log.error("No assets with globalIdentifiers \(assetIdentifiersToFetch)")
                            break
                        }
                        remoteDictionary = self.organizeAssetVersions(assetsDict, basedOnRequested: versions)
                    case .failure(let err):
                        log.error("failed to get assets with globalIdentifiers \(assetIdentifiersToFetch): \(err.localizedDescription)")
                        error = err
                    }
                    group.leave()
                }
                
                group.notify(queue: .global()) {
                    guard error == nil else {
                        completionHandler(.failure(error!))
                        return
                    }
                    
                    ///
                    /// Create a copy of the assets just fetched from the server in the local server (cache)
                    ///
                    let encryptedAssetsToCreate = remoteDictionary.filter({ assetGid, _ in descriptorsByAssetGlobalId[assetGid] != nil }).values
                    self.localServer.create(assets: Array(encryptedAssetsToCreate),
                                            descriptorsByGlobalIdentifier: descriptorsByAssetGlobalId,
                                            uploadState: .completed) { result in
                        if case .failure(let err) = result {
                            log.warning("could not save downloaded server asset to the local cache. This operation will be attempted again, but for now the cache is out of sync. error=\(err.localizedDescription)")
                        }
                        completionHandler(.success(localDictionary.merging(remoteDictionary, uniquingKeysWith: { _, server in server })))
                    }
                }
            }
        }
    }
    
    func upload(
        serverAsset: SHServerAsset,
        asset: any SHEncryptedAsset,
        filterVersions: [SHAssetQuality]? = nil
    ) async throws {
        try await self.remoteServer.upload(serverAsset: serverAsset, asset: asset, filterVersions: filterVersions)
        try await self.localServer.upload(serverAsset: serverAsset, asset: asset, filterVersions: filterVersions)
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
    
    func shareAssetLocally(_ asset: SHShareableEncryptedAsset,
                           completionHandler: @escaping (Result<Void, Error>) -> ()) {
        self.localServer.share(asset: asset) {
            result in
            switch result {
            case .success(_):
                completionHandler(.success(()))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    func getLocalSharingInfo(forAssetIdentifier globalIdentifier: String,
                             for users: [any SHServerUser],
                             completionHandler: @escaping (Result<SHShareableEncryptedAsset?, Error>) -> ()) {
        self.localServer.getSharingInfo(forAssetIdentifier: globalIdentifier, for: users, completionHandler: completionHandler)
    }
    
    func share(_ asset: SHShareableEncryptedAsset,
               isPhotoMessage: Bool,
               suppressNotification: Bool = false,
               completionHandler: @escaping (Result<Void, Error>) -> ()) {
        self.remoteServer.share(
            asset: asset,
            isPhotoMessage: isPhotoMessage,
            suppressNotification: suppressNotification,
            completionHandler: completionHandler
        )
    }
}


extension SHServerProxy {
    
    // - MARK: Groups, Threads and Interactions
    
    public func add(phoneNumbers: [SHPhoneNumber],
                    to groupId: String,
                    completionHandler: @escaping (Result<Void, Error>) -> ()) {
        self.remoteServer.add(phoneNumbers: phoneNumbers, to: groupId, completionHandler: completionHandler)
    }
    
    func setupGroupEncryptionDetails(
        groupId: String,
        recipientsEncryptionDetails: [RecipientEncryptionDetailsDTO],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        log.trace("saving encryption details for group \(groupId) to local server")
        log.debug("[setupGroup] \(recipientsEncryptionDetails.map({ ($0.encryptedSecret, $0.ephemeralPublicKey, $0.secretPublicSignature) }))")
        /// Save the encryption details for this user on local
        self.remoteServer.setGroupEncryptionDetails(
            groupId: groupId,
            recipientsEncryptionDetails: recipientsEncryptionDetails
        ) { remoteResult in
            switch remoteResult {
            case .success:
                log.trace("encryption details for group \(groupId) saved to remote server. Updating local server")
                /// Save the encryption details for all users on server
                self.localServer.setGroupEncryptionDetails(
                    groupId: groupId,
                    recipientsEncryptionDetails: recipientsEncryptionDetails,
                    completionHandler: completionHandler
                )
            case .failure(let error):
                log.error("failed to create group with encryption details locally: \(error.localizedDescription)")
                completionHandler(.failure(error))
            }
        }
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
        completionHandler: @escaping (Result<ConversationThreadOutputDTO, Error>) -> ()
    ) {
        if let recipientsEncryptionDetails {
            log.trace("creating or updating thread with with users with ids \(recipientsEncryptionDetails.map({ $0.recipientUserIdentifier }))")
            log.debug("[setupThread] \(recipientsEncryptionDetails.map({ ($0.encryptedSecret, $0.ephemeralPublicKey, $0.secretPublicSignature, $0.senderPublicSignature) }))")
        }
        self.remoteServer.createOrUpdateThread(
            name: name,
            recipientsEncryptionDetails: recipientsEncryptionDetails
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
        withUsers users: [any SHServerUser],
        completionHandler: @escaping (Result<ConversationThreadOutputDTO?, Error>) -> ()
    ) {
        self.localServer.getThread(withUsers: users) { localResult in
            switch localResult {
            case .failure:
                self.remoteServer.getThread(withUsers: users, completionHandler: completionHandler)
            case .success(let maybeThread):
                if let maybeThread {
                    completionHandler(.success(maybeThread))
                } else {
                    self.remoteServer.getThread(withUsers: users, completionHandler: completionHandler)
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
                /// 
                /// Cache thread assets for offline consumption
                ///
                Task(priority: .background) {
                    do {
                        try await self.localServer.cache(threadAssets, in: threadId)
                    } catch {
                        log.warning("failed to cache thread assets: \(error)")
                    }
                }
                
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
    
    internal func retrieveUserEncryptionDetails(
        forGroup groupId: String,
        completionHandler: @escaping (Result<RecipientEncryptionDetailsDTO?, Error>) -> ()
    ) {
        self.localServer.retrieveUserEncryptionDetails(forGroup: groupId) { localE2EEResult in
            if case .success(let localSelfDetails) = localE2EEResult, let localSelfDetails {
                completionHandler(.success(localSelfDetails))
            } else {
                if case .failure(let error) = localE2EEResult {
                    log.warning("failed to retrieve <SELF> E2EE details for group \(groupId) from local: \(error.localizedDescription)")
                }
                self.remoteServer.retrieveUserEncryptionDetails(forGroup: groupId) { remoteE2EEResult in
                    switch remoteE2EEResult {
                    case .success(let remoteSelfDetails):
                        if let remoteSelfDetails {
                            self.localServer.setGroupEncryptionDetails(
                                groupId: groupId,
                                recipientsEncryptionDetails: [remoteSelfDetails]
                            ) { _ in
                                completionHandler(.success(remoteSelfDetails))
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
    
    internal func topLevelLocalInteractionsSummary(
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
    internal func updateLastUpdatedAt(
        from threads: [ConversationThreadOutputDTO]
    ) async throws {
        guard threads.isEmpty == false else {
            return
        }
        
        return try await withUnsafeThrowingContinuation { continuation in
            self.localServer.updateLastUpdatedAt(
                with: threads
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
    public func syncLocalGraphWithServer(
        dryRun: Bool = true,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        self.localServer.syncLocalGraphWithServer(dryRun: dryRun, completionHandler: completionHandler)
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
