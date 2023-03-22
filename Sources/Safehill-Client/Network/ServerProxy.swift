import Foundation
import Yams


public struct SHServerProxy {
    
    let localServer: LocalServer
    let remoteServer: SHServerHTTPAPI
    
    public init(user: SHLocalUser) {
        self.localServer = LocalServer(requestor: user)
        self.remoteServer = SHServerHTTPAPI(requestor: user)
    }
    
}


// MARK: - Migrations
extension SHServerProxy {
    
    public func runLocalMigrations(completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        self.localServer.runDataMigrations(completionHandler: completionHandler)
    }
    
}


// MARK: - Users & Devices
extension SHServerProxy {
    
    public func createUser(name: String,
                           completionHandler: @escaping (Swift.Result<SHServerUser, Error>) -> ()) {
        self.localServer.createUser(name: name) { result in
            switch result {
            case .success(_):
                self.remoteServer.createUser(name: name, completionHandler: completionHandler)
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    public func updateUser(email: String? = nil,
                           name: String? = nil,
                           password: String? = nil,
                           completionHandler: @escaping (Swift.Result<SHServerUser, Error>) -> ()) {
        self.localServer.updateUser(email: email, name: name) { result in
            switch result {
            case .success(_):
                self.remoteServer.updateUser(email: email, name: name, completionHandler: completionHandler)
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    public func signInWithApple(email: String,
                                name: String,
                                authorizationCode: Data,
                                identityToken: Data,
                                completionHandler: @escaping (Swift.Result<SHAuthResponse, Error>) -> ()) {
        self.remoteServer.signInWithApple(email: email,
                                          name: name,
                                          authorizationCode: authorizationCode,
                                          identityToken: identityToken) { result in
            switch result {
            case .success(let authResponse):
                self.localServer.getUsers(withIdentifiers: [self.localServer.requestor.identifier]) { result in
                    switch result {
                    case .success(let users):
                        guard users.count == 0 || users.count == 1 else {
                            completionHandler(.failure(SHHTTPError.ServerError.unexpectedResponse("Multiple users returned for identifier \(self.localServer.requestor.identifier)")))
                            return
                        }
                        
                        if users.count == 0 {
                            self.localServer.signInWithApple(email: email, name: name, authorizationCode: authorizationCode, identityToken: identityToken, completionHandler: completionHandler)
                        } else {
                            completionHandler(.success(authResponse))
                        }
                    case .failure(let err):
                        completionHandler(.failure(err))
                    }
                }
                
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    public func signIn(name: String, completionHandler: @escaping (Swift.Result<SHAuthResponse, Error>) -> ()) {
        self.remoteServer.signIn(name: name, completionHandler: completionHandler)
    }
    
    public func getUsers(withIdentifiers userIdentifiersToFetch: [String], completionHandler: @escaping (Swift.Result<[SHServerUser], Error>) -> ()) {
        guard userIdentifiersToFetch.count > 0 else {
            return completionHandler(.success([]))
        }
        
        self.remoteServer.getUsers(withIdentifiers: userIdentifiersToFetch) { result in
            switch result {
            case .success(let serverUsers):
                completionHandler(.success(serverUsers))
                for serverUser in serverUsers {
                    self.localServer.createUser(identifier: serverUser.identifier,
                                                name: serverUser.name,
                                                publicKeyData: serverUser.publicKeyData,
                                                publicSignatureData: serverUser.publicSignatureData) { result in
                        if case .failure(let failure) = result {
                            log.error("failed to create server user in local server: \(failure.localizedDescription)")
                        }
                    }
                }
            case .failure(let error):
                // If can't get from the server try to get them from the local cache
                self.localServer.getUsers(withIdentifiers: userIdentifiersToFetch) { localResult in
                    switch localResult {
                    case .success(let serverUsers):
                        if serverUsers.count == userIdentifiersToFetch.count {
                            completionHandler(localResult)
                        } else {
                            completionHandler(.failure(error))
                        }
                    case .failure(_):
                        completionHandler(.failure(error))
                    }
                }
            }
        }
    }
    
    public func searchUsers(query: String, completionHandler: @escaping (Swift.Result<[SHServerUser], Error>) -> ()) {
        self.remoteServer.searchUsers(query: query, completionHandler: completionHandler)
    }
    
    /// Fetch the local user details. If fails fall back to local cache if the server is unreachable or the token is expired
    /// - Parameters:
    ///   - completionHandler: the callback method
    public func fetchUserAccount(completionHandler: @escaping (Swift.Result<SHServerUser?, Error>) -> ()) {
        self.remoteServer.getUsers(withIdentifiers: [self.remoteServer.requestor.identifier]) { result in
            switch result {
            case .success(let users):
                completionHandler(.success(users.first))
            case .failure(let err):
                if err is URLError || err is SHHTTPError.TransportError {
                    // Can't connect to the server, get details from local cache
                    print("Failed to get user details from server. Using local cache\n \(err)")
                    self.fetchLocalUserAccount(originalServerError: err,
                                               completionHandler: completionHandler)
                } else {
                    completionHandler(.failure(err))
                }
            }
        }
    }
    
    public func fetchLocalUserAccount(originalServerError: Error? = nil,
                                      completionHandler: @escaping (Swift.Result<SHServerUser?, Error>) -> ()) {
        self.localServer.getUsers(withIdentifiers: [self.remoteServer.requestor.identifier]) { result in
            switch result {
            case .success(let users):
                if users.count == 0 {
                    completionHandler(.failure(SHHTTPError.ClientError.notFound))
                } else {
                    completionHandler(.success(users.first))
                }
            case .failure(let err):
                completionHandler(.failure(originalServerError ?? err))
            }
        }
    }
    
    public func deleteAccount(completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        self.remoteServer.deleteAccount { result in
            if case .failure(let err) = result {
                completionHandler(.failure(err))
                return
            }
            self.localServer.deleteAccount(completionHandler: completionHandler)
        }
    }
    
    public func deleteAccount(name: String, password: String, completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        self.remoteServer.deleteAccount(name: name, password: password) { result in
            if case .failure(let err) = result {
                completionHandler(.failure(err))
                return
            }
            self.localServer.deleteAccount(completionHandler: completionHandler)
        }
    }
    
    public func deleteLocalAccount(completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        self.localServer.deleteAccount(completionHandler: completionHandler)
    }
    
    public func registerDevice(_ deviceName: String, token: String, completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        self.remoteServer.registerDevice(deviceName, token: token, completionHandler: completionHandler)
    }
}
    
// MARK: - Assets
extension SHServerProxy {
    
    func getLocalAssetDescriptors(originalServerError: Error? = nil,
                                  completionHandler: @escaping (Swift.Result<[SHAssetDescriptor], Error>) -> ()) {
        self.localServer.getAssetDescriptors { result in
            switch result {
            case .failure(let err):
                completionHandler(.failure(originalServerError ?? err))
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
    func getRemoteAssetDescriptors(completionHandler: @escaping (Swift.Result<[SHAssetDescriptor], Error>) -> ()) {
        
        self.remoteServer.getAssetDescriptors { serverResult in
            switch serverResult {
            case .failure(let serverError):
                if serverError is URLError || serverError is SHHTTPError.TransportError {
                    // Can't connect to the server, get descriptors from local cache
                    log.error("Failed to get descriptors from server. Using local descriptors. error=\(serverError.localizedDescription)")
                    self.getLocalAssetDescriptors(originalServerError: serverError, completionHandler: completionHandler)
                } else {
                    completionHandler(.failure(serverError))
                }
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
    }
    
    /// Fill the `.hiResolution` version of the requested assets in the local server (cache)
    /// - Parameter globalIdentifiers: the assets' global identifiers
    private func cacheAssets(with globalIdentifiers: [String],
                             quality: SHAssetQuality) {
        log.trace("[CACHING] Attempting to cache \(quality.rawValue) for assets \(globalIdentifiers)")
        let localUserId = self.localServer.requestor.identifier
        
        ///
        /// Get the remote descriptor from the remote server
        ///
        self.remoteServer.getAssetDescriptors(
            forAssetGlobalIdentifiers: globalIdentifiers
        ) { result in
            switch result {
            case .success(let descriptors):
                let descriptorsByGlobalIdentifier = descriptors.reduce([String: any SHAssetDescriptor]()) {
                    partialResult, descriptor in
                    var result = partialResult
                    result[descriptor.globalIdentifier] = descriptor
                    return result
                }
                
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
                            log.trace("[CACHING] No \(quality.rawValue) for assets \(globalIdentifiers) on remote server")
                            return
                        }
                        
                        for (globalIdentifier, encryptedAsset) in encryptedDict {
                            guard let descriptor = descriptorsByGlobalIdentifier[globalIdentifier] else {
                                log.error("[CACHING] Could not find descriptor for asset \(globalIdentifier)")
                                continue
                            }
                            
                            guard let groupId = descriptor.sharingInfo.sharedWithUserIdentifiersInGroup[localUserId] else {
                                log.error("[CACHING] Could not determine group id for asset \(globalIdentifier)")
                                continue
                            }

                            ///
                            /// Store the asset in the local server (cache)
                            ///
                            self.localServer.create(
                                assets: [encryptedAsset],
                                groupId: groupId,
                                senderUserIdentifier: descriptor.sharingInfo.sharedByUserIdentifier
                            ) {
                                result in
                                switch result {
                                case .success(_):
                                    log.trace("[CACHING] Downloaded and cached \(quality.rawValue) for asset \(globalIdentifier)")
                                case .failure(_):
                                    log.error("[CACHING] Unable to save asset \(globalIdentifier) to local server")
                                }
                            }
                        }
                    case .failure(_):
                        log.error("[CACHING] Unable to get assets \(globalIdentifiers) from remote server")
                    }
                }
            case .failure(_):
                log.error("[CACHING] Unable to get asset descriptors \(globalIdentifiers) from remote server")
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
                        completionHandler: @escaping (Swift.Result<[String: SHEncryptedAsset], Error>) -> ()) {
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
    /// Tries to fetch from local server first, then remote server if some are not present. For those, **it updates the local server (cache)**
    ///
    /// - Parameters:
    ///   - assetIdentifiers: the global asset identifiers to retrieve
    ///   - versions: filter asset version (retrieve just the low res asset or the hi res asset, for instance)
    ///   - completionHandler: the callback, returning the SHEncryptedAsset objects keyed by asset identifier. Note that the output object might not have the same number of assets requested, as some of them might be deleted on the server
    ///
    func getAssets(withGlobalIdentifiers assetIdentifiers: [String],
                   versions: [SHAssetQuality],
                   completionHandler: @escaping (Swift.Result<[String: SHEncryptedAsset], Error>) -> ()) {
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
            withGlobalIdentifiers: assetIdentifiers,
            versions: Array(versions),
            cacheHiResolution: false
        ) { localResult in
            if case .success(let assetsDict) = localResult {
                localDictionary = assetsDict
            }
            group.leave()
        }
        
        guard group.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds)) == .success else {
            completionHandler(.failure(SHHTTPError.TransportError.timedOut))
            return
        }
        
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
        self.remoteServer.getAssetDescriptors(forAssetGlobalIdentifiers: assetIdentifiersToFetch) {
            result in
            switch result {
            case .success(let descriptors):
                descriptorsByAssetGlobalId = descriptors
                    .filter { descriptor in
                        descriptor.uploadState == .completed
                        && descriptor.sharingInfo.sharedWithUserIdentifiersInGroup[self.localServer.requestor.identifier] != nil
                    }
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
        
        guard group.wait(timeout: .now() + .milliseconds(SHDefaultNetworkTimeoutInMilliseconds)) == .success else {
            completionHandler(.failure(SHHTTPError.TransportError.timedOut))
            return
        }
        
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
        
        ///
        /// Organize assetId by the groupId id used to share with this user (remember, non-shared-backups result in an asset shared with self)
        ///
        var assetIdsByGroupId = [String: [String]]()
        var groupIdToSenderId = [String: String]()
        for (assetId, descriptor) in descriptorsByAssetGlobalId {
            let groupId = descriptor.sharingInfo.sharedWithUserIdentifiersInGroup[self.localServer.requestor.identifier]!
            if assetIdsByGroupId[groupId] == nil {
                assetIdsByGroupId[groupId] = [assetId]
            } else {
                assetIdsByGroupId[groupId]!.append(assetId)
            }
            
            ///
            /// Assume each group has only one sender
            ///
            let senderId = descriptor.sharingInfo.sharedByUserIdentifier
            groupIdToSenderId[groupId] = senderId
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
        
        guard group.wait(timeout: .now() + .milliseconds(SHDefaultNetworkTimeoutInMilliseconds)) == .success else {
            completionHandler(.failure(SHHTTPError.TransportError.timedOut))
            return
        }
        guard error == nil else {
            completionHandler(.failure(error!))
            return
        }
        
        ///
        /// Update the local cache with retrieved assets from server, using the proper group identifier
        ///
        
        for (groupId, assetIds) in assetIdsByGroupId {
            let encryptedAssets = remoteDictionary.compactMap { (key: String, value: any SHEncryptedAsset) in
                if assetIds.contains(key) { return value }
                else { return nil }
            }
            
            guard let senderId = groupIdToSenderId[groupId] else {
                log.error("could not save downloaded server asset to the local cache because sender could not be retrieved. This operation will be attempted again, but for now the cache is out of sync.")
                continue
            }
            
            group.enter()
            self.createLocalAssets(encryptedAssets,
                                   groupId: groupId,
                                   senderUserIdentifier: senderId)
            { localResult in
                if case .failure(let err) = localResult {
                    log.error("could not save downloaded server asset to the local cache. This operation will be attempted again, but for now the cache is out of sync. error=\(err.localizedDescription)")
                }
                group.leave()
            }
        }
        
        guard group.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds * assetIdsByGroupId.count)) == .success else {
            completionHandler(.failure(SHHTTPError.TransportError.timedOut))
            return
        }
        
        completionHandler(.success(localDictionary.merging(remoteDictionary, uniquingKeysWith: { _, server in server })))
    }
    
    func createLocalAssets(_ assets: [any SHEncryptedAsset],
                           groupId: String,
                           senderUserIdentifier: String,
                           completionHandler: @escaping (Swift.Result<[SHServerAsset], Error>) -> ()) {
        log.info("Creating local assets \(assets.map { $0.globalIdentifier })")
        
        self.localServer.create(assets: assets,
                                groupId: groupId,
                                senderUserIdentifier: senderUserIdentifier,
                                completionHandler: completionHandler)
    }
    
    func createRemoteAssets(_ assets: [any SHEncryptedAsset],
                            groupId: String,
                            filterVersions: [SHAssetQuality]? = nil,
                            completionHandler: @escaping (Swift.Result<[SHServerAsset], Error>) -> ()) {
        log.info("Creating server assets \(assets.map { $0.globalIdentifier })")
        
        self.remoteServer.create(assets: assets,
                                 groupId: groupId,
                                 filterVersions: filterVersions,
                                 completionHandler: completionHandler)
    }
    
    func upload(serverAsset: SHServerAsset,
                asset: any SHEncryptedAsset,
                filterVersions: [SHAssetQuality]? = nil,
                completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        self.remoteServer.upload(serverAsset: serverAsset, asset: asset, filterVersions: filterVersions) { result in
            switch result {
            case .success():
                self.localServer.upload(serverAsset: serverAsset, asset: asset, filterVersions: filterVersions, completionHandler: completionHandler)
            case .failure(let error):
                log.critical("failed to mark asset as uploaded on the server. This asset is not marked as backed up: \(error.localizedDescription)")
                // TODO: wanna retry later? Or the server should have a background process to update these states from S3?
                completionHandler(.failure(error))
            }
            
        }
    }
    
    public func deleteAssets(withGlobalIdentifiers globalIdentifiers: [String],
                             completionHandler: @escaping (Result<[String], Error>) -> ()) {
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
    
    public func deleteAllLocalAssets(completionHandler: @escaping (Swift.Result<[String], Error>) -> ()) {
        self.localServer.deleteAllAssets(completionHandler: completionHandler)
    }
    
    func shareAssetLocally(_ asset: SHShareableEncryptedAsset,
                           completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
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
                             for users: [SHServerUser],
                             completionHandler: @escaping (Swift.Result<SHShareableEncryptedAsset?, Error>) -> ()) {
        self.localServer.getSharingInfo(forAssetIdentifier: globalIdentifier, for: users, completionHandler: completionHandler)
    }
    
    func share(_ asset: SHShareableEncryptedAsset,
               completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        self.remoteServer.share(asset: asset, completionHandler: completionHandler)
    }
}


// MARK: - Subscriptions
extension SHServerProxy {
    public func validateTransaction(originalTransactionId: String,
                                    receipt: String,
                                    productId: String,
                                    completionHandler: @escaping (Result<SHReceiptValidationResponse, Error>) -> ()) {
        let group = DispatchGroup()
        var localResult: Result<SHReceiptValidationResponse, Error>? = nil
        var serverResult: Result<SHReceiptValidationResponse, Error>? = nil
        
        group.enter()
        self.localServer.validateTransaction(originalTransactionId: originalTransactionId,
                                             receipt: receipt,
                                             productId: productId) { result in
            localResult = result
            group.leave()
        }
        
        group.enter()
        self.remoteServer.validateTransaction(originalTransactionId: originalTransactionId,
                                              receipt: receipt,
                                              productId: productId) { result in
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
                completionHandler(.failure(serverErr))
            }
        }
    }
}
