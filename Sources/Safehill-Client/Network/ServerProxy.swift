import Foundation
import Yams


public struct SHServerProxy {
    
    let localServer: LocalServer
    let remoteServer: SHServerHTTPAPI
    
    public init(user: SHLocalUser) {
        self.localServer = LocalServer(requestor: user)
        self.remoteServer = SHServerHTTPAPI(requestor: user)
    }
    
    public func runLocalMigrations(completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        self.localServer.runDataMigrations(completionHandler: completionHandler)
    }
    
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
    
    public func getLocalAssetDescriptors(originalServerError: Error? = nil,
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
                    log.debug("[DESCRIPTORS] from local server: \(descriptors.count)")
                } else {
                    log.debug("[DESCRIPTORS] from local server: empty")
                }
#endif
                completionHandler(result)
            }
        }
    }
    
    /// Get all visible asset descriptors to this user. Fall back to local descriptor if server is unreachable
    /// - Parameter completionHandler: the callback method
    public func getRemoteAssetDescriptors(completionHandler: @escaping (Swift.Result<[SHAssetDescriptor], Error>) -> ()) {
        
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
                    log.debug("[DESCRIPTORS] from remote server: \(descriptors.count)")
                } else {
                    log.debug("[DESCRIPTORS] from remote server: empty")
                }
#endif
                completionHandler(.success(descriptors))
            }
        }
    }
    
    public func getLocalAssets(withGlobalIdentifiers assetIdentifiers: [String],
                               versions: [SHAssetQuality],
                               completionHandler: @escaping (Swift.Result<[String: SHEncryptedAsset], Error>) -> ()) {
        self.localServer.getAssets(withGlobalIdentifiers: assetIdentifiers,
                                   versions: versions,
                                   completionHandler: completionHandler)
    }
    
    public func getLocalDecryptedAssets(withGlobalIdentifiers assetIdentifiers: [String],
                                        versions: [SHAssetQuality],
                                        completionHandler: @escaping (Swift.Result<[String: SHDecryptedAsset], Error>) -> ()) {
        self.localServer.getDecryptedAssets(withGlobalIdentifiers: assetIdentifiers,
                                            versions: versions,
                                            completionHandler: completionHandler)
    }
    
    ///
    /// Retrieves assets versions with given identifiers.
    /// Tries to fetch from local server first, then remote server if some are not present. For those, **it updates the local server (cache)**
    ///
    /// - Parameters:
    ///   - assetIdentifiers: the global asset identifiers to retrieve
    ///   - versions: filter asset version (retrieve just the low res asset or the hi res asset, for instance)
    ///   - saveLocallyWithSenderIdentifier: when saving assets in the local server mark this asset as shared by this user public identifier
    ///   - completionHandler: the callback, returning the SHEncryptedAsset objects keyed by asset identifier. Note that the output object might not have the same number of assets requested, as some of them might be deleted on the server
    ///
    public func getAssets(withGlobalIdentifiers assetIdentifiers: [String],
                          versions: [SHAssetQuality],
                          saveLocallyWithSenderIdentifier senderUserIdentifier: String,
                          completionHandler: @escaping (Swift.Result<[String: SHEncryptedAsset], Error>) -> ()) {
        if assetIdentifiers.count == 0 {
            completionHandler(.success([:]))
            return
        }
        
        var localDictionary: [String: any SHEncryptedAsset] = [:]
        var assetIdentifiersToFetch = assetIdentifiers
        
        let group = DispatchGroup()
        
        ///
        /// Get the asset from the local server cache
        /// Do this first to support offline access, and rely on the AssetDownloader to clean up local assets that were deleted on server.
        /// The right thing way to do this is to retrieve descriptors first and fetch local assets later, but that would not support offline.
        ///
        
        group.enter()
        self.getLocalAssets(withGlobalIdentifiers: assetIdentifiers,
                            versions: versions) { localResult in
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
        /// - filter out assets that haven't been uploaded yet. The call to the CDN would fail for those.
        /// - filter out the ones that are no longer shared with this user. In fact, it is ossible the client still asks for this asset, but it should not be fetched.
        /// - determine the groupId used to upload or share by/with this user. That is the groupId that should be saved with the asset sharing info by the `LocalServer`.
        ///
        
        var error: Error? = nil
        var descriptorsByAssetGlobalId: [String: any SHAssetDescriptor] = [:]
        
        group.enter()
        self.remoteServer.getAssetDescriptors { result in
            switch result {
            case .success(let descriptors):
                descriptorsByAssetGlobalId = descriptors
                    .filter { descriptor in
                        assetIdentifiersToFetch.contains(descriptor.globalIdentifier) && // TODO: Replace this with server filtering
                        descriptor.uploadState == .completed && descriptor.sharingInfo.sharedWithUserIdentifiersInGroup[self.localServer.requestor.identifier] != nil
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
        
        /// Reset the descriptors to fetch based on the server descriptors
        if assetIdentifiersToFetch.count != descriptorsByAssetGlobalId.count {
            log.warning("Some assets requested could not be found in the server manifest, shared with you. Skipping those")
        }
        assetIdentifiersToFetch = Array(descriptorsByAssetGlobalId.keys)
        
        /// Organize assetId by the groupId id used to share with this user (remember, uploads result in an asset shared with self)
        var assetIdsByGroupId = [String: [String]]()
        for (assetId, descriptor) in descriptorsByAssetGlobalId {
            let groupId = descriptor.sharingInfo.sharedWithUserIdentifiersInGroup[self.localServer.requestor.identifier]!
            if assetIdsByGroupId[groupId] == nil {
                assetIdsByGroupId[groupId] = [assetId]
            } else {
                assetIdsByGroupId[groupId]!.append(assetId)
            }
        }
        
        ///
        /// Get the asset from the remote Safehill server.
        ///
        
        var remoteDictionary: [String: any SHEncryptedAsset] = [:]
        
        group.enter()
        self.remoteServer.getAssets(withGlobalIdentifiers: assetIdentifiersToFetch,
                                    versions: versions) { serverResult in
            switch serverResult {
            case .success(let assetsDict):
                guard assetsDict.count > 0 else {
                    log.error("No assets with globalIdentifiers \(assetIdentifiersToFetch)")
                    break
                }
                remoteDictionary = assetsDict
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
            let assets = remoteDictionary.compactMap { (key: String, value: any SHEncryptedAsset) in
                if assetIds.contains(key) { return value }
                else { return nil }
            }
            
            group.enter()
            self.createLocalAssets(assets,
                                   groupId: groupId,
                                   senderUserIdentifier: senderUserIdentifier)
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
    
    public func createLocalAssets(_ assets: [any SHEncryptedAsset],
                                  groupId: String,
                                  senderUserIdentifier: String,
                                  completionHandler: @escaping (Swift.Result<[SHServerAsset], Error>) -> ()) {
        log.info("Creating local assets \(assets.map { $0.globalIdentifier })")
        
        self.localServer.create(assets: assets,
                                groupId: groupId,
                                senderUserIdentifier: senderUserIdentifier,
                                completionHandler: completionHandler)
    }
    
    public func createRemoteAssets(_ assets: [any SHEncryptedAsset],
                                   groupId: String,
                                   completionHandler: @escaping (Swift.Result<[SHServerAsset], Error>) -> ()) {
        log.info("Creating server assets \(assets.map { $0.globalIdentifier })")
        
        self.remoteServer.create(assets: assets,
                                 groupId: groupId,
                                 completionHandler: completionHandler)
    }
    
    public func upload(serverAsset: SHServerAsset,
                       asset: any SHEncryptedAsset,
                       completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        self.remoteServer.upload(serverAsset: serverAsset, asset: asset) { result in
            switch result {
            case .success():
                self.localServer.upload(serverAsset: serverAsset, asset: asset, completionHandler: completionHandler)
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
    
    public func shareAssetLocally(_ asset: SHShareableEncryptedAsset,
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
    
    public func getLocalSharingInfo(forAssetIdentifier globalIdentifier: String,
                                    for users: [SHServerUser],
                                    completionHandler: @escaping (Swift.Result<SHShareableEncryptedAsset?, Error>) -> ()) {
        self.localServer.getSharingInfo(forAssetIdentifier: globalIdentifier, for: users, completionHandler: completionHandler)
    }
    
    public func share(_ asset: SHShareableEncryptedAsset,
                      completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        self.remoteServer.share(asset: asset, completionHandler: completionHandler)
    }
}

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
