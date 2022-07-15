import Foundation

public var SHServerUserInMemoryCache = [String: SHServerUser]()

public struct SHServerProxy {
    
    let localServer: LocalServer
    let remoteServer: SHServerHTTPAPI
    
    public init(user: SHLocalUser) {
        self.localServer = LocalServer(requestor: user)
        self.remoteServer = SHServerHTTPAPI(requestor: user)
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
    
    public func getUsers(withIdentifiers userIdentifiers: [String], completionHandler: @escaping (Swift.Result<[SHServerUser], Error>) -> ()) {
        let userIdentifiers = Set(userIdentifiers)
        guard userIdentifiers.count > 0 else {
            return completionHandler(.success([]))
        }
        
        var response = [SHServerUser]()
        for userIdentifier in userIdentifiers {
            if let serverUser = SHServerUserInMemoryCache[userIdentifier] {
                response.append(serverUser)
            }
        }
        
        let userIdentifiersToFetch = Array(userIdentifiers).subtract(response.map({$0.identifier}))
        guard userIdentifiersToFetch.count > 0 else {
            return completionHandler(.success(response))
        }
        
        self.remoteServer.getUsers(withIdentifiers: userIdentifiersToFetch) { result in
            switch result {
            case .success(let serverUsers):
                for serverUser in serverUsers {
                    SHServerUserInMemoryCache[serverUser.identifier] = serverUser
                }
                response.append(contentsOf: serverUsers)
                completionHandler(.success(response))
            case .failure(let error):
                completionHandler(.failure(error))
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
                switch err {
                case is SHHTTPError.TransportError:
                    // Can't connect to the server, get details from local cache
                    print("Failed to get user details from server. Using local cache\n \(err)")
                    self.fetchLocalUserAccount(originalServerError: err,
                                               completionHandler: completionHandler)
                default:
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
        // No need to check email and password for local accounts
        self.localServer.deleteAccount(completionHandler: completionHandler)
    }
    
    public func getLocalAssetDescriptors(originalServerError: Error? = nil,
                                         completionHandler: @escaping (Swift.Result<[SHAssetDescriptor], Error>) -> ()) {
        self.localServer.getAssetDescriptors { result in
            if case .failure(let err) = result {
                completionHandler(.failure(originalServerError ?? err))
                return
            }
            completionHandler(result)
        }
    }
    
    public func getAssetDescriptors(completionHandler: @escaping (Swift.Result<[SHAssetDescriptor], Error>) -> ()) {
        
        var serverDescriptors = [SHAssetDescriptor]()
        
        self.remoteServer.getAssetDescriptors { serverResult in
            switch serverResult {
            case .failure(let serverError):
                switch serverError {
                case is SHHTTPError.TransportError:
                    // Can't connect to the server, get descriptors from local cache
                    log.error("Failed to get descriptors from server. Using local descriptors. error=\(serverError.localizedDescription)")
                    self.getLocalAssetDescriptors(originalServerError: serverError, completionHandler: completionHandler)
                default:
                    completionHandler(.failure(serverError))
                }
            case .success(let descriptors):
                serverDescriptors = descriptors
                completionHandler(.success(descriptors))
                
                self.getLocalAssetDescriptors { localResult in
                    if case .success(let descriptors) = localResult {
                        guard
                            descriptors.count == serverDescriptors.count,
                            descriptors.map({ d in d.globalIdentifier }).elementsEqual(serverDescriptors.map { d in d.globalIdentifier })
                        else {
                            // TODO: The following code interferes with assets being uploaded, as it removes the details for the uploads in-flight.
                            // We store the assets and other details (like the secrets) in the SHUploadOperation to be picked up by the uploader.
                            // However, as the SHDownloadOperation runs in parallel, it has a chance to delete these details and makes the upload fail (`serverProxy.getLocalAssets` fails to retrieve the asset).
                            // The solution is to either store details for in-flight uploads somewhere else, or to mark them as in-flight and avoid removing them when calling deleteAssets below
                            return
                            
//                            ///
//                            /// Remove all non-existent server descriptors from local server
//                            ///
//                            let toRemoveLocally = descriptors.map({ d in d.globalIdentifier }).subtract(serverDescriptors.map({ d in d.globalIdentifier }))
//                            guard toRemoveLocally.count > 0 else {
//                                return
//                            }
//                            self.localServer.deleteAssets(withGlobalIdentifiers: Array(toRemoveLocally)) { result in
//                                if case .failure(let error) = result {
//                                    log.error("some assets were deleted on server but couldn't be deleted from lcoal cache. THis operation will be attempted again, but for now the cache is out of sync. error=\(error.localizedDescription)")
//                                }
//                            }
//                            return
                        }
                    }
                }
            }
        }
    }
    
    public func getLocalAssets(withGlobalIdentifiers assetIdentifiers: [String],
                               versions: [SHAssetQuality]?,
                               completionHandler: @escaping (Swift.Result<[String: SHEncryptedAsset], Error>) -> ()) {
        self.localServer.getAssets(withGlobalIdentifiers: assetIdentifiers,
                                   versions: versions,
                                   completionHandler: completionHandler)
    }
    
    /// Retrieves assets versions with given identifiers.
    /// Tries to fetch from local server first, then remote server if some are not present. For those, it updates the local server (cache)
    /// - Parameters:
    ///   - assetIdentifiers: the global asset identifiers to retrieve
    ///   - versions: filter asset version (retrieve just the low res asset or the hi res asset, for instance)
    ///   - saveLocallyAsOwnedByUserIdentifier: when saving assets in the local server mark this asset as shared by this user public identifier
    ///   - completionHandler: the callback
    public func getAssets(withGlobalIdentifiers assetIdentifiers: [String],
                          versions: [SHAssetQuality]?,
                          saveLocallyAsOwnedByUserIdentifier: String,
                          completionHandler: @escaping (Swift.Result<[String: SHEncryptedAsset], Error>) -> ()) {
        if assetIdentifiers.count == 0 {
            completionHandler(.success([:]))
            return
        }
        
        self.getLocalAssets(withGlobalIdentifiers: assetIdentifiers,
                            versions: versions) { localResult in
            var localDictionary: [String: SHEncryptedAsset] = [:]
            var assetIdentifiersToFetch = assetIdentifiers
            if case .success(let assetsDict) = localResult {
                localDictionary = assetsDict
                assetIdentifiersToFetch = assetIdentifiers.subtract(Array(assetsDict.keys))
                
                guard assetIdentifiersToFetch.count > 0 else {
                    completionHandler(localResult)
                    return
                }
            }
            
            self.remoteServer.getAssets(withGlobalIdentifiers: assetIdentifiersToFetch,
                                        versions: versions) { serverResult in
                switch serverResult {
                case .success(let assetsDict):
                    ///
                    /// Save retrieved assets to local server (cache)
                    ///
                    self.storeAssetsLocally(Array(assetsDict.values),
                                            senderUserIdentifier: saveLocallyAsOwnedByUserIdentifier)
                    { localResult in
                        if case .failure(let err) = localResult {
                            log.error("could not save downloaded server asset to the local cache. This operation will be attempted again, but for now the cache is out of sync. error=\(err.localizedDescription)")
                        }
                        completionHandler(.success(localDictionary.merging(assetsDict, uniquingKeysWith: { _, server in server })))
                    }
                case .failure(let error):
                    log.error("failed to get assets with globalIdentifiers \(assetIdentifiersToFetch): \(error.localizedDescription)")
                    completionHandler(serverResult)
                }
            }
        }
    }
    
    public func storeAssetsLocally(_ assets: [SHEncryptedAsset],
                                   senderUserIdentifier: String,
                                   completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        self.localServer.create(assets: assets, senderUserIdentifier: senderUserIdentifier) {
            result in
            switch result {
            case .success(_):
                completionHandler(.success(()))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    public func create(asset: SHEncryptedAsset,
                       completionHandler: @escaping (Swift.Result<SHServerAsset, Error>) -> ()) {
        log.info("Creating server asset \(asset.globalIdentifier)")
        
        self.remoteServer.create(assets: [asset]) { result in
            switch result {
            case .success(let assets):
                self.storeAssetsLocally([asset],
                                        senderUserIdentifier: self.localServer.requestor.identifier)
                { localResult in
                    switch localResult {
                    case .success(_):
                        completionHandler(.success(assets.first!))
                    case .failure(let err):
                        log.critical("asset was created on the server but not in the local cache. As the two servers are out of sync this can fail other operations downstream. error=\(err.localizedDescription)")
                        completionHandler(.success(assets.first!))
                    }
                }
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    public func upload(serverAsset: SHServerAsset,
                       asset: SHEncryptedAsset,
                       completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        self.remoteServer.upload(serverAsset: serverAsset, asset: asset, completionHandler: completionHandler)
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
