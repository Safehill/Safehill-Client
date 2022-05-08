//
//  ServerProxy.swift
//  Enkey
//
//  Created by Gennaro Frazzingaro on 9/5/21.
//

import Foundation

public struct SHServerProxy {
    
    let localServer: LocalServer
    let remoteServer: SHServerHTTPAPI
    
    public init(user: SHLocalUser) {
        self.localServer = LocalServer(requestor: user)
        self.remoteServer = SHServerHTTPAPI(requestor: user)
    }
    
    public func createUser(email: String,
                           name: String,
                           password: String,
                           completionHandler: @escaping (Swift.Result<SHServerUser, Error>) -> ()) {
        self.localServer.createUser(email: email, name: name, password: "") { result in
            switch result {
            case .success(_):
                self.remoteServer.createUser(email: email, name: name, password: password, completionHandler: completionHandler)
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    public func updateUser(email: String? = nil,
                           name: String? = nil,
                           password: String? = nil,
                           completionHandler: @escaping (Swift.Result<SHServerUser, Error>) -> ()) {
        self.localServer.updateUser(email: email, name: name, password: "") { result in
            switch result {
            case .success(_):
                self.remoteServer.updateUser(email: email, name: name, password: password, completionHandler: completionHandler)
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
    
    public func signIn(email: String?, password: String, completionHandler: @escaping (Swift.Result<SHAuthResponse, Error>) -> ()) {
        self.remoteServer.signIn(email: email, password: password, completionHandler: completionHandler)
    }
    
    public func getUsers(withIdentifiers userIdentifiers: [String], completionHandler: @escaping (Swift.Result<[SHServerUser], Error>) -> ()) {
        self.remoteServer.getUsers(withIdentifiers: userIdentifiers, completionHandler: completionHandler)
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
    
    public func deleteAccount(email: String, password: String, completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        self.remoteServer.deleteAccount(email: email, password: password) { result in
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
            case .failure(let err):
                switch err {
                case is SHHTTPError.TransportError:
                    // Can't connect to the server, get descriptors from local cache
                    print("Failed to get descriptors from server. Using local descriptors\n \(err)")
                    self.getLocalAssetDescriptors(originalServerError: err, completionHandler: completionHandler)
                default:
                    completionHandler(.failure(err))
                }
            case .success(let descriptors):
                serverDescriptors = descriptors
                completionHandler(.success(descriptors))
                
                self.getLocalAssetDescriptors { localResult in
                    if case .success(let descriptors) = localResult {
                        guard descriptors.count == serverDescriptors.count,
                              descriptors.map({ d in d.globalIdentifier }).elementsEqual(
                                serverDescriptors.map { d in d.globalIdentifier }
                              ) else {
                                  // TODO: Remove all non-existent server descriptors from cache
                                  return
                        }
                    }
                }
            }
        }
    }
    
    public func getLowResAssets(withGlobalIdentifiers assetIdentifiers: [String], completionHandler: @escaping (Swift.Result<[String: SHEncryptedAsset], Error>) -> ()) {
        if assetIdentifiers.count == 0 {
            completionHandler(.success([:]))
            return
        }
        
        self.localServer.getAssets(withGlobalIdentifiers: assetIdentifiers, quality: .lowResolution) { localResult in
            let localDictionary: [String: SHEncryptedAsset] = [:]
            var assetIdentifiersToFetch = assetIdentifiers
            if case .success(let assetsDict) = localResult {
                if assetsDict.keys.count == assetIdentifiers.count {
                    completionHandler(.success(assetsDict))
                    return
                } else {
                    assetIdentifiersToFetch = Array(Set(assetsDict.keys).symmetricDifference(assetIdentifiers))
                }
            }
         
            self.remoteServer.getAssets(withGlobalIdentifiers: assetIdentifiersToFetch, quality: .lowResolution) { serverResult in
                if assetIdentifiersToFetch == assetIdentifiers {
                    completionHandler(serverResult)
                    return
                }
                
                if case .success(let assetsDict) = localResult {
                    completionHandler(.success(localDictionary.merging(assetsDict, uniquingKeysWith: { _, svr in svr })))
                } else {
                    completionHandler(serverResult)
                }
            }
        }
    }
    
    public func getHiResAssets(withGlobalIdentifiers assetIdentifiers: [String], completionHandler: @escaping (Swift.Result<[String: SHEncryptedAsset], Error>) -> ()) {
        if assetIdentifiers.count == 0 {
            completionHandler(.success([:]))
            return
        }
        
        self.localServer.getAssets(withGlobalIdentifiers: assetIdentifiers, quality: .hiResolution) { localResult in
            let localDictionary: [String: SHEncryptedAsset] = [:]
            var assetIdentifiersToFetch = assetIdentifiers
            if case .success(let assetsDict) = localResult {
                if assetsDict.keys.count == assetIdentifiers.count {
                    completionHandler(.success(assetsDict))
                    return
                } else {
                    assetIdentifiersToFetch = Array(Set(assetsDict.keys).symmetricDifference(assetIdentifiers))
                }
            }
         
            self.remoteServer.getAssets(withGlobalIdentifiers: assetIdentifiersToFetch, quality: .hiResolution) { serverResult in
                if assetIdentifiersToFetch == assetIdentifiers {
                    completionHandler(serverResult)
                    return
                }
                
                if case .success(let assetsDict) = localResult {
                    completionHandler(.success(localDictionary.merging(assetsDict, uniquingKeysWith: { _, svr in svr })))
                } else {
                    completionHandler(serverResult)
                }
            }
        }
    }
    
    public func storeAssetLocally(lowResAsset: SHEncryptedAsset,
                                  hiResAsset: SHEncryptedAsset,
                                  completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        self.localServer.createAsset(lowResAsset: lowResAsset, hiResAsset: hiResAsset) {
            result in
            switch result {
            case .success(_):
                completionHandler(.success(()))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    public func createAsset(lowResAsset: SHEncryptedAsset,
                            hiResAsset: SHEncryptedAsset,
                            completionHandler: @escaping (Swift.Result<SHServerAsset, Error>) -> ()) {
        guard lowResAsset.globalIdentifier == hiResAsset.globalIdentifier &&
                lowResAsset.localIdentifier == hiResAsset.localIdentifier else {
            completionHandler(.failure(SHHTTPError.ClientError.badRequest("identifiers for both low and hi resolution assets need to match")))
            return
        }
        
        self.remoteServer.createAsset(lowResAsset: lowResAsset, hiResAsset: hiResAsset) { result in
            completionHandler(result)
            if case .success(_) = result {
                self.storeAssetLocally(lowResAsset: lowResAsset, hiResAsset: hiResAsset) { result in
                    if case .failure(let err) = result {
                        print("Asset was stored to server but not in the local cache: \(err)")
                    }
                }
            }
        }
    }
    
    public func uploadLowResAsset(assetVersion: SHServerAssetVersion,
                                  encryptedAsset: SHEncryptedAsset,
                                  completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        self.remoteServer.uploadLowResAsset(serverAssetVersion: assetVersion, encryptedAsset: encryptedAsset, completionHandler: completionHandler)
    }
    
    public func uploadHiResAsset(assetVersion: SHServerAssetVersion,
                                 encryptedAsset: SHEncryptedAsset,
                                 completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        self.remoteServer.uploadHiResAsset(serverAssetVersion: assetVersion, encryptedAsset: encryptedAsset, completionHandler: completionHandler)
    }
    
    public func deleteAssets(withGlobalIdentifiers globalIdentifiers: [String], completionHandler: @escaping (Result<[String], Error>) -> ()) {
        self.remoteServer.deleteAssets(withGlobalIdentifiers: globalIdentifiers) { result in
            switch result {
            case .success(_):
                self.localServer.deleteAssets(withGlobalIdentifiers: globalIdentifiers) { result in
                    if case .failure(let err) = result {
                        print("Asset was deleted on server but not from the local cache: \(err)")
                    }
                }
                completionHandler(result)
            
            case .failure(let err):
                log.error("asset deletion failed. Error: \(err.localizedDescription)")
                completionHandler(.failure(err))
            }
        }
    }
}
