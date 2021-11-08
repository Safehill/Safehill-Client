//
//  ServerProxy.swift
//  Enkey
//
//  Created by Gennaro Frazzingaro on 9/5/21.
//

import Foundation

public enum SHHTTPError{
    public enum ClientError : Error {
        case badRequest(String)
        
        public func toCode() -> Int {
            return 400
        }
    }
    
    public enum ServerError : Error {
        case generic(String)
        case notImplemented
        case outdatedKeys
        case noData
        
        public func toCode() -> Int {
            switch self {
            case .notImplemented:
                return 501
            default:
                return 500
            }
        }
    }
    
    public enum TransportError : Error {
        case generic(Error)
    }
}

public struct SHServerProxy {
    
    let localServer: LocalServer
    let remoteServer: SHServerHTTPAPI
    
    public init(user: SHLocalUser) {
        self.localServer = LocalServer(requestor: user)
        self.remoteServer = SHServerHTTPAPI(requestor: user)
    }
    
    public func createUser(completionHandler: @escaping (Swift.Result<SHServerUser?, Error>) -> ()) {
        self.remoteServer.createUser { result in
            switch result {
            case .success(_):
                self.localServer.createUser(completionHandler: completionHandler)
            case .failure(let err):
                // TODO: Remove this fallback when the server API is implemented
                if case SHHTTPError.ServerError.notImplemented = err {
                    self.localServer.createUser(completionHandler: completionHandler)
                } else {
                    completionHandler(.failure(err))
                }
            }
        }
    }
    
    public func sendAuthenticationCode(completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        self.remoteServer.sendAuthenticationCode(completionHandler: completionHandler)
    }
    
    public func validateAuthenticationCode(completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        self.remoteServer.validateAuthenticationCode(completionHandler: completionHandler)
    }
    
    public func getUsers(withIdentifiers userIdentifiers: [String], completionHandler: @escaping (Swift.Result<[SHServerUser], Error>) -> ()) {
        self.remoteServer.getUsers(withIdentifiers: userIdentifiers, completionHandler: completionHandler)
    }
    
    public func getLocalAssetDescriptors(completionHandler: @escaping (Swift.Result<[SHAssetDescriptor], Error>) -> ()) {
        self.localServer.getAssetDescriptors(completionHandler: completionHandler)
    }
    
    public func getAssetDescriptors(completionHandler: @escaping (Swift.Result<[SHAssetDescriptor], Error>) -> ()) {
        
        var serverDescriptors = [SHAssetDescriptor]()
        
        self.remoteServer.getAssetDescriptors { serverResult in
            switch serverResult {
            case .failure(let err):
                print("Failed to get descriptors from server. Using local descriptors\n \(err)")
                self.getLocalAssetDescriptors(completionHandler: completionHandler)
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
        
        self.localServer.getLowResAssets(withGlobalIdentifiers: assetIdentifiers) { localResult in
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
         
            self.remoteServer.getLowResAssets(withGlobalIdentifiers: assetIdentifiersToFetch) { serverResult in
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
        
        self.localServer.getHiResAssets(withGlobalIdentifiers: assetIdentifiers) { localResult in
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
         
            self.remoteServer.getHiResAssets(withGlobalIdentifiers: assetIdentifiersToFetch) { serverResult in
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
        self.localServer.storeAsset(lowResAsset: lowResAsset, hiResAsset: hiResAsset, completionHandler: completionHandler)
    }
    
    public func storeAsset(lowResAsset: SHEncryptedAsset,
                           hiResAsset: SHEncryptedAsset,
                           completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        guard lowResAsset.globalIdentifier == hiResAsset.globalIdentifier &&
                lowResAsset.localIdentifier == hiResAsset.localIdentifier else {
            completionHandler(.failure(SHHTTPError.ClientError.badRequest("identifiers for both low and hi resolution assets need to match")))
            return
        }
        
        self.remoteServer.storeAsset(lowResAsset: lowResAsset, hiResAsset: hiResAsset) { result in
            completionHandler(result)
            if case .success() = result {
                self.storeAssetLocally(lowResAsset: lowResAsset, hiResAsset: hiResAsset) { result in
                    if case .failure(let err) = result {
                        print("Asset was stored to server but not in the local cache: \(err)")
                    }
                }
            }
        }
    }
    
    public func deleteAssets(withGlobalIdentifiers globalIdentifiers: [String], completionHandler: @escaping (Result<[String], Error>) -> ()) {
        self.remoteServer.deleteAssets(withGlobalIdentifiers: globalIdentifiers) { result in
            completionHandler(result)
            if case .success(_) = result {
                self.localServer.deleteAssets(withGlobalIdentifiers: globalIdentifiers) { result in
                    if case .failure(let err) = result {
                        print("Asset was deleted on server but not from the local cache: \(err)")
                    }
                }
            }
        }
    }
}
