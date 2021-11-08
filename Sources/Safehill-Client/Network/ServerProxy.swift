//
//  ServerProxy.swift
//  Enkey
//
//  Created by Gennaro Frazzingaro on 9/5/21.
//

import Foundation

public enum SHServer4xxError : Error {
    case badRequest(String)
    
    public func toCode() -> Int {
        return 400
    }
}

public enum SHServer5xxError : Error {
    case notImplemented
    case outdatedKeys
    
    public func toCode() -> Int {
        if self == .notImplemented {
            return 501
        }
        return 500
    }
}

public struct SHServerProxy {
    
    let cachedApi: SHServerAPI
    let serverApi: SHServerAPI
    
    public init(user: SHLocalUser) {
        self.cachedApi = LocalServer(requestor: user)
        self.serverApi = LocalServer(requestor: user)
    }
    
    public func createUser(completionHandler: @escaping (Swift.Result<SHServerUser?, Error>) -> ()) {
        self.serverApi.createUser(completionHandler: completionHandler)
    }
    
    public func sendAuthenticationCode(completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        self.serverApi.sendAuthenticationCode(completionHandler: completionHandler)
    }
    
    public func validateAuthenticationCode(completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        self.serverApi.validateAuthenticationCode(completionHandler: completionHandler)
    }
    
    public func getUsers(withIdentifiers userIdentifiers: [String], completionHandler: @escaping (Swift.Result<[SHServerUser], Error>) -> ()) {
        self.serverApi.getUsers(withIdentifiers: userIdentifiers, completionHandler: completionHandler)
    }
    
    public func getAssetDescriptors(completionHandler: @escaping (Swift.Result<[SHAssetDescriptor], Error>) -> ()) {
        
        var serverDescriptors = [SHAssetDescriptor]()
        
        self.serverApi.getAssetDescriptors { serverResult in
            switch serverResult {
            case .failure(let err):
                completionHandler(.failure(err))
            case .success(let descriptors):
                serverDescriptors = descriptors
                completionHandler(.success(descriptors))
                
                self.cachedApi.getAssetDescriptors { localResult in
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
        
        self.cachedApi.getLowResAssets(withGlobalIdentifiers: assetIdentifiers) { localResult in
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
         
            self.serverApi.getLowResAssets(withGlobalIdentifiers: assetIdentifiersToFetch) { serverResult in
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
        
        self.cachedApi.getHiResAssets(withGlobalIdentifiers: assetIdentifiers) { localResult in
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
         
            self.serverApi.getHiResAssets(withGlobalIdentifiers: assetIdentifiersToFetch) { serverResult in
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
    
    public func storeAsset(lowResAsset: SHEncryptedAsset,
                           hiResAsset: SHEncryptedAsset,
                           completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        guard lowResAsset.globalIdentifier == hiResAsset.globalIdentifier &&
                lowResAsset.localIdentifier == hiResAsset.localIdentifier else {
            completionHandler(.failure(SHServer4xxError.badRequest("identifiers for both low and hi resolution assets need to match")))
            return
        }
        
        self.serverApi.storeAsset(lowResAsset: lowResAsset, hiResAsset: hiResAsset) { result in
            completionHandler(result)
            if case .success() = result {
                self.cachedApi.storeAsset(lowResAsset: lowResAsset, hiResAsset: hiResAsset) { result in
                    if case .failure(let err) = result {
                        print("Asset was stored to server but not in the local cache: \(err)")
                    }
                }
            }
        }
    }
    
    public func deleteAssets(withGlobalIdentifiers globalIdentifiers: [String], completionHandler: @escaping (Result<[String], Error>) -> ()) {
        self.serverApi.deleteAssets(withGlobalIdentifiers: globalIdentifiers) { result in
            completionHandler(result)
            if case .success(_) = result {
                self.cachedApi.deleteAssets(withGlobalIdentifiers: globalIdentifiers) { result in
                    if case .failure(let err) = result {
                        print("Asset was deleted on server but not from the local cache: \(err)")
                    }
                }
            }
        }
    }
}
