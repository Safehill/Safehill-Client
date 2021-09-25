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
    
    let api: SHServerAPI
    
    public init(user: SHLocalUser) {
        self.api = LocalServer(requestor: user)
    }
    
    public func createUser(completionHandler: @escaping (Swift.Result<SHServerUser?, Error>) -> ()) {
        DispatchQueue.global(qos: .userInitiated).asyncAfter(deadline: .now() + .seconds(1)) {
            self.api.createUser(completionHandler: completionHandler)
        }
    }
    
    public func sendAuthenticationCode(completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        DispatchQueue.global(qos: .userInitiated).asyncAfter(deadline: .now() + .seconds(1)) {
            self.api.sendAuthenticationCode(completionHandler: completionHandler)
        }
    }
    
    public func validateAuthenticationCode(completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        DispatchQueue.global(qos: .userInitiated).asyncAfter(deadline: .now() + .seconds(1)) {
            self.api.validateAuthenticationCode(completionHandler: completionHandler)
        }
    }
    
    public func getUsers(withIdentifiers userIdentifiers: [String], completionHandler: @escaping (Swift.Result<[SHServerUser], Error>) -> ()) {
        DispatchQueue.global(qos: .userInitiated).asyncAfter(deadline: .now() + .seconds(1)) {
            self.api.getUsers(withIdentifiers: userIdentifiers, completionHandler: completionHandler)
        }
    }
    
    public func getAssetDescriptors(completionHandler: @escaping (Swift.Result<[SHAssetDescriptor], Error>) -> ()) {
        DispatchQueue.global(qos: .userInitiated).asyncAfter(deadline: .now() + .seconds(1)) {
            self.api.getAssetDescriptors(completionHandler: completionHandler)
        }
    }
    
    public func getLowResAssets(withGlobalIdentifiers assetIdentifiers: [String], completionHandler: @escaping (Swift.Result<[SHEncryptedAsset], Error>) -> ()) {
        DispatchQueue.global(qos: .userInitiated).asyncAfter(deadline: .now() + .seconds(3)) {
            self.api.getLowResAssets(withGlobalIdentifiers: assetIdentifiers,
                                     completionHandler: completionHandler)
        }
    }
    
    public func getHiResAssets(withGlobalIdentifiers assetIdentifiers: [String], completionHandler: @escaping (Swift.Result<[SHEncryptedAsset], Error>) -> ()) {
        DispatchQueue.global(qos: .userInitiated).asyncAfter(deadline: .now() + .seconds(3)) {
            self.api.getHiResAssets(withGlobalIdentifiers: assetIdentifiers,
                                    completionHandler: completionHandler)
        }
    }
    
//    public func getLowResAssets(excludingAssetIdentifiers assetIdsToExclude: [String],
//                         excludingLocalAssetIdentifiers localAssetIdsToExclude: [String],
//                         completionHandler: @escaping (Swift.Result<[Asset], Error>) -> ()) {
//        self.api.getLowResAssets(excludingAssetIdentifiers: assetIdsToExclude,
//                                 excludingLocalAssetIdentifiers: localAssetIdsToExclude,
//                                 completionHandler: completionHandler)
//    }
//
//    public func getHiResAssets(excludingAssetIdentifiers assetIdsToExclude: [String],
//                        excludingLocalAssetIdentifiers localAssetIdsToExclude: [String],
//                        completionHandler: @escaping (Swift.Result<[Asset], Error>) -> ()) {
//        self.api.getHiResAssets(excludingAssetIdentifiers: assetIdsToExclude,
//                                excludingLocalAssetIdentifiers: localAssetIdsToExclude,
//                                completionHandler: completionHandler)
//    }
    
    public func storeAsset(lowResAsset: SHEncryptedAsset,
                    hiResAsset: SHEncryptedAsset,
                    completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        guard lowResAsset.globalIdentifier == hiResAsset.globalIdentifier &&
                lowResAsset.localIdentifier == hiResAsset.localIdentifier else {
            completionHandler(.failure(SHServer4xxError.badRequest("identifiers for both low and hi resolution assets need to match")))
            return
        }
        DispatchQueue.global(qos: .userInitiated).asyncAfter(deadline: .now() + .seconds(5)) {
            self.api.storeAsset(lowResAsset: lowResAsset, hiResAsset: hiResAsset, completionHandler: completionHandler)
        }
    }
}
