//
//  LocalServer.swift
//  Safehill-Client
//
//  Created by Gennaro Frazzingaro on 9/9/21.
//

import Foundation
import KnowledgeBase

let userStore = KBKVStore.store(withName: "com.gf.Enkey.LocalServer.users")
let assetStore = KBKVStore.store(withName: "com.gf.Enkey.LocalServer.assets")

struct LocalServer : SHServerAPI {
    
    let requestor: SHLocalUser
    
    init(requestor: SHLocalUser) {
        self.requestor = requestor
    }
    
    private func createUser(email: String,
                            name: String,
                            password: String? = nil,
                            ssoIdentifier: String?,
                            completionHandler: @escaping (Result<SHServerUser, Error>) -> ()) {
        let key = requestor.identifier
        userStore.value(for: key) { getResult in
            switch getResult {
            case .success(let value):
                // User already exists. Return it
                if let value = value as? [String: Any] {
                    guard value["publicKey"] as? Data == requestor.publicKeyData,
                          value["publicSignature"] as? Data == requestor.publicSignatureData else {
                              completionHandler(.failure(SHHTTPError.ClientError.methodNotAllowed))
                              return
                          }
                    
                    completionHandler(.success(requestor))
                    return
                }
                
                // User doesn't exist. Create it
                var value = [
                    "identifier": key,
                    "publicKey": requestor.publicKeyData,
                    "publicSignature": requestor.publicSignatureData,
                    "name": name,
                    "email": email,
                ] as [String : Any]
                if let ssoIdentifier = ssoIdentifier {
                    value["ssoIdentifier"] = ssoIdentifier
                }
                userStore.set(value: value, for: key) { (postResult: Swift.Result) in
                    switch postResult {
                    case .success:
                        completionHandler(.success(requestor))
                    case .failure(let err):
                        completionHandler(.failure(err))
                    }
                }
            case .failure(let err):
                completionHandler(.failure(err))
                return
            }
        }
    }
    
    func updateUser(email: String?,
                    name: String?,
                    password: String?,
                    completionHandler: @escaping (Swift.Result<SHServerUser, Error>) -> ()) {
        guard email != nil || name != nil || password != nil else {
            completionHandler(.failure(SHHTTPError.ClientError.badRequest("Invalid parameters")))
            return
        }
        
        let key = requestor.identifier
        userStore.value(for: key) { getResult in
            switch getResult {
            case .success(let user):
                // User already exists. Return it
                if let user = user as? [String: Any] {
                    guard user["publicKey"] as? Data == requestor.publicKeyData,
                          user["publicSignature"] as? Data == requestor.publicSignatureData else {
                              completionHandler(.failure(SHHTTPError.ClientError.methodNotAllowed))
                              return
                          }
                    
                    let value = [
                        "identifier": key,
                        "publicKey": requestor.publicKeyData,
                        "publicSignature": requestor.publicSignatureData,
                        "name": name ?? user["name"]!,
                        "email": email ?? user["email"]!,
                    ] as [String : Any]
                    userStore.set(value: value, for: key) { (postResult: Swift.Result) in
                        switch postResult {
                        case .success:
                            completionHandler(.success(requestor))
                        case .failure(let err):
                            completionHandler(.failure(err))
                        }
                    }
                    
                    completionHandler(.success(requestor))
                    return
                }
            case .failure(let err):
                completionHandler(.failure(err))
                return
            }
        }

    }
    
    func createUser(email: String, name: String, password: String, completionHandler: @escaping (Result<SHServerUser, Error>) -> ()) {
        self.createUser(email: email, name: name, password: password, ssoIdentifier: nil, completionHandler: completionHandler)
    }
    
    func deleteAccount(email: String = "", password: String = "", completionHandler: @escaping (Result<Void, Error>) -> ()) {
        let dispatch = KBTimedDispatch()
        
        dispatch.group.enter()
        userStore.removeAll { result in
            if case .failure(let err) = result {
                dispatch.interrupt(err)
            } else {
                dispatch.group.leave()
            }
        }
        
        dispatch.group.enter()
        assetStore.removeAll { result in
            if case .failure(let err) = result {
                dispatch.interrupt(err)
            } else {
                dispatch.group.leave()
            }
        }
        
        do {
            try dispatch.wait()
            completionHandler(.success(()))
        } catch {
            completionHandler(.failure(error))
        }
    }
    
    func signInWithApple(email: String,
                         name: String,
                         authorizationCode: Data,
                         identityToken: Data,
                         completionHandler: @escaping (Result<SHAuthResponse, Error>) -> ()) {
        let ssoIdentifier = identityToken.base64EncodedString()
        self.createUser(email: email, name: name, password: "", ssoIdentifier: ssoIdentifier) { result in
            switch result {
            case .success(let user):
                let authResponse = SHAuthResponse(user: user as! SHRemoteUser, bearerToken: "")
                completionHandler(.success(authResponse))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    public func signIn(email: String?, password: String, completionHandler: @escaping (Swift.Result<SHAuthResponse, Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func getUsers(withIdentifiers userIdentifiers: [String], completionHandler: @escaping (Result<[SHServerUser], Error>) -> ()) {
        userStore.values(for: userIdentifiers) { result in
            switch result {
            case .success(let resList):
                var userList = [SHServerUser]()
                if let resList = resList as? [[String: Any]] {
                    for res in resList {
                        if let identifier = res["identifier"] as? String,
                           let name = res["name"] as? String,
                           let email = res["email"] as? String,
                           let publicKeyData = res["publicKey"] as? Data,
                           let publicSignatureData = res["publicSignature"] as? Data {
                            if let user = try? SHRemoteUser(identifier: identifier,
                                                          name: name,
                                                           email: email,
                                                           publicKeyData: publicKeyData,
                                                            publicSignatureData: publicSignatureData) {
                                userList.append(user)
                            }
                        }
                    }
                }
                completionHandler(.success(userList))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    func getAssetDescriptors(completionHandler: @escaping (Swift.Result<[SHAssetDescriptor], Error>) -> ()) {
        let condition = KBGenericCondition(.beginsWith, value: "low::")
        assetStore.dictionaryRepresentation(forKeysMatching: condition) { (result: Swift.Result) in
            switch result {
            case .success(let keyValues):
                var descriptors = [SHGenericAssetDescriptor]()
                for (k, v) in keyValues {
                    let globalIdentifier: String
                    if let range = k.range(of: "low::") {
                        globalIdentifier = "" + k[range.upperBound...]
                    } else {
                        break
                    }
                    guard let value = v as? [String: Any],
                          let phAssetIdentifier = value["applePhotosAssetIdentifier"] as? String?,
                          let creationDate = value["creationDate"] as? Date? else {
                        break
                    }
                    
                    var sharedBy: String? = nil
                    var err: Error? = nil
                    
                    do {
                        let condition = KBGenericCondition(.beginsWith, value: "sender::").and(KBGenericCondition(.endsWith, value: globalIdentifier))
                        let keys = try assetStore.keys(matching: condition)
                        if keys.count > 0, let key = keys.first {
                            let components = key.components(separatedBy: "::")
                            if components.count == 3 {
                                sharedBy = components[1]
                            } else {
                                print("failed to retrieve sharing information for asset \(globalIdentifier)")
                                err = KBError.fatalError("Invalid sender information for asset")
                            }
                        } else {
                            print("failed to retrieve sender information for asset \(globalIdentifier)")
                            err = KBError.fatalError("No sender information for asset")
                        }
                    } catch {
                        print("failed to retrieve sender information for asset \(globalIdentifier): \(error)")
                        err = error
                    }
                    
                    guard err == nil, let sharedBy = sharedBy else {
                        completionHandler(.failure(err!))
                        return
                    }
                    
                    var sharedWith = [String]()
                    
                    do {
                        let condition = KBGenericCondition(.beginsWith, value: "receiver::").and(KBGenericCondition(.endsWith, value: globalIdentifier))
                        let keys = try assetStore.keys(matching: condition)
                        if keys.count > 0, let key = keys.first {
                            let components = key.components(separatedBy: "::")
                            if components.count == 3 {
                                sharedWith.append(components[1])
                            } else {
                                print("failed to retrieve sharing information for asset \(globalIdentifier)")
                            }
                        } else {
                            print("failed to retrieve sharing information for asset \(globalIdentifier)")
                        }
                    } catch {
                        print("failed to retrieve sharing information for asset \(globalIdentifier): \(error)")
                    }
                    
                    let descriptor = SHGenericAssetDescriptor(globalIdentifier: globalIdentifier,
                                                              localIdentifier: phAssetIdentifier,
                                                              creationDate: creationDate,
                                                              sharedByUserIdentifier: sharedBy,
                                                              sharedWithUserIdentifiers: sharedWith)
                    descriptors.append(descriptor)
                }
                completionHandler(.success(descriptors))
                
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    func getAssets(withGlobalIdentifiers assetIdentifiers: [String], quality: SHAssetQuality, completionHandler: @escaping (Swift.Result<[String: SHEncryptedAsset], Error>) -> ()) {
        let prefixCondition = KBGenericCondition(.beginsWith, value: quality.rawValue + "::")
        var assetCondition = KBGenericCondition(value: true)
        for assetIdentifier in assetIdentifiers {
            assetCondition = assetCondition.or(KBGenericCondition(.endsWith, value: assetIdentifier))
        }
        assetStore.values(forKeysMatching: prefixCondition.and(assetCondition)) {
            (result: Swift.Result) in
            switch result {
            case .success(let values):
                guard let values = values as? [[String: Any]] else {
                    completionHandler(.failure(KBError.unexpectedData(values)))
                    return
                }
                
                var assets = [String: SHEncryptedAsset]()
                for value in values {
                    if let asset = try? SHGenericEncryptedAsset.fromDict(value) {
                        assets[asset.globalIdentifier] = asset
                    } else {
                        print("Unexpected value \(value)")
                    }
                }
                completionHandler(.success(assets))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    func createAsset(lowResAsset: SHEncryptedAsset,
                     hiResAsset: SHEncryptedAsset,
                     completionHandler: @escaping (Result<SHServerAsset, Error>) -> ()) {
        
        let lowResDict: [String: Any?] = [
            "assetIdentifier": lowResAsset.globalIdentifier,
            "applePhotosAssetIdentifier": lowResAsset.localIdentifier,
            "encryptedData": lowResAsset.encryptedData,
            "encryptedSecret": lowResAsset.encryptedSecret,
            "publicKey": lowResAsset.publicKeyData,
            "publicSignature": lowResAsset.publicSignatureData,
            "creationDate": lowResAsset.creationDate
        ]
        let hiResDict: [String: Any?] = [
            "assetIdentifier": hiResAsset.globalIdentifier,
            "applePhotosAssetIdentifier": hiResAsset.localIdentifier,
            "encryptedData": hiResAsset.encryptedData,
            "encryptedSecret": hiResAsset.encryptedSecret,
            "publicKey": hiResAsset.publicKeyData,
            "publicSignature": hiResAsset.publicSignatureData,
            "creationDate": hiResAsset.creationDate
        ]
        
        let writeBatch = assetStore.writeBatch()
        writeBatch.set(value: lowResDict, for: "low::" + lowResAsset.globalIdentifier)
        writeBatch.set(value: hiResDict, for: "hi::" + hiResAsset.globalIdentifier)
        writeBatch.set(value: true, for: ["sender",
                                          requestor.identifier,
                                          hiResAsset.globalIdentifier]
                        .joined(separator: "::"))
        writeBatch.set(value: true, for: ["receiver",
                                          requestor.identifier,
                                          hiResAsset.globalIdentifier]
                        .joined(separator: "::"))
        
        writeBatch.write { (result: Swift.Result) in
            switch result {
            case .success():
                let lowRes = SHServerAssetVersion(versionName: SHAssetQuality.lowResolution.rawValue,
                                                  publicKeyData: lowResAsset.publicKeyData,
                                                  publicSignatureData: lowResAsset.publicSignatureData,
                                                  encryptedSecret: lowResAsset.encryptedSecret,
                                                  presignedURL: "",
                                                  presignedURLExpiresInMinutes: 0)
                let hiRes = SHServerAssetVersion(versionName: SHAssetQuality.hiResolution.rawValue,
                                                 publicKeyData: hiResAsset.publicKeyData,
                                                 publicSignatureData: hiResAsset.publicSignatureData,
                                                 encryptedSecret: hiResAsset.encryptedSecret,
                                                 presignedURL: "",
                                                 presignedURLExpiresInMinutes: 0)
                let serverAsset = SHServerAsset(globalIdentifier: lowResAsset.globalIdentifier,
                                                localIdentifier: lowResAsset.localIdentifier,
                                                creationDate: lowResAsset.creationDate,
                                                versions: [lowRes, hiRes])
                
                completionHandler(.success(serverAsset))
            case .failure(let err):
                completionHandler(.failure(err))
                return
            }
        }
    }
    
    func uploadLowResAsset(serverAssetVersion: SHServerAssetVersion,
                           encryptedAsset: SHEncryptedAsset,
                           completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        completionHandler(.success(()))
    }
    
    func uploadHiResAsset(serverAssetVersion: SHServerAssetVersion,
                          encryptedAsset: SHEncryptedAsset,
                          completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        completionHandler(.success(()))
    }
    
    func deleteAssets(withGlobalIdentifiers globalIdentifiers: [String], completionHandler: @escaping (Result<[String], Error>) -> ()) {
        var condition = KBGenericCondition(value: true)
        for globalIdentifier in globalIdentifiers {
            condition = condition.or(
                KBGenericCondition(.equal, value: "low::" + globalIdentifier)
            ).or(
                KBGenericCondition(.equal, value: "hi::" + globalIdentifier)
            ).or(
                KBGenericCondition(.beginsWith, value: "sender::").and(KBGenericCondition(.endsWith, value: globalIdentifier))
            ).or(
                KBGenericCondition(.beginsWith, value: "receiver::").and(KBGenericCondition(.endsWith, value: globalIdentifier))
            )
        }
        
        assetStore.removeValues(forKeysMatching: condition) { result in
            switch result {
            case .failure(let err):
                completionHandler(.failure(err))
            case .success(let keysRemoved):
                var removedGids = Set<String>()
                for key in keysRemoved {
                    if key.contains("low::") {
                        removedGids.insert(String(key.suffix(key.count - 5)))
                    } else if key.contains("hi::") {
                        removedGids.insert(String(key.suffix(key.count - 4)))
                    }
                }
                completionHandler(.success(Array(removedGids)))
            }
        }
    }
    
}
