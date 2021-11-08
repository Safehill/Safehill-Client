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
    
    func createUser(completionHandler: @escaping (Result<SHServerUser?, Error>) -> ()) {
        let key = requestor.identifier
        userStore.value(for: key) { getResult in
            switch getResult {
            case .success(let value):
                if let value = value as? [String: Any] {
                    guard value["publicKey"] as? Data == requestor.publicKeyData,
                          value["publicSignature"] as? Data == requestor.publicSignatureData else {
                              completionHandler(.failure(SHHTTPError.ServerError.outdatedKeys))
                              return
                          }
                    
                    completionHandler(.success(requestor))
                    return
                }
                
                userStore.set(value: [
                    "identifier": key,
                    "publicKey": requestor.publicKeyData,
                    "publicSignature": requestor.publicSignatureData,
                    "name": requestor.name!,
                    "phoneNumber": requestor.phoneNumber!,
                ], for: key) { (postResult: Swift.Result) in
                    switch postResult {
                    case .success:
                        completionHandler(.success(requestor))
                    case .failure(let err):
                        completionHandler(.failure(err))
                        return
                    }
                }
            case .failure(let err):
                completionHandler(.failure(err))
                return
            }
        }
    }
    
    func sendAuthenticationCode(completionHandler: @escaping (Result<Void, Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func validateAuthenticationCode(completionHandler: @escaping (Result<Void, Error>) -> ()) {
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
                           let phoneNumber = res["phoneNumber"] as? String,
                           let publicKeyData = res["publicKey"] as? Data,
                           let publicSignatureData = res["publicSignature"] as? Data {
                            if let user = try? SHRemoteUser(identifier: identifier,
                                                          name: name,
                                                           phoneNumber: phoneNumber,
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
    
    private func getAssets(withIdentifiers assetIdentifiers: [String],
                           quality: String,
                           completionHandler: @escaping (Swift.Result<[String: SHEncryptedAsset], Error>) -> ()) {
        let prefixCondition = KBGenericCondition(.beginsWith, value: quality + "::")
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
    
    func getLowResAssets(withGlobalIdentifiers assetIdentifiers: [String], completionHandler: @escaping (Swift.Result<[String: SHEncryptedAsset], Error>) -> ()) {
        getAssets(withIdentifiers: assetIdentifiers,
                  quality: "low",
                  completionHandler: completionHandler)
    }
    
    func getHiResAssets(withGlobalIdentifiers assetIdentifiers: [String], completionHandler: @escaping (Swift.Result<[String: SHEncryptedAsset], Error>) -> ()) {
        getAssets(withIdentifiers: assetIdentifiers,
                  quality: "hi",
                  completionHandler: completionHandler)
    }
    
    func storeAsset(lowResAsset: SHEncryptedAsset, hiResAsset: SHEncryptedAsset, completionHandler: @escaping (Result<Void, Error>) -> ()) {
        
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
                completionHandler(.success(()))
            case .failure(let err):
                completionHandler(.failure(err))
                return
            }
        }
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
