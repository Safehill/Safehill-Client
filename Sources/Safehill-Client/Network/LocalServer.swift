import Foundation
import KnowledgeBase

let userStore = KBKVStore.store(withName: "com.gf.safehill.LocalServer.users")
let assetStore = KBKVStore.store(withName: "com.gf.safehill.LocalServer.assets")

struct LocalServer : SHServerAPI {
    
    let requestor: SHLocalUser
    
    init(requestor: SHLocalUser) {
        self.requestor = requestor
    }
    
    private func createUser(name: String,
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
                    completionHandler: @escaping (Swift.Result<SHServerUser, Error>) -> ()) {
        guard email != nil || name != nil else {
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
    
    func createUser(name: String, completionHandler: @escaping (Result<SHServerUser, Error>) -> ()) {
        self.createUser(name: name, ssoIdentifier: nil, completionHandler: completionHandler)
    }
    
    func deleteAccount(name: String, password: String, completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        self.deleteAccount(completionHandler: completionHandler)
    }
    
    func deleteAccount(completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        var userRemovalError: Error? = nil
        var assetsRemovalError: Error? = nil
        let group = DispatchGroup()
        
        group.enter()
        userStore.removeAll { result in
            if case .failure(let err) = result {
                userRemovalError = err
            }
            group.leave()
        }
        
        group.enter()
        assetStore.removeAll { result in
            if case .failure(let err) = result {
                assetsRemovalError = err
            }
            group.leave()
        }
        
        let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultNetworkTimeoutInMilliseconds * 2))
        guard dispatchResult == .success else {
            return completionHandler(.failure(SHHTTPError.TransportError.timedOut))
        }
        guard userRemovalError == nil else {
            return completionHandler(.failure(userRemovalError!))
        }
        guard assetsRemovalError == nil else {
            return completionHandler(.failure(assetsRemovalError!))
        }
        completionHandler(.success(()))
    }
    
    func signInWithApple(email: String,
                         name: String,
                         authorizationCode: Data,
                         identityToken: Data,
                         completionHandler: @escaping (Result<SHAuthResponse, Error>) -> ()) {
        let ssoIdentifier = identityToken.base64EncodedString()
        self.createUser(name: name, ssoIdentifier: ssoIdentifier) { result in
            switch result {
            case .success(let user):
                let authResponse = SHAuthResponse(user: user as! SHRemoteUser, bearerToken: "")
                completionHandler(.success(authResponse))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    public func signIn(name: String, completionHandler: @escaping (Swift.Result<SHAuthResponse, Error>) -> ()) {
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
                           let publicKeyData = res["publicKey"] as? Data,
                           let publicSignatureData = res["publicSignature"] as? Data {
                            let user = SHRemoteUser(identifier: identifier,
                                                    name: name,
                                                    publicKeyData: publicKeyData,
                                                    publicSignatureData: publicSignatureData)
                            userList.append(user)
                        }
                    }
                }
                completionHandler(.success(userList))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    func searchUsers(query: String, completionHandler: @escaping (Result<[SHServerUser], Error>) -> ()) {
        // TODO: Store and retrieve users in the knowledge graph
        completionHandler(.success([]))
    }
    
    func getAssetDescriptors(completionHandler: @escaping (Swift.Result<[SHAssetDescriptor], Error>) -> ()) {
        /// No need to pull all versions when constructing descriptor, pulling "low" version only.
        /// This assumes that sharing information and other metadata are common across all versions (low and hi)
        let condition = KBGenericCondition(.beginsWith, value: "low::").or(KBGenericCondition(.beginsWith, value: "hi::"))
        assetStore.dictionaryRepresentation(forKeysMatching: condition) { (result: Swift.Result) in
            switch result {
            case .success(let keyValues):
                var versionUploadState = [SHAssetQuality: SHAssetDescriptorUploadState]()
                var descriptors = [SHGenericAssetDescriptor]()
                for (k, v) in keyValues {
                    let globalIdentifier: String
                    
                    guard let value = v as? [String: Any],
                          let phAssetIdentifier = value["applePhotosAssetIdentifier"] as? String?,
                          let creationDate = value["creationDate"] as? Date? else {
                        break
                    }
                    
                    if let range = k.range(of: "low::") {
                        globalIdentifier = "" + k[range.upperBound...]
                        
                        if let uploadStateStr = value["uploadState"] as? String,
                           let uploadState = SHAssetDescriptorUploadState(rawValue: uploadStateStr) {
                            versionUploadState[.lowResolution] = uploadState
                        } else {
                            versionUploadState[.hiResolution] = .notStarted
                        }
                        
                    } else if let _ = k.range(of: "hi::") {
                        if let uploadStateStr = value["uploadState"] as? String,
                           let uploadState = SHAssetDescriptorUploadState(rawValue: uploadStateStr) {
                            versionUploadState[.hiResolution] = uploadState
                        } else {
                            versionUploadState[.hiResolution] = .notStarted
                        }
                        
                        break
                        // Terminate here, as from this point there's no version-specific information
                        // All the asset descriptor details will be pulled from the "low::" version
                    } else {
                        break
                    }
                    
                    var sharedBy: String? = nil
                    var err: Error? = nil
                    
                    do {
                        let condition = KBGenericCondition(.beginsWith, value: "sender::").and(KBGenericCondition(.endsWith, value: globalIdentifier))
                        let keys = try assetStore.keys(matching: condition)
                        if keys.count > 0, let key = keys.first {
                            let components = key.components(separatedBy: "::")
                            if components.count == 4 {
                                sharedBy = components[1]
                            } else {
                                log.error("failed to retrieve sender information for asset \(globalIdentifier)")
                                err = KBError.fatalError("Invalid sender information for asset")
                            }
                        } else {
                            log.error("failed to retrieve sender information for asset \(globalIdentifier)")
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
                    
                    var groupInfoById = [String: SHAssetGroupInfo]()
                    var sharedWithUsersInGroup = [String: String]()
                    
                    do {
                        let condition = KBGenericCondition(
                            .beginsWith, value: "receiver::"
                        ).and(KBGenericCondition(
                            .endsWith, value: globalIdentifier)
                        ).and(KBGenericCondition(
                            .contains, value: "::low::")
                        )

                        let keysAndValues = try assetStore.dictionaryRepresentation(forKeysMatching: condition)
                        if keysAndValues.count > 0 {
                            for (key, value) in keysAndValues {
                                guard let value = value as? [String: String] else {
                                    print("failed to retrieve sharing information for asset \(globalIdentifier). Type is not a dictionary")
                                    continue
                                }
                                let components = key.components(separatedBy: "::")
                                if components.count == 4, let groupId = value["groupId"] {
                                    sharedWithUsersInGroup[components[1]] = groupId
                                } else {
                                    print("failed to retrieve sharing information for asset \(globalIdentifier). Invalid formal")
                                }
                                
                                if let groupId = value["groupId"], let groupCreationDate = value["groupCreationDate"] {
                                    groupInfoById[groupId] = SHGenericAssetGroupInfo(name: nil, createdAt: groupCreationDate.iso8601withFractionalSeconds)
                                }
                            }
                        } else {
                            print("failed to retrieve sharing information for asset \(globalIdentifier). No data")
                        }
                    } catch {
                        print("failed to retrieve sharing information for asset \(globalIdentifier): \(error)")
                    }
                    
                    let sharingInfo = SHGenericDescriptorSharingInfo(
                        sharedByUserIdentifier: sharedBy,
                        sharedWithUserIdentifiersInGroup: sharedWithUsersInGroup,
                        groupInfoById: groupInfoById
                    )
                    
                    let combinedUploadState = versionUploadState.reduce(SHAssetDescriptorUploadState.notStarted, {
                        (partialResult: SHAssetDescriptorUploadState, item) in
                        let (_, value) = item
                        if partialResult == .completed {
                            if value == .completed { return .completed }
                            else { return .partial }
                        } else {
                            return value
                        }
                    })
                    
                    let descriptor = SHGenericAssetDescriptor(
                        globalIdentifier: globalIdentifier,
                        localIdentifier: phAssetIdentifier,
                        creationDate: creationDate,
                        uploadState: combinedUploadState,
                        sharingInfo: sharingInfo
                    )
                    descriptors.append(descriptor)
                }
                completionHandler(.success(descriptors))
                
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    func getAssets(withGlobalIdentifiers assetIdentifiers: [String],
                   versions: [SHAssetQuality]? = nil,
                   completionHandler: @escaping (Swift.Result<[String: SHEncryptedAsset], Error>) -> ()) {
        
        var prefixCondition = KBGenericCondition(value: false)
        
        let versions = versions ?? SHAssetQuality.all
        for quality in versions {
            prefixCondition = prefixCondition.or(KBGenericCondition(.beginsWith, value: quality.rawValue + "::"))
        }
        
        var assetCondition = KBGenericCondition(value: false)
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
                
                do {
                    completionHandler(.success(try SHGenericEncryptedAsset.fromDicts(values)))
                } catch {
                    completionHandler(.failure(error))
                }
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    func create(assets: [SHEncryptedAsset],
                completionHandler: @escaping (Result<[SHServerAsset], Error>) -> ()) {
        self.create(assets: assets, senderUserIdentifier: requestor.identifier, completionHandler: completionHandler)
    }
    
    func create(assets: [SHEncryptedAsset],
                senderUserIdentifier: String,
                completionHandler: @escaping (Result<[SHServerAsset], Error>) -> ()) {
        
        let writeBatch = assetStore.writeBatch()
        
        for asset in assets {
            for encryptedVersion in asset.encryptedVersions {
                let version: [String: Any?] = [
                    "quality": encryptedVersion.quality.rawValue,
                    "assetIdentifier": asset.globalIdentifier,
                    "applePhotosAssetIdentifier": asset.localIdentifier,
                    "encryptedData": encryptedVersion.encryptedData,
                    "senderEncryptedSecret": encryptedVersion.encryptedSecret,
                    "publicKey": encryptedVersion.publicKeyData,
                    "publicSignature": encryptedVersion.publicSignatureData,
                    "creationDate": asset.creationDate,
                    "uploadState": SHAssetDescriptorUploadState.notStarted.rawValue
                ]
                
                writeBatch.set(value: version, for: "\(encryptedVersion.quality.rawValue)::" + asset.globalIdentifier)
                writeBatch.set(value: true,
                               for: [
                                "sender",
                                senderUserIdentifier,
                                encryptedVersion.quality.rawValue,
                                asset.globalIdentifier
                               ].joined(separator: "::")
                )
                let sharedVersionDetails: [String: String] = [
                    "senderEncryptedSecret": encryptedVersion.encryptedSecret.base64EncodedString(),
                    "ephemeralPublicKey": encryptedVersion.publicKeyData.base64EncodedString(),
                    "publicSignature": encryptedVersion.publicSignatureData.base64EncodedString(),
                    "groupId": asset.groupId,
                    "groupCreationDate": Date().iso8601withFractionalSeconds
                ]
                writeBatch.set(value: sharedVersionDetails,
                               for: [
                                "receiver",
                                requestor.identifier,
                                encryptedVersion.quality.rawValue,
                                asset.globalIdentifier
                               ].joined(separator: "::")
                )
            }
        }
        
        writeBatch.write { (result: Swift.Result) in
            switch result {
            case .success():
                var serverAssets = [SHServerAsset]()
                for asset in assets {
                    var serverAssetVersions = [SHServerAssetVersion]()
                    for encryptedVersion in asset.encryptedVersions {
                        serverAssetVersions.append(
                            SHServerAssetVersion(
                                versionName: encryptedVersion.quality.rawValue,
                                publicKeyData: encryptedVersion.publicKeyData,
                                publicSignatureData: encryptedVersion.publicSignatureData,
                                encryptedSecret: encryptedVersion.encryptedSecret,
                                presignedURL: "",
                                presignedURLExpiresInMinutes: 0
                            )
                        )
                    }
                    
                    let serverAsset = SHServerAsset(globalIdentifier: asset.globalIdentifier,
                                                    localIdentifier: asset.localIdentifier,
                                                    creationDate: asset.creationDate,
                                                    groupId: asset.groupId,
                                                    versions: serverAssetVersions)
                    serverAssets.append(serverAsset)
                }
                
                completionHandler(.success(serverAssets))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    func upload(serverAsset: SHServerAsset,
                asset: SHEncryptedAsset,
                completionHandler: @escaping (Result<Void, Error>) -> ()) {
        completionHandler(.success(()))
    }
    
    func markAsUploaded(_ assetVersion: SHEncryptedAssetVersion,
                        assetGlobalIdentifier globalAssetId: String,
                        completionHandler: @escaping (Result<Void, Error>) -> ()) {
        let condition = KBGenericCondition(.beginsWith, value: "\(assetVersion.quality.rawValue)::\(globalAssetId)")
        assetStore.dictionaryRepresentation(forKeysMatching: condition) { (result: Swift.Result) in
            switch result {
            case .success(let keyValues):
                for (k, v) in keyValues {
                    guard var value = v as? [String: Any],
                          let _ = value["uploadState"] as? String?
                    else {
                        completionHandler(.failure(KBError.unexpectedData(v)))
                        return
                    }
                    
                    value["uploadState"] = SHAssetDescriptorUploadState.completed.rawValue
                    let writeBatch = assetStore.writeBatch()
                    writeBatch.set(value: value, for: k)
                    writeBatch.write(completionHandler: completionHandler)
                }
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    
    func share(asset: SHShareableEncryptedAsset,
               completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        let writeBatch = assetStore.writeBatch()
        
        for sharedVersion in asset.sharedVersions {
            let sharedVersionDetails: [String: String] = [
                "senderEncryptedSecret": sharedVersion.encryptedSecret.base64EncodedString(),
                "ephemeralPublicKey": sharedVersion.ephemeralPublicKey.base64EncodedString(),
                "publicSignature": sharedVersion.publicSignature.base64EncodedString(),
                "groupId": asset.groupId,
                "groupCreationDate": Date().iso8601withFractionalSeconds
            ]
            writeBatch.set(value: sharedVersionDetails,
                           for: [
                            "receiver",
                            sharedVersion.userPublicIdentifier,
                            sharedVersion.quality.rawValue,
                            asset.globalIdentifier
                           ].joined(separator: "::")
            )
        }
        
        writeBatch.write(completionHandler: completionHandler)
    }
    
    public func getSharingInfo(forAssetIdentifier globalIdentifier: String,
                               for users: [SHServerUser],
                               completionHandler: @escaping (Swift.Result<SHShareableEncryptedAsset?, Error>) -> ()) {
        var condition = KBGenericCondition(value: true)
        for user in users {
            let start = KBGenericCondition(.beginsWith, value: [
                "receiver",
                user.identifier
            ].joined(separator: "::"))
            let end = KBGenericCondition(.endsWith, value: globalIdentifier)
            condition = condition.or(start.and(end))
        }
        
        assetStore.dictionaryRepresentation(forKeysMatching: condition) { result in
            switch result {
            case .success(let keyValues):
                var shareableVersions = [SHShareableEncryptedAssetVersion]()
                
                guard keyValues.count > 0 else {
                    completionHandler(.success(nil))
                    return
                }
                
                var groupId: String? = nil
                
                for (k, v) in keyValues {
                    guard let value = v as? [String: Any?] else {
                        completionHandler(.failure(KBError.unexpectedData(v)))
                        return
                    }
                    guard let range = k.range(of: "receiver::") else {
                        completionHandler(.failure(KBError.unexpectedData(k)))
                        return
                    }
                    
                    let userAssetIds = ("" + k[range.upperBound...]).components(separatedBy: "::")
                    guard userAssetIds.count == 3 else {
                        completionHandler(.failure(KBError.unexpectedData(k)))
                        return
                    }
                    
                    let (userPublicId, qualityRaw) = (userAssetIds[0], userAssetIds[1])
                    
                    guard let quality = SHAssetQuality(rawValue: qualityRaw) else {
                        completionHandler(.failure(KBError.unexpectedData(qualityRaw)))
                        return
                    }
                    guard let versionEncryptedSecretBase64 = value["senderEncryptedSecret"] as? String,
                          let encryptedSecret = Data(base64Encoded: versionEncryptedSecretBase64) else {
                        completionHandler(.failure(KBError.unexpectedData(value)))
                        return
                    }
                    guard let ephemeralPublicKeyBase64 = value["ephemeralPublicKey"] as? String,
                          let ephemeralPublicKey = Data(base64Encoded: ephemeralPublicKeyBase64) else {
                        completionHandler(.failure(KBError.unexpectedData(value)))
                        return
                    }
                    guard let publicSignatureBase64 = value["publicSignature"] as? String,
                          let publicSignature = Data(base64Encoded: publicSignatureBase64) else {
                        completionHandler(.failure(KBError.unexpectedData(value)))
                        return
                    }
                    guard let gid = value["groupId"] as? String else {
                        completionHandler(.failure(KBError.unexpectedData(value)))
                        return
                    }
                    /// Although groupId is stored as a property of version, it is safe to coalesce,
                    /// as all versions should have the same groupId
                    groupId = gid
                    
                    let shareableVersion = SHGenericShareableEncryptedAssetVersion(
                        quality: quality,
                        userPublicIdentifier: userPublicId,
                        encryptedSecret: encryptedSecret,
                        ephemeralPublicKey: ephemeralPublicKey,
                        publicSignature: publicSignature
                    )
                    shareableVersions.append(shareableVersion)
                }
                
                guard let groupId = groupId else {
                    completionHandler(.failure(KBError.unexpectedData(groupId)))
                    return
                }
                
                completionHandler(.success(SHGenericShareableEncryptedAsset(
                    globalIdentifier: globalIdentifier,
                    sharedVersions: shareableVersions,
                    groupId: groupId
                )))
            case .failure(let err):
                completionHandler(.failure(err))
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
