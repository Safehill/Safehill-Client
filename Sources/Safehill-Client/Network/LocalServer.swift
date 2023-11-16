import Foundation
import KnowledgeBase

public let SHDefaultDBTimeoutInMilliseconds = 15000 // 15 seconds

struct LocalServer : SHServerAPI {
    
    let requestor: SHLocalUser
    
    init(requestor: SHLocalUser) {
        self.requestor = requestor
    }
    
    private func createUser(name: String,
                            ssoIdentifier: String?,
                            completionHandler: @escaping (Result<SHServerUser, Error>) -> ()) {
        let userStore: KBKVStore
        do {
            userStore = try SHDBManager.sharedInstance.userStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        let key = requestor.identifier
        userStore.value(for: key) { getResult in
            switch getResult {
            case .success(let value):
                // User already exists. Return it
                if let value = value as? [String: Any] {
                    guard value["publicKey"] as? Data == self.requestor.publicKeyData,
                          value["publicSignature"] as? Data == self.requestor.publicSignatureData else {
                              completionHandler(.failure(SHHTTPError.ClientError.methodNotAllowed))
                              return
                          }
                    
                    completionHandler(.success(self.requestor))
                    return
                }
                
                // User doesn't exist. Create it
                var value = [
                    "identifier": key,
                    "publicKey": self.requestor.publicKeyData,
                    "publicSignature": self.requestor.publicSignatureData,
                    "name": name,
                ] as [String : Any]
                if let ssoIdentifier = ssoIdentifier {
                    value["ssoIdentifier"] = ssoIdentifier
                }
                userStore.set(value: value, for: key) { (postResult: Swift.Result) in
                    switch postResult {
                    case .success:
                        completionHandler(.success(self.requestor))
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
    
    internal func createUser(identifier: UserIdentifier,
                             name: String,
                             publicKeyData: Data,
                             publicSignatureData: Data,
                             completionHandler: @escaping (Result<SHServerUser, Error>) -> ()) {
        let userStore: KBKVStore
        do {
            userStore = try SHDBManager.sharedInstance.userStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        let value = [
            "identifier": identifier,
            "publicKey": publicKeyData,
            "publicSignature": publicSignatureData,
            "name": name,
        ] as [String : Any]
        userStore.set(value: value, for: identifier) { (postResult: Swift.Result) in
            switch postResult {
            case .success:
                completionHandler(.success(self.requestor))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    func sendCodeToUser(countryCode: Int,
                        phoneNumber: Int,
                        code: String,
                        medium: SendCodeToUserRequestDTO.Medium,
                        completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func updateUser(name: String?,
                    phoneNumber: String? = nil,
                    email: String? = nil,
                    completionHandler: @escaping (Swift.Result<SHServerUser, Error>) -> ()) {
        guard email != nil || name != nil || phoneNumber != nil else {
            completionHandler(.failure(SHHTTPError.ClientError.badRequest("Invalid parameters")))
            return
        }
        
        let userStore: KBKVStore
        do {
            userStore = try SHDBManager.sharedInstance.userStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        let key = requestor.identifier
        userStore.value(for: key) { getResult in
            switch getResult {
            case .success(let user):
                // User already exists. Return it
                guard let user = user as? [String: Any] else {
                    completionHandler(.failure(SHHTTPError.ServerError.unexpectedResponse(String(describing: user))))
                    return
                }
                
                guard user["publicKey"] as? Data == self.requestor.publicKeyData,
                      user["publicSignature"] as? Data == self.requestor.publicSignatureData
                else {
                    completionHandler(.failure(SHHTTPError.ClientError.methodNotAllowed))
                    return
                }
                
                var value = [
                    "identifier": key,
                    "publicKey": self.requestor.publicKeyData,
                    "publicSignature": self.requestor.publicSignatureData
                ] as [String : Any]
                if let name = name {
                    value["name"] = name
                }
                if let phoneNumber = phoneNumber {
                    value["phoneNumber"] = phoneNumber
                }
                if let email = email {
                    value["email"] = email
                }
                
                userStore.set(value: value, for: key) { (postResult: Swift.Result) in
                    switch postResult {
                    case .success:
                        completionHandler(.success(self.requestor))
                    case .failure(let err):
                        completionHandler(.failure(err))
                    }
                }
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }

    }
    
    func createUser(name: String, completionHandler: @escaping (Result<SHServerUser, Error>) -> ()) {
        self.createUser(name: name, ssoIdentifier: nil, completionHandler: completionHandler)
    }
    
    func deleteUsers(withIdentifiers identifiers: [UserIdentifier],
                     completionHandler: @escaping (Result<Void, Error>) -> ()) {
        let userStore: KBKVStore
        do {
            userStore = try SHDBManager.sharedInstance.userStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        var condition = KBGenericCondition(value: false)
        for userIdentifier in identifiers {
            condition = condition.or(KBGenericCondition(.equal, value: userIdentifier))
        }
        
        userStore.removeValues(forKeysMatching: condition) { getResult in
            switch getResult {
            case .success(_):
                completionHandler(.success(()))
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    func deleteAccount(name: String, password: String, completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        self.deleteAccount(completionHandler: completionHandler)
    }
    
    func deleteAllAssets(completionHandler: @escaping (Swift.Result<[String], Error>) -> ()) {
        let assetStore: KBKVStore
        do {
            assetStore = try SHDBManager.sharedInstance.assetStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        assetStore.removeAll(completionHandler: completionHandler)
    }
    
    func deleteAccount(completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        let userStore: KBKVStore
        do {
            userStore = try SHDBManager.sharedInstance.userStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
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
        self.deleteAllAssets { result in
            if case .failure(let err) = result {
                assetsRemovalError = err
            }
            group.leave()
        }
        
        let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds * 2))
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
    
    public func signIn(name: String, clientBuild: Int?, completionHandler: @escaping (Swift.Result<SHAuthResponse, Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func getUsers(withIdentifiers userIdentifiers: [UserIdentifier]?, completionHandler: @escaping (Result<[SHServerUser], Error>) -> ()) {
        let userStore: KBKVStore
        do {
            userStore = try SHDBManager.sharedInstance.userStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        let callback: (Result<[Any], Error>) -> () = { result in
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
        
        if let ids = userIdentifiers {
            userStore.values(for: ids) { result in
                if case .success(let resList) = result {
                    callback(.success(resList.filter({ $0 != nil }) as [Any]))
                }
            }
        } else {
            userStore.values(completionHandler: callback)
        }
    }
    
    func searchUsers(query: String, completionHandler: @escaping (Result<[SHServerUser], Error>) -> ()) {
        // TODO: Store and retrieve users in the knowledge graph
        completionHandler(.success([]))
    }
    
    func getAssetDescriptors(forAssetGlobalIdentifiers: [GlobalIdentifier]? = nil,
                             completionHandler: @escaping (Swift.Result<[SHAssetDescriptor], Error>) -> ()) {
        let assetStore: KBKVStore
        do {
            assetStore = try SHDBManager.sharedInstance.assetStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        /// No need to pull all versions when constructing descriptor, pulling "low" version only.
        /// This assumes that sharing information and other metadata are common across all versions (low and hi)
        var condition = KBGenericCondition(value: false)
        
        for quality in SHAssetQuality.all {
            condition = condition.or(KBGenericCondition(.beginsWith, value: "\(quality.rawValue)::"))
        }
        
        if let filterGids = forAssetGlobalIdentifiers {
            var gidCondition = KBGenericCondition(value: false)
            for gid in filterGids {
                gidCondition = gidCondition.or(KBGenericCondition(.endsWith, value: "::\(gid)"))
            }
            condition = condition.and(gidCondition)
        }
        
        assetStore.dictionaryRepresentation(forKeysMatching: condition) { (result: Swift.Result) in
            switch result {
            case .success(let keyValues):
                var versionUploadStateByIdentifierQuality = [String: [SHAssetQuality: SHAssetDescriptorUploadState]]()
                var localInfoByGlobalIdentifier = [String: (phAssetId: String?, creationDate: Date?)]()
                var descriptors = [SHGenericAssetDescriptor]()
                
                for (k, v) in keyValues {
                    guard let value = v as? [String: Any],
                          let phAssetIdentifier = value["applePhotosAssetIdentifier"] as? String?,
                          let creationDate = value["creationDate"] as? Date? else {
                        continue
                    }
                    
                    let doProcessState = { (globalIdentifier: String, quality: SHAssetQuality) in
                        let state: SHAssetDescriptorUploadState
                        
                        if let uploadStateStr = value["uploadState"] as? String,
                           let uploadState = SHAssetDescriptorUploadState(rawValue: uploadStateStr) {
                            state = uploadState
                        } else {
                            state = .notStarted
                        }
                        
                        if versionUploadStateByIdentifierQuality[globalIdentifier] == nil {
                            versionUploadStateByIdentifierQuality[globalIdentifier] = [quality: state]
                        } else {
                            versionUploadStateByIdentifierQuality[globalIdentifier]![quality] = state
                        }
                    }
                    
                    let globalIdentifier: String
                    if let range = k.range(of: "\(SHAssetQuality.lowResolution.rawValue)::") {
                        globalIdentifier = "" + k[range.upperBound...]
                        doProcessState(globalIdentifier, .lowResolution)
                    } else if let range = k.range(of: "\(SHAssetQuality.midResolution.rawValue)::") {
                        globalIdentifier = "" + k[range.upperBound...]
                        doProcessState(globalIdentifier, .midResolution)
                    } else if let range = k.range(of: "\(SHAssetQuality.hiResolution.rawValue)::") {
                        globalIdentifier = "" + k[range.upperBound...]
                        doProcessState(globalIdentifier, .hiResolution)
                    } else {
                        continue
                    }
                    
                    localInfoByGlobalIdentifier[globalIdentifier] = (
                        phAssetId: localInfoByGlobalIdentifier[globalIdentifier]?.phAssetId ?? phAssetIdentifier,
                        creationDate: localInfoByGlobalIdentifier[globalIdentifier]?.creationDate ?? creationDate
                    )
                }
                
                for globalIdentifier in versionUploadStateByIdentifierQuality.keys {
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
                            .contains, value: "::low::") // Can safely assume all versions are shared using the same group id
                        )

                        let keysAndValues = try assetStore.dictionaryRepresentation(forKeysMatching: condition)
                        if keysAndValues.count > 0 {
                            for (key, value) in keysAndValues {
                                guard let value = value as? [String: String] else {
                                    print("failed to retrieve sharing information for asset \(globalIdentifier). Type is not a dictionary")
                                    continue
                                }
                                let components = key.components(separatedBy: "::")
                                /// Components:
                                /// 0) "receiver"
                                /// 1) receiver user public identifier
                                /// 2) version quality
                                /// 3) asset identifier
                                
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
                    
                    
                    // MARK: Calculate combined upload state
                    ///
                    /// Before doing so, adjust upload state as follows:
                    /// - if .mid is completed set .hi as completed
                    /// - if .hi is completed set .mid as completed
                    /// - if one between .mid or .hi are failed but the other one isn't, use the other one's state
                    /// 
                    /// Because .mid is a surrogate for .hi, if that is completed, the client can assume that the asset was completely uploaded.
                    ///
                    if versionUploadStateByIdentifierQuality[globalIdentifier]![.midResolution] == .completed ||
                        versionUploadStateByIdentifierQuality[globalIdentifier]![.hiResolution] == .completed {
                        versionUploadStateByIdentifierQuality[globalIdentifier]![.midResolution] = .completed
                        versionUploadStateByIdentifierQuality[globalIdentifier]![.hiResolution] = .completed
                    }
                    if versionUploadStateByIdentifierQuality[globalIdentifier]![.hiResolution] == .failed,
                       versionUploadStateByIdentifierQuality[globalIdentifier]![.midResolution] != .failed {
                        versionUploadStateByIdentifierQuality[globalIdentifier]![.hiResolution] = versionUploadStateByIdentifierQuality[globalIdentifier]![.midResolution]
                    }
                    if versionUploadStateByIdentifierQuality[globalIdentifier]![.midResolution] == .failed,
                       versionUploadStateByIdentifierQuality[globalIdentifier]![.hiResolution] != .failed {
                        versionUploadStateByIdentifierQuality[globalIdentifier]![.midResolution] = versionUploadStateByIdentifierQuality[globalIdentifier]![.hiResolution]
                    }
                    
                    var combinedUploadState: SHAssetDescriptorUploadState = .notStarted
                    if let uploadStates = versionUploadStateByIdentifierQuality[globalIdentifier] {
                        if uploadStates.allSatisfy({ (_, value) in value == .completed }) {
                            // ALL completed successfully
                            combinedUploadState = .completed
                        } else if uploadStates.allSatisfy({ (_, value) in value == .notStarted }) {
                            // ALL didn't start
                            combinedUploadState = .notStarted
                        } else if uploadStates.contains(where: { (_, value) in value == .failed }) {
                            // SOME failed
                            combinedUploadState = .failed
                        }
                    }
                    
                    let descriptor = SHGenericAssetDescriptor(
                        globalIdentifier: globalIdentifier,
                        localIdentifier: localInfoByGlobalIdentifier[globalIdentifier]?.phAssetId,
                        creationDate: localInfoByGlobalIdentifier[globalIdentifier]?.creationDate,
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
        guard assetIdentifiers.count > 0 else {
            completionHandler(.success([:]))
            return
        }
        
        let assetStore: KBKVStore
        do {
            assetStore = try SHDBManager.sharedInstance.assetStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        var resultDictionary = [String: any SHEncryptedAsset]()
        var err: Error? = nil
        
        let group = DispatchGroup()
        
        for assetIdentifiersChunk in assetIdentifiers.chunked(into: 10) {
            var prefixCondition = KBGenericCondition(value: false)
            
            let versions = versions ?? SHAssetQuality.all
            for quality in versions {
                prefixCondition = prefixCondition
                    .or(KBGenericCondition(.beginsWith, value: quality.rawValue + "::"))
                    .or(KBGenericCondition(.beginsWith, value: "data::" + quality.rawValue + "::"))
            }
            
            var assetCondition = KBGenericCondition(value: false)
            for assetIdentifier in assetIdentifiersChunk {
                assetCondition = assetCondition.or(KBGenericCondition(.endsWith, value: assetIdentifier))
            }
            
            group.enter()
            assetStore.dictionaryRepresentation(forKeysMatching: prefixCondition.and(assetCondition)) {
                (result: Swift.Result) in
                switch result {
                case .success(let keyValues):
                    guard let keyValues = keyValues as? [String: [String: Any]] else {
                        err = KBError.unexpectedData(keyValues)
                        group.leave()
                        return
                    }
                    
                    do {
                        resultDictionary.merge(
                            try SHGenericEncryptedAsset.fromDicts(keyValues),
                            uniquingKeysWith: { (_, b) in return b }
                        )
                        
                    } catch {
                        err = error
                    }
                    group.leave()
                case .failure(let error):
                    err = error
                    group.leave()
                }
            }
        }
        
        let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds * 10))
        guard dispatchResult == .success else {
            return completionHandler(.failure(SHBackgroundOperationError.timedOut))
        }
        
        if let err = err {
            completionHandler(.failure(err))
        } else {
            completionHandler(.success(resultDictionary))
        }
    }
    
    func create(assets: [any SHEncryptedAsset],
                groupId: String,
                filterVersions: [SHAssetQuality]?,
                completionHandler: @escaping (Result<[SHServerAsset], Error>) -> ()) {
        // TODO: Filter versions from `assets`
        
        var descriptorsByGlobalId = [GlobalIdentifier: any SHAssetDescriptor]()
        for encryptedAsset in assets {
            let phantomAssetDescriptor = SHGenericAssetDescriptor(
                globalIdentifier: encryptedAsset.globalIdentifier,
                localIdentifier: encryptedAsset.localIdentifier,
                creationDate: encryptedAsset.creationDate,
                uploadState: .notStarted,
                sharingInfo: SHGenericDescriptorSharingInfo(
                    sharedByUserIdentifier: self.requestor.identifier,
                    sharedWithUserIdentifiersInGroup: [self.requestor.identifier: groupId],
                    groupInfoById: [:]
                )
            )
            guard descriptorsByGlobalId[encryptedAsset.globalIdentifier] == nil else {
                completionHandler(.failure(SHAssetStoreError.invalidRequest("duplicate asset global identifiers to create")))
                return
            }
            descriptorsByGlobalId[encryptedAsset.globalIdentifier] = phantomAssetDescriptor
        }
        
        self.create(assets: assets,
                    descriptorsByGlobalIdentifier: descriptorsByGlobalId,
                    uploadState: .notStarted,
                    completionHandler: completionHandler)
    }
    
    func create(assets: [any SHEncryptedAsset],
                descriptorsByGlobalIdentifier: [GlobalIdentifier: any SHAssetDescriptor],
                uploadState: SHAssetDescriptorUploadState,
                completionHandler: @escaping (Result<[SHServerAsset], Error>) -> ()) {
        let assetStore: KBKVStore
        do {
            assetStore = try SHDBManager.sharedInstance.assetStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        let writeBatch = assetStore.writeBatch()
        
        for asset in assets {
            guard let descriptor = descriptorsByGlobalIdentifier[asset.globalIdentifier] else {
                completionHandler(.failure(SHAssetStoreError.invalidRequest("No descriptor provided for asset to create with global identifier \(asset.globalIdentifier)")))
                return
            }
            
            guard let senderUploadGroupId = descriptor.sharingInfo.sharedWithUserIdentifiersInGroup[descriptor.sharingInfo.sharedByUserIdentifier] else {
                completionHandler(.failure(SHAssetStoreError.invalidRequest("No groupId specified in descriptor for asset to create for sender user: userId=\(descriptor.sharingInfo.sharedByUserIdentifier)")))
                return
            }
            
            for encryptedVersion in asset.encryptedVersions.values {
                
                if encryptedVersion.quality == .hiResolution,
                   asset.encryptedVersions.keys.contains(.midResolution) == false {
                    ///
                    /// `.midResolution` is a surrogate for high-resolution when sharing only
                    /// If creation of the `.hiResolution` version happens after the `.midResolution`
                    /// there's no need to keep a copy of the mid-resolution in the local store
                    ///
                    writeBatch.set(value: nil, for: "\(SHAssetQuality.midResolution.rawValue)::" + asset.globalIdentifier)
                    writeBatch.set(value: nil, for: "data::" + "\(SHAssetQuality.midResolution.rawValue)::" + asset.globalIdentifier)
                    writeBatch.set(value: nil, for: [
                        "sender",
                        descriptor.sharingInfo.sharedByUserIdentifier,
                        SHAssetQuality.midResolution.rawValue,
                        asset.globalIdentifier
                       ].joined(separator: "::"))
                    
                    for (recipientUserId, _) in descriptor.sharingInfo.sharedWithUserIdentifiersInGroup {
                        writeBatch.set(value: nil, for: [
                            "receiver",
                            recipientUserId,
                            SHAssetQuality.midResolution.rawValue,
                            asset.globalIdentifier
                           ].joined(separator: "::"))
                    }
                }
                
                let versionMetadata: [String: Any?] = [
                    "quality": encryptedVersion.quality.rawValue,
                    "assetIdentifier": asset.globalIdentifier,
                    "applePhotosAssetIdentifier": asset.localIdentifier,
                    "senderEncryptedSecret": encryptedVersion.encryptedSecret,
                    "publicKey": encryptedVersion.publicKeyData,
                    "publicSignature": encryptedVersion.publicSignatureData,
                    "creationDate": asset.creationDate,
                    "uploadState": uploadState.rawValue
                ]
                let versionData: [String: Any?] = [
                    "assetIdentifier": asset.globalIdentifier,
                    "encryptedData": encryptedVersion.encryptedData
                ]
                
                writeBatch.set(value: versionMetadata, for: "\(encryptedVersion.quality.rawValue)::" + asset.globalIdentifier)
                writeBatch.set(value: versionData, for: "data::" + "\(encryptedVersion.quality.rawValue)::" + asset.globalIdentifier)
                writeBatch.set(value: true,
                               for: [
                                "sender",
                                descriptor.sharingInfo.sharedByUserIdentifier,
                                encryptedVersion.quality.rawValue,
                                asset.globalIdentifier
                               ].joined(separator: "::")
                )
                let sharedVersionDetails: [String: String] = [
                    "senderEncryptedSecret": encryptedVersion.encryptedSecret.base64EncodedString(),
                    "ephemeralPublicKey": encryptedVersion.publicKeyData.base64EncodedString(),
                    "publicSignature": encryptedVersion.publicSignatureData.base64EncodedString(),
                    "groupId": senderUploadGroupId,
                    "groupCreationDate": Date().iso8601withFractionalSeconds
                ]
                writeBatch.set(
                    value: sharedVersionDetails,
                    for: [
                        "receiver",
                        descriptor.sharingInfo.sharedByUserIdentifier,
                        encryptedVersion.quality.rawValue,
                        asset.globalIdentifier
                    ].joined(separator: "::")
                )
                for (recipientUserId, recipientGroupId) in descriptor.sharingInfo.sharedWithUserIdentifiersInGroup {
                    if recipientUserId == descriptor.sharingInfo.sharedByUserIdentifier {
                        continue
                    }
                    writeBatch.set(
                        value: ["groupId": recipientGroupId],
                        for: [
                            "receiver",
                            recipientUserId,
                            encryptedVersion.quality.rawValue,
                            asset.globalIdentifier
                        ].joined(separator: "::"))
                }
            }
        }
        
        writeBatch.write { (result: Swift.Result) in
            switch result {
            case .success():
                var serverAssets = [SHServerAsset]()
                for asset in assets {
                    let descriptor = descriptorsByGlobalIdentifier[asset.globalIdentifier]!
                    let senderUploadGroupId = descriptor.sharingInfo.sharedWithUserIdentifiersInGroup[descriptor.sharingInfo.sharedByUserIdentifier]!
                    var serverAssetVersions = [SHServerAssetVersion]()
                    for encryptedVersion in asset.encryptedVersions.values {
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
                                                    groupId: senderUploadGroupId,
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
                asset: any SHEncryptedAsset,
                filterVersions: [SHAssetQuality]?,
                completionHandler: @escaping (Result<Void, Error>) -> ()) {
        let group = DispatchGroup()
        for encryptedAssetVersion in asset.encryptedVersions.values {
            guard filterVersions == nil || filterVersions!.contains(encryptedAssetVersion.quality) else {
                continue
            }
            
            group.enter()
            self.markAsset(with: asset.globalIdentifier,
                           quality: encryptedAssetVersion.quality,
                           as: .completed) { _ in
                group.leave()
            }
        }
        let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds * asset.encryptedVersions.count))
        guard dispatchResult == .success else {
            return completionHandler(.failure(SHHTTPError.TransportError.timedOut))
        }
        completionHandler(.success(()))
    }
    
    func markAsset(with assetGlobalIdentifier: GlobalIdentifier,
                   quality: SHAssetQuality,
                   as state: SHAssetDescriptorUploadState,
                   completionHandler: @escaping (Result<Void, Error>) -> ()) {
        let assetStore: KBKVStore
        do {
            assetStore = try SHDBManager.sharedInstance.assetStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        let condition = KBGenericCondition(.beginsWith, value: "\(quality.rawValue)::\(assetGlobalIdentifier)")
        assetStore.dictionaryRepresentation(forKeysMatching: condition) { (result: Swift.Result) in
            switch result {
            case .success(let keyValues):
                let writeBatch = assetStore.writeBatch()
                guard keyValues.count > 0 else {
                    completionHandler(.failure(SHAssetStoreError.noEntries))
                    return
                }
                for (k, v) in keyValues {
                    guard var value = v as? [String: Any],
                          let _ = value["uploadState"] as? String?
                    else {
                        log.error("unexpected uploadState data for key \(k): \(String(describing: v))")
                        continue
                    }
                    
                    value["uploadState"] = state.rawValue
                    writeBatch.set(value: value, for: k)
                }
                writeBatch.write(completionHandler: completionHandler)
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    
    func share(asset: SHShareableEncryptedAsset,
               completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        let assetStore: KBKVStore
        do {
            assetStore = try SHDBManager.sharedInstance.assetStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
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
    
    func unshareAll(with userIdentifiers: [UserIdentifier],
                    completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        let assetStore: KBKVStore
        do {
            assetStore = try SHDBManager.sharedInstance.assetStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        var condition = KBGenericCondition(value: false)
        for userIdentifier in userIdentifiers {
            condition = condition.or(KBGenericCondition(.beginsWith, value: [
                "receiver",
                userIdentifier,
            ].joined(separator: "::")))
        }
        assetStore.removeValues(forKeysMatching: condition) { result in
            switch result {
            case .success(_):
                completionHandler(.success(()))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    func unshare(assetId: GlobalIdentifier,
                 with userPublicIdentifier: UserIdentifier,
                 completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        let assetStore: KBKVStore
        do {
            assetStore = try SHDBManager.sharedInstance.assetStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        var condition = KBGenericCondition(value: false)
        for quality in SHAssetQuality.all {
            condition = condition.or(KBGenericCondition(.equal, value: [
                "receiver",
                userPublicIdentifier,
                quality.rawValue,
                assetId
               ].joined(separator: "::")))
        }
        
        assetStore.removeValues(forKeysMatching: condition) { result in
            switch result {
            case .success(_):
                completionHandler(.success(()))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    public func getSharingInfo(forAssetIdentifier globalIdentifier: GlobalIdentifier,
                               for users: [SHServerUser],
                               completionHandler: @escaping (Swift.Result<SHShareableEncryptedAsset?, Error>) -> ()) {
        let assetStore: KBKVStore
        do {
            assetStore = try SHDBManager.sharedInstance.assetStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
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
    
    func deleteAssets(withGlobalIdentifiers globalIdentifiers: [GlobalIdentifier], completionHandler: @escaping (Result<[String], Error>) -> ()) {
        guard globalIdentifiers.count > 0 else {
            completionHandler(.success([]))
            return
        }
        
        let assetStore: KBKVStore
        do {
            assetStore = try SHDBManager.sharedInstance.assetStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        var removedGlobalIdentifiers = Set<String>()
        var err: Error? = nil
        let group = DispatchGroup()
        
        for globalIdentifierBatch in globalIdentifiers.chunked(into: 5) {
            for globalIdentifier in globalIdentifierBatch {
                var condition = KBGenericCondition(value: true)
                for quality in SHAssetQuality.all {
                    condition = condition
                        .or(
                            KBGenericCondition(.equal, value: "\(quality.rawValue)::\(globalIdentifier)"
                                              ))
                        .or(
                            KBGenericCondition(.equal, value: "data::\(quality.rawValue)::\(globalIdentifier)"
                                              ))
                }
                condition = condition.or(
                    KBGenericCondition(.beginsWith, value: "sender::").and(KBGenericCondition(.endsWith, value: globalIdentifier))
                ).or(
                    KBGenericCondition(.beginsWith, value: "receiver::").and(KBGenericCondition(.endsWith, value: globalIdentifier))
                )
                
                group.enter()
                assetStore.removeValues(forKeysMatching: condition) { result in
                    switch result {
                    case .failure(let error):
                        err = error
                    case .success(let keysRemoved):
                        for key in keysRemoved {
                            for quality in SHAssetQuality.all {
                                if key.contains("data::\(quality.rawValue)::") {
                                    removedGlobalIdentifiers.insert(String(key.suffix(key.count - 11)))
                                }
                            }
                        }
                    }
                    group.leave()
                }
            }
        }
        
        let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds * 5))
        guard dispatchResult == .success else {
            return completionHandler(.failure(SHBackgroundOperationError.timedOut))
        }
        
        if let err = err {
            completionHandler(.failure(err))
        } else {
            completionHandler(.success(Array(removedGlobalIdentifiers)))
        }
    }
    
    func createGroup(
        groupId: String,
        recipientsEncryptionDetails: [RecipientEncryptionDetailsDTO],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        let assetStore: KBKVStore
        do {
            assetStore = try SHDBManager.sharedInstance.assetStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        let writeBatch = assetStore.writeBatch()
        
        for recipientsEncryptionDetail in recipientsEncryptionDetails {
            guard recipientsEncryptionDetail.userIdentifier == self.requestor.identifier else {
                continue
            }
            
            writeBatch.set(value: nil, for: "\(groupId)::encryptedSecret" + recipientsEncryptionDetail.encryptedSecret)
            writeBatch.set(value: nil, for: "\(groupId)::ephemeralPublicKey" + recipientsEncryptionDetail.ephemeralPublicKey)
            writeBatch.set(value: nil, for: "\(groupId)::secretPublicSignature" + recipientsEncryptionDetail.secretPublicSignature)
        }
        
        writeBatch.write(completionHandler: completionHandler)
    }
    
    func addToGroup(
        groupId: String,
        recipientsEncryptionDetails: [RecipientEncryptionDetailsDTO],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func retrieveGroupUserEncryptionDetails(for groupId: String, completionHandler: @escaping (Result<RecipientEncryptionDetailsDTO, Error>) -> ()) {
        let assetStore: KBKVStore
        do {
            assetStore = try SHDBManager.sharedInstance.assetStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        let keysToRetrieve = [
            "\(groupId)::encryptedSecret",
            "\(groupId)::ephemeralPublicKey",
            "\(groupId)::secretPublicSignature"
        ]
        var condition = KBGenericCondition(value: false)
        for key in keysToRetrieve {
            condition = condition.or(KBGenericCondition(.equal, value: key))
        }
        
        assetStore.dictionaryRepresentation(forKeysMatching: condition) { result in
            switch result {
            case .success(let keyValues):
                var encryptionDetails = RecipientEncryptionDetailsDTO(
                    userIdentifier: requestor.identifier,
                    ephemeralPublicKey: "",
                    encryptedSecret: "",
                    secretPublicSignature: ""
                )
                for (k, v) in keyValues {
                    switch k {
                    case let str where str.contains("encryptedSecret"):
                        encryptionDetails.encryptedSecret = v as? String ?? encryptionDetails.encryptedSecret
                    case let str where str.contains("ephemeralPublicKey"):
                        encryptionDetails.ephemeralPublicKey = v as? String ?? encryptionDetails.ephemeralPublicKey
                    case let str where str.contains("secretPublicSignature"):
                        encryptionDetails.secretPublicSignature = v as? String ?? encryptionDetails.secretPublicSignature
                    default:
                        break
                    }
                }
                guard !encryptionDetails.encryptedSecret.isEmpty,
                      !encryptionDetails.encryptedSecret.isEmpty,
                      !encryptionDetails.encryptedSecret.isEmpty else {
                    completionHandler(.failure(KBError.unexpectedData(keyValues)))
                    return
                }
                completionHandler(.success(encryptionDetails))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    func addReactions(
        _ reactions: [ReactionOutputDTO],
        toGroupId groupId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        let reactionStore: KBKVStore
        do {
            reactionStore = try SHDBManager.sharedInstance.reactionStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        let writeBatch = reactionStore.writeBatch()
        
        for reaction in reactions {
            var key = "\(groupId)::\(reaction.senderUserIdentifier)::\(reaction.interactionId)"
            if let assetGid = reaction.inReplyToAssetGlobalIdentifier {
                key += "::\(assetGid)"
            } else {
                key += "::"
            }
            if let interactionId = reaction.inReplyToInteractionId {
                key += "::\(interactionId)"
            } else {
                key += "::"
            }
            writeBatch.set(value: reaction.reactionType.rawValue, for: key)
        }
        
        writeBatch.write(completionHandler: completionHandler)
    }
    
    func removeReaction(
        withIdentifier interactionId: String,
        fromGroupId groupId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        let reactionStore: KBKVStore
        do {
            reactionStore = try SHDBManager.sharedInstance.reactionStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        let condition = KBGenericCondition(.beginsWith, value: "\(groupId)::\(self.requestor.identifier)::\(interactionId)")
        reactionStore.removeValues(forKeysMatching: condition) { result in
            switch result {
            case .success(_):
                completionHandler(.success(()))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    func retrieveInteractions(
        in groupId: String,
        per: Int,
        page: Int,
        completionHandler: @escaping (Result<InteractionsGroupDTO, Error>) -> ()
    ) {
        
        let reactionStore: KBKVStore
        do {
            reactionStore = try SHDBManager.sharedInstance.reactionStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        let messagesStore: KBQueueStore
        do {
            messagesStore = try SHDBManager.sharedInstance.messageQueue()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        self.retrieveGroupUserEncryptionDetails(for: groupId) { encryptionDetailsResult in
            switch encryptionDetailsResult {
            case .failure(let err):
                completionHandler(.failure(err))
            case .success(let encryptionDetails):
                let condition = KBGenericCondition(.beginsWith, value: "\(groupId)::")
                reactionStore.keyValuesAndTimestamps(forKeysMatching: condition) { reactionsResult in
                    switch reactionsResult {
                    case .success(let reactionKvts):
                        var reactions = [ReactionOutputDTO]()
                        reactionKvts.forEach({
                            let key = $0.key
                            var interactionId: String? = nil
                            var senderId: String? = nil
                            var inReplyToAssetGid: String? = nil
                            var inReplyToInteractionGid: String? = nil
                            
                            let keyComponents = key.components(separatedBy: "::")
                            guard keyComponents.count > 3 else {
                                log.warning("unexpected key format in reactions DB: \(key)")
                                return
                            }
                            senderId = keyComponents[1]
                            interactionId = keyComponents[2]
                            if keyComponents.count > 4,
                                !keyComponents[3].isEmpty {
                                inReplyToAssetGid = keyComponents[3]
                            }
                            if keyComponents.count > 5,
                                !keyComponents[4].isEmpty {
                                inReplyToInteractionGid = keyComponents[4]
                            }
                            
                            guard let senderId = senderId,
                                  let interactionId = interactionId else {
                                log.warning("invalid key format in reactions DB: \(key)")
                                return
                            }
                            
                            guard let reactionTypeInt = $0.value as? Int,
                                  let reactionType = ReactionType(rawValue: reactionTypeInt) else {
                                log.warning("unexpected value in reactions DB: \(String(describing: $0.value))")
                                return
                            }
                            
                            let output = ReactionOutputDTO(
                                interactionId: interactionId,
                                senderUserIdentifier: senderId,
                                inReplyToAssetGlobalIdentifier: inReplyToAssetGid,
                                inReplyToInteractionId: inReplyToInteractionGid,
                                reactionType: reactionType,
                                addedAt: $0.timestamp.iso8601withFractionalSeconds
                            )
                            reactions.append(output)
                        })
                        messagesStore.keyValuesAndTimestamps(forKeysMatching: condition) { messagesResult in
                            switch messagesResult {
                            case .success(let messageKvts):
                                var messages = [MessageOutputDTO]()
                                // TODO: Implement messages retrieval and decryption
                                let result = InteractionsGroupDTO(
                                    messages: messages,
                                    reactions: reactions,
                                    ephemeralPublicKey: encryptionDetails.ephemeralPublicKey,
                                    encryptedSecret: encryptionDetails.encryptedSecret,
                                    secretPublicSignature: encryptionDetails.secretPublicSignature
                                )
                                completionHandler(.success(result))
                            case .failure(let err):
                                completionHandler(.failure(err))
                            }
                        }
                    case .failure(let err):
                        completionHandler(.failure(err))
                    }
                }
            }
        }
    }
    
    func addMessage(
        _ message: MessageInputDTO,
        toGroupId: String,
        completionHandler: @escaping (Result<MessageOutputDTO, Error>) -> ()
    ) {
        // TODO: Implement message enqueueing
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
}
