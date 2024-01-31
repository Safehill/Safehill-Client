import Foundation
import KnowledgeBase
import Contacts

public let SHDefaultDBTimeoutInMilliseconds = 15000 // 15 seconds

struct LocalServer : SHServerAPI {
    
    let requestor: SHLocalUser
    
    init(requestor: SHLocalUser) {
        self.requestor = requestor
    }
    
    internal func createOrUpdateUser(identifier: UserIdentifier,
                                     name: String,
                                     publicKeyData: Data,
                                     publicSignatureData: Data,
                                     completionHandler: @escaping (Result<any SHServerUser, Error>) -> ()) {
        let userStore: KBKVStore
        do {
            userStore = try SHDBManager.sharedInstance.userStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        userStore.value(for: identifier) { getResult in
            switch getResult {
            case .success(let oldValue):
                var newValue = [String : Any]()
                
                if let oldValue = oldValue as? [String: Any] {
                    ///
                    /// User already exists. Update it
                    ///
                    newValue = oldValue
                    newValue["identifier"] = identifier
                    newValue["publicKey"] = publicKeyData
                    newValue["publicSignature"] = publicSignatureData
                    newValue["name"] = name
                } else {
                    ///
                    /// User doesn't exists. Create it
                    ///
                    newValue = [
                        "identifier": identifier,
                        "publicKey": publicKeyData,
                        "publicSignature": publicSignatureData,
                        "name": name,
                    ] as [String : Any]
                }
                userStore.set(value: newValue, for: identifier) { (postResult: Result) in
                    switch postResult {
                    case .success:
                        let serverUser = serializeUser(newValue)!
                        completionHandler(.success(serverUser))
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
    
    func sendCodeToUser(countryCode: Int,
                        phoneNumber: Int,
                        code: String,
                        medium: SendCodeToUserRequestDTO.Medium,
                        completionHandler: @escaping (Result<Void, Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func updateUser(name: String?,
                    phoneNumber: SHPhoneNumber? = nil,
                    completionHandler: @escaping (Result<any SHServerUser, Error>) -> ()) {
        guard name != nil || phoneNumber != nil else {
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
                var value = [String : Any]()
                if let user = user as? [String: Any] {
                    guard user["publicKey"] as? Data == self.requestor.publicKeyData,
                          user["publicSignature"] as? Data == self.requestor.publicSignatureData
                    else {
                        completionHandler(.failure(SHHTTPError.ClientError.methodNotAllowed))
                        return
                    }
                    
                    value = [
                        "identifier": key,
                        "publicKey": requestor.publicKeyData,
                        "publicSignature": requestor.publicSignatureData
                    ]
                    if let existingName = user["name"] {
                        value["name"] = existingName
                    }
                    if let existingPn = user["phoneNumber"] {
                        value["phoneNumber"] = existingPn
                    }
                } else {
                    value = [
                        "identifier": key,
                        "publicKey": requestor.publicKeyData,
                        "publicSignature": requestor.publicSignatureData
                    ]
                }
                
                if let name = name {
                    value["name"] = name
                }
                if let phoneNumber = phoneNumber {
                    value["phoneNumber"] = phoneNumber.e164FormattedNumber
                }
                
                userStore.set(value: value, for: key) { (postResult: Result) in
                    switch postResult {
                    case .success:
                        let serializedUser = serializeUser(value)!
                        completionHandler(.success(serializedUser))
                    case .failure(let err):
                        completionHandler(.failure(err))
                    }
                }
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    func update(user: SHRemoteUser,
                phoneNumber: SHPhoneNumber,
                linkedSystemContact: CNContact,
                completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        let userStore: KBKVStore
        do {
            userStore = try SHDBManager.sharedInstance.userStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        let value = [
            "identifier": user.identifier,
            "name": user.name,
            "phoneNumber": phoneNumber.e164FormattedNumber,
            "publicKey": user.publicKeyData,
            "publicSignature": user.publicSignatureData,
            "systemContactId": linkedSystemContact.identifier
        ] as [String : Any]
        
        userStore.set(value: value, for: user.identifier) { (postResult: Result) in
            switch postResult {
            case .success:
                completionHandler(.success(()))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    func removeLinkedSystemContact(from users: [SHRemoteUserLinkedToContact],
                                   completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        let userStore: KBKVStore
        do {
            userStore = try SHDBManager.sharedInstance.userStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        let writeBatch = userStore.writeBatch()
        
        for user in users {
            let value = [
                "identifier": user.identifier,
                "name": user.name,
                "phoneNumber": user.phoneNumber,
                "publicKey": user.publicKeyData,
                "publicSignature": user.publicSignatureData
            ] as [String : Any]
            
            writeBatch.set(value: value, for: user.identifier)
        }
        
        writeBatch.write { (result: Result) in
            switch result {
            case .success:
                completionHandler(.success(()))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    func createOrUpdateUser(name: String, completionHandler: @escaping (Result<any SHServerUser, Error>) -> ()) {
        self.createOrUpdateUser(
            identifier: requestor.identifier,
            name: name,
            publicKeyData: requestor.publicKeyData,
            publicSignatureData: requestor.publicSignatureData,
            completionHandler: completionHandler
        )
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
                do {
                    try SHKGQuery.removeUsers(with: identifiers)
                    completionHandler(.success(()))
                } catch {
                    completionHandler(.failure(error))
                }
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    func deleteAccount(name: String, password: String, completionHandler: @escaping (Result<Void, Error>) -> ()) {
        self.deleteAccount(completionHandler: completionHandler)
    }
    
    func deleteAllAssets(completionHandler: @escaping (Result<[String], Error>) -> ()) {
        let assetStore: KBKVStore
        do {
            assetStore = try SHDBManager.sharedInstance.assetStore()
            assetStore.removeAll(completionHandler: completionHandler)
            try SHKGQuery.deepClean()
        } catch {
            completionHandler(.failure(error))
            return
        }
    }
    
    func deleteAccount(completionHandler: @escaping (Result<Void, Error>) -> ()) {
        let userStore: KBKVStore
        do {
            userStore = try SHDBManager.sharedInstance.userStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        let reactionStore: KBKVStore
        do {
            reactionStore = try SHDBManager.sharedInstance.reactionStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        let messagesQueue: KBQueueStore
        do {
            messagesQueue = try SHDBManager.sharedInstance.messageQueue()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        var userRemovalError: Error? = nil
        var assetsRemovalError: Error? = nil
        var reactionsRemovalError: Error? = nil
        var messagesRemovalError: Error? = nil
        let group = DispatchGroup()
        
        group.enter()
        userStore.removeAll { result in
            if case .failure(let err) = result {
                userRemovalError = err
            }
            group.leave()
        }

        group.enter()
        reactionStore.removeAll { result in
            if case .failure(let err) = result {
                reactionsRemovalError = err
            }
            group.leave()
        }
        
        group.enter()
        messagesQueue.removeAll { result in
            if case .failure(let err) = result {
                messagesRemovalError = err
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
        guard reactionsRemovalError == nil else {
            return completionHandler(.failure(reactionsRemovalError!))
        }
        guard messagesRemovalError == nil else {
            return completionHandler(.failure(messagesRemovalError!))
        }
        completionHandler(.success(()))
    }
    
    public func signIn(clientBuild: Int?, completionHandler: @escaping (Result<SHAuthResponse, Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    private func serializeUser(_ res: Any?) -> (any SHServerUser)? {
        var serialized: (any SHServerUser)? = nil
        
        if let res = res as? [String: Any] {
            if let identifier = res["identifier"] as? String,
               let name = res["name"] as? String,
               let publicKeyData = res["publicKey"] as? Data,
               let publicSignatureData = res["publicSignature"] as? Data {
                
                let remoteUser: SHServerUser
                if let phoneNumber = res["phoneNumber"] as? String,
                   let systemContactId = res["systemContactId"] as? String {
                    remoteUser = SHRemoteUserLinkedToContact(
                        identifier: identifier,
                        name: name,
                        publicKeyData: publicKeyData,
                        publicSignatureData: publicSignatureData,
                        phoneNumber: phoneNumber,
                        linkedSystemContactId: systemContactId
                    )
                } else {
                    remoteUser = SHRemoteUser(
                        identifier: identifier,
                        name: name,
                        publicKeyData: publicKeyData,
                        publicSignatureData: publicSignatureData
                    )
                }
                serialized = remoteUser
            }
        }
        
        return serialized
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
                var userList = [any SHServerUser]()
                if let resList = resList as? [[String: Any]] {
                    for res in resList {
                        if let serverUser = serializeUser(res) {
                            userList.append(serverUser)
                        } else {
                            log.warning("unable to serialize user in local DB \(res)")
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
    
    func getUsers(withHashedPhoneNumbers hashedPhoneNumbers: [String], completionHandler: @escaping (Result<[String: any SHServerUser], Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func getAllLocalUsers(completionHandler: @escaping (Swift.Result<[any SHServerUser], Error>) -> ()) {
        let userStore: KBKVStore
        do {
            userStore = try SHDBManager.sharedInstance.userStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        userStore.values() { getResult in
            switch getResult {
            case .success(let resList):
                var userList = [any SHServerUser]()
                if let resList = resList as? [[String: Any]] {
                    for res in resList {
                        if let serverUser = serializeUser(res) {
                            userList.append(serverUser)
                        } else {
                            log.warning("unable to serialize user in local DB \(res)")
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
    
    func getAssetDescriptors(forAssetGlobalIdentifiers: [GlobalIdentifier]? = nil,
                             completionHandler: @escaping (Result<[any SHAssetDescriptor], Error>) -> ()) {
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
        
        assetStore.dictionaryRepresentation(forKeysMatching: condition) { (result: Result) in
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
    
    func getAssets(withGlobalIdentifiers assetIdentifiers: [GlobalIdentifier],
                   versions: [SHAssetQuality]? = nil,
                   completionHandler: @escaping (Result<[GlobalIdentifier: any SHEncryptedAsset], Error>) -> ()) {
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
        
        var resultDictionary = [GlobalIdentifier: any SHEncryptedAsset]()
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
                (result: Result) in
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
        
        group.notify(queue: .global()) {
            if let err = err {
                completionHandler(.failure(err))
            } else {
                completionHandler(.success(resultDictionary))
            }
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
        
        writeBatch.write { (result: Result) in
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
    
    func addAssetRecipients(to globalIdentifier: GlobalIdentifier,
                            basedOn groupIdByRecipientId: [UserIdentifier: String],
                            versions: [SHAssetQuality]? = nil,
                            completionHandler: @escaping (Result<Void, Error>) -> ()) {
        let assetStore: KBKVStore
        do {
            assetStore = try SHDBManager.sharedInstance.assetStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        let versions = versions ?? SHAssetQuality.all
        
        let writeBatch = assetStore.writeBatch()
        
        for version in versions {
            for (recipientUserId, groupId) in groupIdByRecipientId {
                writeBatch.set(
                    value: ["groupId": groupId],
                    for: [
                        "receiver",
                        recipientUserId,
                        version.rawValue,
                        globalIdentifier
                       ].joined(separator: "::")
                )
            }
        }
        
        writeBatch.write(completionHandler: completionHandler)
    }
    
    func removeAssetRecipients(recipientUserIds: [UserIdentifier],
                               from globalIdentifier: GlobalIdentifier,
                               versions: [SHAssetQuality]? = nil,
                               completionHandler: @escaping (Result<Void, Error>) -> ()) {
        let assetStore: KBKVStore
        do {
            assetStore = try SHDBManager.sharedInstance.assetStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        let versions = versions ?? SHAssetQuality.all
        
        let writeBatch = assetStore.writeBatch()
        
        for version in versions {
            for recipientUserId in recipientUserIds {
                writeBatch.set(value: nil, for: [
                    "receiver",
                    recipientUserId,
                    version.rawValue,
                    globalIdentifier
                   ].joined(separator: "::"))
            }
        }
        
        writeBatch.write(completionHandler: completionHandler)
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
        assetStore.dictionaryRepresentation(forKeysMatching: condition) { (result: Result) in
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
               completionHandler: @escaping (Result<Void, Error>) -> ()) {
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
    
    func add(phoneNumbers: [SHPhoneNumber], to groupId: String, completionHandler: @escaping (Result<Void, Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func unshareAll(with userIdentifiers: [UserIdentifier],
                    completionHandler: @escaping (Result<Void, Error>) -> ()) {
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
                 completionHandler: @escaping (Result<Void, Error>) -> ()) {
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
                               completionHandler: @escaping (Result<SHShareableEncryptedAsset?, Error>) -> ()) {
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
        
        for globalIdentifierBatch in globalIdentifiers.chunked(into: 10) {
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
        
        guard err == nil else {
            completionHandler(.failure(err!))
            return
        }
        
        do {
            try SHKGQuery.removeAssets(with: globalIdentifiers)
        } catch {
            completionHandler(.failure(error))
        }
        
        completionHandler(.success(Array(removedGlobalIdentifiers)))
    }
    
    @available(*, deprecated, renamed: "createOrUpdateThread(threadId:name:lastUpdatedAt:recipientEncryptionDetails:completionHandler:)", message: "Do not use the protocol method when storing a thread locally. Information from server should be provided.")
    func createOrUpdateThread(
        name: String?,
        recipientsEncryptionDetails: [RecipientEncryptionDetailsDTO]?,
        completionHandler: @escaping (Result<ConversationThreadOutputDTO, Error>) -> ()
    ) {
        completionHandler(.failure(SHHTTPError.ClientError.badRequest("Call the sister method and provide a thread identifier when storing a thread to local. This method should not be called.")))
    }
    
    /// Creates a new thread on the local database
    /// - Parameters:
    ///   - serverThread: the thread retrieved from the server to store locally
    ///   - completionHandler: the callback, returning
    func createOrUpdateThread(
        serverThread: ConversationThreadOutputDTO,
        completionHandler: @escaping (Result<ConversationThreadOutputDTO, Error>) -> ()
    ) {
        guard serverThread.encryptionDetails.userIdentifier == self.requestor.identifier
        else {
            completionHandler(.failure(SHHTTPError.ClientError.badRequest("encryption details don't match the requestor")))
            return
        }
        
        let userStore: KBKVStore
        do {
            userStore = try SHDBManager.sharedInstance.userStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        let writeBatch = userStore.writeBatch()
        
        writeBatch.set(value: serverThread.encryptionDetails.encryptedSecret, for: "\(InteractionAnchor.thread.rawValue)::\(serverThread.threadId)::encryptedSecret")
        writeBatch.set(value: serverThread.encryptionDetails.ephemeralPublicKey, for: "\(InteractionAnchor.thread.rawValue)::\(serverThread.threadId)::ephemeralPublicKey")
        writeBatch.set(value: serverThread.encryptionDetails.secretPublicSignature, for: "\(InteractionAnchor.thread.rawValue)::\(serverThread.threadId)::secretPublicSignature")
        
        writeBatch.set(value: serverThread.name, for: "\(InteractionAnchor.thread.rawValue)::\(serverThread.threadId)::name")
        writeBatch.set(value: serverThread.lastUpdatedAt?.iso8601withFractionalSeconds?.timeIntervalSince1970, for: "\(InteractionAnchor.thread.rawValue)::\(serverThread.threadId)::lastUpdatedAt")
        
        writeBatch.write { result in
            switch result {
            case .failure(let error):
                completionHandler(.failure(error))
            case .success:
                completionHandler(.success(serverThread))
            }
        }
    }
    
    func listThreads(
        completionHandler: @escaping (Result<[ConversationThreadOutputDTO], Error>) -> ()
    ) {
        let userStore: KBKVStore
        do {
            userStore = try SHDBManager.sharedInstance.userStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        let condition = KBGenericCondition(.beginsWith, value: "\(InteractionAnchor.thread.rawValue)::")
        
        let kvPairs: KBKVPairs
        do {
            kvPairs = try userStore
                .dictionaryRepresentation(forKeysMatching: condition)
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        let list = kvPairs.reduce([String: ConversationThreadOutputDTO](), { (partialResult, pair) in
            let (key, value) = pair
            
            let components = key.components(separatedBy: "::")
            guard components.count == 3 else {
                log.warning("invalid key in local DB for thread: \(String(describing: key))")
                return partialResult
            }
            
            let threadId = components[1]
            var name: String? = nil
            var lastUpdatedAt: Date? = nil
            var encryptedSecret: String? = nil
            var ephemeralPublicKey: String? = nil
            var secretPublicSignature: String? = nil
            
            switch components[2] {
            case "encryptedSecret":
                encryptedSecret = value as? String
            case "ephemeralPublicKey":
                ephemeralPublicKey = value as? String
            case "secretPublicSignature":
                secretPublicSignature = value as? String
            case "name":
                name = value as? String
            case "lastUpdatedAt":
                if let lastUpdatedInterval = value as? Double {
                    lastUpdatedAt = Date(timeIntervalSince1970: lastUpdatedInterval)
                }
            default:
                break
            }
            
            var result = partialResult
            if let encryptedSecret, let ephemeralPublicKey, let secretPublicSignature {
                result[threadId] = ConversationThreadOutputDTO(
                    threadId: threadId,
                    name: name,
                    membersPublicIdentifier: [],
                    lastUpdatedAt: lastUpdatedAt?.iso8601withFractionalSeconds,
                    encryptionDetails: RecipientEncryptionDetailsDTO(
                        userIdentifier: self.requestor.identifier,
                        ephemeralPublicKey: ephemeralPublicKey,
                        encryptedSecret: encryptedSecret,
                        secretPublicSignature: secretPublicSignature
                    )
                )
            }
            
            return result
        })
            .values
        
        completionHandler(.success(Array(list)))
    }
    
    func setGroupEncryptionDetails(
        groupId: String,
        recipientsEncryptionDetails: [RecipientEncryptionDetailsDTO],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        guard let selfEncryptionDetails = recipientsEncryptionDetails.first(where: { $0.userIdentifier == self.requestor.identifier })
        else {
            completionHandler(.failure(SHHTTPError.ClientError.badRequest("encryption details don't match the requestor")))
            return
        }
        
        let userStore: KBKVStore
        do {
            userStore = try SHDBManager.sharedInstance.userStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        let writeBatch = userStore.writeBatch()

        writeBatch.set(value: selfEncryptionDetails.encryptedSecret, for: "\(InteractionAnchor.group.rawValue)::\(groupId)::encryptedSecret")
        writeBatch.set(value: selfEncryptionDetails.ephemeralPublicKey, for: "\(InteractionAnchor.group.rawValue)::\(groupId)::ephemeralPublicKey")
        writeBatch.set(value: selfEncryptionDetails.secretPublicSignature, for: "\(InteractionAnchor.group.rawValue)::\(groupId)::secretPublicSignature")
        
        writeBatch.write(completionHandler: completionHandler)
    }
    
    func deleteGroup(
        groupId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        self.delete(anchor: .group, anchorId: groupId, completionHandler: completionHandler)
    }
    
    private func delete(
        anchor: InteractionAnchor,
        anchorId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        let userStore: KBKVStore
        do {
            userStore = try SHDBManager.sharedInstance.userStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
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
        
        do {
            let condition = KBGenericCondition(.beginsWith, value: "\(anchor.rawValue)::\(anchorId)::")
            let _ = try userStore.removeValues(forKeysMatching: condition)
            let _ = try reactionStore.removeValues(forKeysMatching: condition)
            let _ = try messagesStore.removeValues(forKeysMatching: condition)
            completionHandler(.success(()))
        } catch {
            completionHandler(.failure(error))
        }
    }
    
    func retrieveUserEncryptionDetails(
        forGroup groupId: String,
        completionHandler: @escaping (Result<RecipientEncryptionDetailsDTO?, Error>) -> ()
    ) {
        self.retrieveUserEncryptionDetails(
            anchorType: .group,
            anchorId: groupId,
            completionHandler: completionHandler
        )
    }
    
    func getThread(
        withId threadId: String,
        completionHandler: @escaping (Result<ConversationThreadOutputDTO?, Error>) -> ()
    ) {
        self.retrieveUserEncryptionDetails(
            anchorType: .thread,
            anchorId: threadId
        ) { e2eeResult in
            switch e2eeResult {
            case .failure(let err):
                completionHandler(.failure(err))
            case .success(let encryptionDetails):
                guard let encryptionDetails else {
                    completionHandler(.success(nil))
                    return
                }
                
                let userStore: KBKVStore
                do {
                    userStore = try SHDBManager.sharedInstance.userStore()
                    
                    let threadName = try userStore.value(for: "\(InteractionAnchor.thread.rawValue)::\(threadId)::name") as? String
                    let threadLastUpdated = try userStore.value(for: "\(InteractionAnchor.thread.rawValue)::\(threadId)::lastUpdatedAt") as? Double
                    let lastUpdatedAt = threadLastUpdated == nil ? Date() : Date(timeIntervalSince1970: threadLastUpdated!)
                    let thread = ConversationThreadOutputDTO(
                        threadId: threadId,
                        name: threadName,
                        membersPublicIdentifier: [],
                        lastUpdatedAt: lastUpdatedAt.iso8601withFractionalSeconds,
                        encryptionDetails: encryptionDetails
                    )
                    
                    completionHandler(.success(thread))
                } catch {
                    completionHandler(.failure(error))
                    return
                }
            }
        }
    }
    
    @available(*, deprecated, message: "Local database doesn't store thread members information, only keys and name for local threads")
    func getThread(
        withUsers users: [any SHServerUser],
        completionHandler: @escaping (Result<ConversationThreadOutputDTO?, Error>) -> ()
    ) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    private func retrieveUserEncryptionDetails(
        anchorType: InteractionAnchor,
        anchorId: String,
        completionHandler: @escaping (Result<RecipientEncryptionDetailsDTO?, Error>) -> ()
    ) {
        let userStore: KBKVStore
        do {
            userStore = try SHDBManager.sharedInstance.userStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        let keysToRetrieve = [
            "\(anchorType.rawValue)::\(anchorId)::encryptedSecret",
            "\(anchorType.rawValue)::\(anchorId)::ephemeralPublicKey",
            "\(anchorType.rawValue)::\(anchorId)::secretPublicSignature"
        ]
        var condition = KBGenericCondition(value: false)
        for key in keysToRetrieve {
            condition = condition.or(KBGenericCondition(.equal, value: key))
        }
        
        userStore.dictionaryRepresentation(forKeysMatching: condition) { (result: Result<KBKVPairs, Error>) in
            switch result {
            case .success(let keyValues):
                guard keyValues.count > 0 else {
                    completionHandler(.success(nil))
                    return
                }
                
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
    
    func deleteThread(withId threadId: String, completionHandler: @escaping (Result<Void, Error>) -> ()) {
        self.delete(anchor: .thread, anchorId: threadId, completionHandler: completionHandler)
    }
    
    func addReactions(
        _ reactions: [ReactionInput],
        inGroup groupId: String,
        completionHandler: @escaping (Result<[ReactionOutputDTO], Error>) -> ()
    ) {
        self.addReactions(reactions, anchorType: .group, anchorId: groupId, completionHandler: completionHandler)
    }
    
    func addReactions(
        _ reactions: [ReactionInput],
        inThread threadId: String,
        completionHandler: @escaping (Result<[ReactionOutputDTO], Error>) -> ()
    ) {
        self.addReactions(reactions, anchorType: .thread, anchorId: threadId, completionHandler: completionHandler)
    }
    
    private func addReactions(
        _ reactions: [ReactionInput],
        anchorType: InteractionAnchor,
        anchorId: String,
        completionHandler: @escaping (Result<[ReactionOutputDTO], Error>) -> ()
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
            guard let interactionId = reaction.interactionId else {
                log.warning("can not save interaction to local store without an interaction identifier from server")
                continue
            }
            var key = "\(anchorType.rawValue)::\(anchorId)::\(interactionId)"
            if let interactionId = reaction.inReplyToInteractionId {
                key += "::\(interactionId)"
            } else {
                key += "::"
            }
            if let assetGid = reaction.inReplyToAssetGlobalIdentifier {
                key += "::\(assetGid)"
            } else {
                key += "::"
            }
            key += "::\(reaction.senderUserIdentifier!)"
            writeBatch.set(value: reaction.reactionType.rawValue, for: key)
        }
        
        writeBatch.write { result in
            switch result {
            case .failure(let error):
                completionHandler(.failure(error))
            case .success():
                completionHandler(.success(reactions.map({
                    $0 as! ReactionOutputDTO
                })))
            }
        }
    }
    
    func removeReactions(
        _ reactions: [ReactionInput],
        inGroup groupId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        self.removeReactions(reactions, anchorType: .group, anchorId: groupId, completionHandler: completionHandler)
    }
    
    func removeReactions(
        _ reactions: [ReactionInput],
        inThread threadId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        self.removeReactions(reactions, anchorType: .thread, anchorId: threadId, completionHandler: completionHandler)
    }
    
    private func removeReactions(
        _ reactions: [ReactionInput],
        anchorType: InteractionAnchor,
        anchorId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        let reactionStore: KBKVStore
        do {
            reactionStore = try SHDBManager.sharedInstance.reactionStore()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        var condition = KBGenericCondition(value: false)
        for reaction in reactions {
            let keyStart = "\(anchorType.rawValue)::\(anchorId)"
            var keyEnd = ""
            if let interactionId = reaction.inReplyToInteractionId {
                keyEnd += "::\(interactionId)"
            } else {
                keyEnd += "::"
            }
            if let assetGid = reaction.inReplyToAssetGlobalIdentifier {
                keyEnd += "::\(assetGid)"
            } else {
                keyEnd += "::"
            }
            keyEnd += "::\(reaction.senderUserIdentifier!)"
            
            let thisCondition = KBGenericCondition(.beginsWith, value: keyStart).and(KBGenericCondition(.endsWith, value: keyEnd))
            
            condition = condition.or(thisCondition)
        }
        
        reactionStore.removeValues(forKeysMatching: condition) { result in
            switch result {
            case .success(_):
                completionHandler(.success(()))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    func countInteractions(
        inGroup groupId: String,
        completionHandler: @escaping (Result<InteractionsCounts, Error>) -> ()
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
        
        var counts: InteractionsCounts = (reactions: [ReactionType: [UserIdentifier]](), messages: 0)
        
        let condition = KBGenericCondition(.beginsWith, value: "\(InteractionAnchor.group.rawValue)::\(groupId)::")
        reactionStore.dictionaryRepresentation(forKeysMatching: condition) { reactionsResult in
            switch reactionsResult {
            case .success(let reactionsKeysAndValues):
                var reactionsCountDict = [ReactionType: [UserIdentifier]]()
                for (k, v) in reactionsKeysAndValues {
                    guard let rawValue = v as? Int, let reactionType = ReactionType(rawValue: rawValue) else {
                        log.warning("unknown reaction type in local DB for group \(groupId): \(String(describing: v))")
                        continue
                    }
                    let components = k.components(separatedBy: "::")
                    guard components.count >= 2 else {
                        log.warning("invalid reaction key in local DB for group \(groupId): \(String(describing: k))")
                        continue
                    }
                    let senderIdentifier = components[1]
                    if reactionsCountDict[reactionType] != nil {
                        reactionsCountDict[reactionType]!.append(senderIdentifier)
                    } else {
                        reactionsCountDict[reactionType] = [senderIdentifier]
                    }
                }
                counts.reactions = reactionsCountDict
            case .failure(let error):
                log.critical("failed to retrieve reactions for group \(groupId): \(error.localizedDescription)")
            }
            messagesStore.keys(matching: condition) { messagesResult in
                switch messagesResult {
                case .success(let messagesKeys):
                    counts.messages = messagesKeys.count
                case .failure(let error):
                    log.critical("failed to retrieve messages for group \(groupId): \(error.localizedDescription)")
                }
                completionHandler(.success(counts))
            }
        }
    }
    
    func retrieveInteractions(
        inGroup groupId: String,
        underMessage messageId: String?,
        per: Int,
        page: Int,
        completionHandler: @escaping (Result<InteractionsGroupDTO, Error>) -> ()
    ) {
        self.retrieveInteractions(
            anchorType: .group,
            anchorId: groupId,
            underMessage: messageId,
            per: per,
            page: page,
            completionHandler: completionHandler
        )
    }
    
    func retrieveInteractions(
        inThread threadId: String,
        underMessage messageId: String?,
        per: Int,
        page: Int,
        completionHandler: @escaping (Result<InteractionsGroupDTO, Error>) -> ()
    ) {
        self.retrieveInteractions(
            anchorType: .thread,
            anchorId: threadId,
            underMessage: messageId,
            per: per,
            page: page,
            completionHandler: completionHandler
        )
    }
    
    private func retrieveInteractions(
        anchorType: InteractionAnchor,
        anchorId: String,
        underMessage refMessageId: String?,
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
        
        self.retrieveUserEncryptionDetails(anchorType: anchorType, anchorId: anchorId) {
            encryptionDetailsResult in
            switch encryptionDetailsResult {
            case .failure(let err):
                completionHandler(.failure(err))
            case .success(let e2eeResult):
                guard let encryptionDetails = e2eeResult else {
                    switch anchorType {
                    case .group:
                        completionHandler(.failure(SHBackgroundOperationError.missingE2EEDetailsForGroup(anchorId)))
                    case .thread:
                        completionHandler(.failure(SHBackgroundOperationError.missingE2EEDetailsForThread(anchorId)))
                    }
                    return
                }
                
                ///
                /// KEY FORMAT:
                /// `{anchor_type}::{anchor_id}::{interaction_id}::{ref_interaction_id}::{ref_asset_id}::{sender_id}`
                /// - `anchor_type`: either "thread' or "group", for threads and shares, respectively
                /// - `anchor_id`: either the `threadId` or the `groupId`, to identify a thread or a share, respectively
                /// - `interaction_id`: the unique interaction identifier as provided by the server
                /// - `ref_interaction_id`: a pointer to the interaction this interaction references (for replies to messages the origin message id)
                /// - `ref_asset_id`: a pointer to the global asset identifer this interaction references
                /// - `sender_id`: the user public identifier, author of the interaction
                ///
                
                var conditionStr = "\(anchorType)::\(anchorId)::"
                if let refMessageId {
                    conditionStr += refMessageId
                }
                let condition = KBGenericCondition(.beginsWith, value: conditionStr)
                reactionStore.keyValuesAndTimestamps(
                    forKeysMatching: condition,
                    paginate: KBPaginationOptions(page: page, per: per),
                    sort: .descending
                ) { reactionsResult in
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
                            guard keyComponents.count > 5 else {
                                log.warning("unexpected key format in reactions DB: \(key)")
                                return
                            }
                            guard let reactionTypeInt = $0.value as? Int,
                                  let reactionType = ReactionType(rawValue: reactionTypeInt) else {
                                log.warning("unexpected value in reactions DB: \(String(describing: $0.value))")
                                return
                            }
                            
                            senderId = keyComponents[5]
                            interactionId = keyComponents[2]
                            
                            guard let senderId = senderId,
                                  let interactionId = interactionId else {
                                log.warning("invalid key format in reactions DB: \(key)")
                                return
                            }
                            
                            if !keyComponents[3].isEmpty {
                                inReplyToInteractionGid = keyComponents[3]
                            }
                            if !keyComponents[4].isEmpty {
                                inReplyToAssetGid = keyComponents[4]
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
                        
                        messagesStore.keyValuesAndTimestamps(
                            forKeysMatching: condition,
                            paginate: KBPaginationOptions(page: page, per: per),
                            sort: .descending
                        ) { messagesResult in
                            switch messagesResult {
                            case .success(let messageKvts):
                                var messages = [MessageOutputDTO]()
                                
                                for messageKvt in messageKvts {
                                    do {
                                        guard let serializedMessage = messageKvt.value as? Data else {
                                            throw SHBackgroundOperationError.unexpectedData(messageKvt.value)
                                        }
                                        
                                        let unarchiver: NSKeyedUnarchiver
                                        if #available(macOS 10.13, *) {
                                            unarchiver = try NSKeyedUnarchiver(forReadingFrom: serializedMessage)
                                        } else {
                                            unarchiver = NSKeyedUnarchiver(forReadingWith: serializedMessage)
                                        }
                                        guard let message = unarchiver.decodeObject(
                                            of: DBSecureSerializableUserMessage.self,
                                            forKey: NSKeyedArchiveRootObjectKey
                                        ) else {
                                            continue
                                        }
                                        
                                        messages.append(
                                            MessageOutputDTO(
                                                interactionId: message.interactionId,
                                                senderUserIdentifier: message.senderUserIdentifier,
                                                inReplyToAssetGlobalIdentifier: message.inReplyToAssetGlobalIdentifier,
                                                inReplyToInteractionId: message.inReplyToInteractionId,
                                                encryptedMessage: message.encryptedMessage,
                                                createdAt: message.createdAt
                                            )
                                        )
                                    } catch {
                                        log.error("failed to retrieve message with key \(messageKvt.key)")
                                        continue
                                    }
                                }
                                
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
    
    func addMessages(
        _ messages: [MessageInput],
        inGroup groupId: String,
        completionHandler: @escaping (Result<[MessageOutputDTO], Error>) -> ()
    ) {
        self.addMessages(messages, anchorType: .group, anchorId: groupId, completionHandler: completionHandler)
    }
    
    func addMessages(
        _ messages: [MessageInput],
        inThread threadId: String,
        completionHandler: @escaping (Result<[MessageOutputDTO], Error>) -> ()
    ) {
        self.addMessages(messages, anchorType: .thread, anchorId: threadId, completionHandler: completionHandler)
    }
    
    private func addMessages(
        _ messages: [MessageInput],
        anchorType: InteractionAnchor,
        anchorId: String,
        completionHandler: @escaping (Result<[MessageOutputDTO], Error>) -> ()
    ) {
        let messagesStore: KBQueueStore
        do {
            messagesStore = try SHDBManager.sharedInstance.messageQueue()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        var result = [MessageOutputDTO]()
        var firstError: Error? = nil
        
        for message in messages {
            guard let interactionId = message.interactionId else {
                log.warning("can not save message to local store because without an interaction identifier from server")
                continue
            }
            
            let messageOutput = MessageOutputDTO(
                interactionId: interactionId,
                senderUserIdentifier: message.senderUserIdentifier!,
                inReplyToAssetGlobalIdentifier: message.inReplyToInteractionId,
                inReplyToInteractionId: message.inReplyToInteractionId,
                encryptedMessage: message.encryptedMessage,
                createdAt: message.createdAt!
            )
            
            do {
                var key = "\(anchorType)::\(anchorId)::\(interactionId)"
                if let interactionId = message.inReplyToInteractionId {
                    key += "::\(interactionId)"
                } else {
                    key += "::"
                }
                if let assetGid = message.inReplyToAssetGlobalIdentifier {
                    key += "::\(assetGid)"
                } else {
                    key += "::"
                }
                key += "::\(message.senderUserIdentifier!)"
                let value = DBSecureSerializableUserMessage(
                    interactionId: message.interactionId!,
                    senderUserIdentifier: message.senderUserIdentifier!,
                    inReplyToAssetGlobalIdentifier: message.inReplyToInteractionId,
                    inReplyToInteractionId: message.inReplyToInteractionId,
                    encryptedMessage: message.encryptedMessage,
                    createdAt: message.createdAt!
                )
                
                let data = try NSKeyedArchiver.archivedData(withRootObject: value, requiringSecureCoding: true)
                try messagesStore.insert(
                    data,
                    withIdentifier: key,
                    timestamp: message.createdAt!.iso8601withFractionalSeconds!
                )
                
                result.append(messageOutput)
            } catch {
                if messages.count == 1 {
                    completionHandler(.failure(error))
                }
                if firstError == nil {
                    firstError = error
                }
                switch anchorType {
                case .group:
                    log.error("failed to locally add message with id \(message.interactionId!) to group \(anchorId)")
                case .thread:
                    log.error("failed to locally add message with id \(message.interactionId!) to thread \(anchorId)")
                }
            }
            
            if result.isEmpty {
                completionHandler(.failure(firstError!))
            } else {
                completionHandler(.success(result))
            }
        }
    }
    
}
