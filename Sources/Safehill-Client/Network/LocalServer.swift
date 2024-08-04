import Foundation
import KnowledgeBase
import Contacts

public let SHDefaultDBTimeoutInMilliseconds = 15000 // 15 seconds

public enum SHLocalServerError: Error, LocalizedError {
    case failedToCreateFile
    
    public var errorDescription: String? {
        switch self {
        case .failedToCreateFile:
            "Failed to create asset file on disk"
        }
    }
}

struct LocalServer : SHServerAPI {
    
    let requestor: SHLocalUserProtocol
    
    ///
    /// Because fetching descriptors is happening frequently and it may be expensive
    /// cache the results
    /// 
    let assetDescriptorInMemoryCache = ThreadSafeCache<String, SHGenericAssetDescriptorClass>()
    
    static var dataFolderURL: URL {
        guard let baseUrl = FileManager.default.urls(
            for: .documentDirectory,
            in: .userDomainMask
        ).first
        else {
            fatalError("failed to initialize directory for encrypted data")
        }
        
        let encryptedDataURL: URL
        if #available(iOS 16.0, macOS 13.0, *) {
            encryptedDataURL = baseUrl.appending(components: "safehill", "encryptedData")
        } else {
            encryptedDataURL = baseUrl
                .appendingPathComponent("safehill")
                .appendingPathExtension("encryptedData")
        }
        
        let fileManager = FileManager.default
        if !fileManager.fileExists(atPath: encryptedDataURL.relativePath) {
            do {
                try fileManager.createDirectory(
                    at: encryptedDataURL,
                    withIntermediateDirectories: true
                )
            } catch {
                fatalError("failed to create directory for encrypted data. \(error.localizedDescription)")
            }
        }
        return encryptedDataURL
    }
    
    init(requestor: SHLocalUserProtocol) {
        self.requestor = requestor
        /// Ensure the data folder can be created
        let _ = Self.dataFolderURL
    }
    
    internal func createOrUpdateUser(identifier: UserIdentifier,
                                     name: String,
                                     publicKeyData: Data,
                                     publicSignatureData: Data,
                                     completionHandler: @escaping (Result<any SHServerUser, Error>) -> ()) {
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            completionHandler(.failure(KBError.databaseNotReady))
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
        
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            completionHandler(.failure(KBError.databaseNotReady))
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
                
                if let name {
                    value["name"] = name
                }
                if let phoneNumber {
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
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            completionHandler(.failure(KBError.databaseNotReady))
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
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            completionHandler(.failure(KBError.databaseNotReady))
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
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        do {
            try SHKGQuery.removeUsers(with: identifiers)
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
    
    func deleteAccount(name: String, password: String, completionHandler: @escaping (Result<Void, Error>) -> ()) {
        self.deleteAccount(completionHandler: completionHandler)
    }
    
    func deleteAllAssets(completionHandler: @escaping (Result<[String], Error>) -> ()) {
        guard let assetStore = SHDBManager.sharedInstance.assetStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        do {
            try SHKGQuery.deepClean()
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        assetStore.removeAll(completionHandler: completionHandler)
        self.assetDescriptorInMemoryCache.removeAll()
        
        let queuesToClear = BackgroundOperationQueue.OperationType.allCases
        for queueType in queuesToClear {
            do {
                let queue = try BackgroundOperationQueue.of(type: queueType)
                let _ = try queue.removeAll()
            } catch {
                log.error("failed to remove items from the \(queueType.identifier) queue")
                completionHandler(.failure(error))
                return
            }
        }
    }
    
    func deleteAccount(completionHandler: @escaping (Result<Void, Error>) -> ()) {
        
        self.assetDescriptorInMemoryCache.removeAll()
        ServerUserCache.shared.clear()
        
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        guard let reactionStore = SHDBManager.sharedInstance.reactionStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        guard let messagesQueue = SHDBManager.sharedInstance.messagesQueue else {
            completionHandler(.failure(KBError.databaseNotReady))
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
    
    func getUsers(
        withIdentifiers userIdentifiers: [UserIdentifier]?,
        completionHandler: @escaping (Result<[SHServerUser], Error>) -> ()
    ) {
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            completionHandler(.failure(KBError.databaseNotReady))
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
            let condition = KBGenericCondition(.contains, value: "::", negated: true)
            userStore.values(forKeysMatching: condition) { result in
                switch result {
                case .failure(let err):
                    completionHandler(.failure(err))
                case .success(let values):
                    callback(.success(values.compactMap({ $0 })))
                }
            }
        }
    }
    
    func getUsers(withHashedPhoneNumbers hashedPhoneNumbers: [String], completionHandler: @escaping (Result<[String: any SHServerUser], Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func searchUsers(query: String, completionHandler: @escaping (Result<[SHServerUser], Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func countUploaded(
        completionHandler: @escaping (Swift.Result<Int, Error>) -> ()
    ) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func authorizeUsers(
        with userPublicIdentifiers: [String],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func blockUsers(
        with userPublicIdentifiers: [String],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func pendingOrBlockedUsers(
        completionHandler: @escaping (Result<UserAuthorizationStatusDTO, Error>) -> ()
    ) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    @available(*, deprecated, renamed: "getAssetDescriptors(forAssetGlobalIdentifiers:filteringGroupIds:after:completionHandler:)", message: "Do not use the protocol method")
    internal func getAssetDescriptors(
        after: Date?,
        completionHandler: @escaping (Result<[any SHAssetDescriptor], Error>) -> ()
    ) {
        self.getAssetDescriptors(
            forAssetGlobalIdentifiers: [],
            after: after,
            useCache: false,
            completionHandler: completionHandler
        )
    }
    
    func getAssetDescriptors(forAssetGlobalIdentifiers: [GlobalIdentifier], filteringGroupIds: [String]?, after: Date?, completionHandler: @escaping (Result<[any SHAssetDescriptor], Error>) -> ()) {
        self.getAssetDescriptors(forAssetGlobalIdentifiers: forAssetGlobalIdentifiers, filteringGroupIds: filteringGroupIds, after: after, useCache: false, completionHandler: completionHandler)
    }
    
    func getAssetDescriptors(
        forAssetGlobalIdentifiers globalIdentifiers: [GlobalIdentifier],
        filteringGroupIds: [String]? = nil,
        after: Date? = nil,
        useCache: Bool,
        completionHandler: @escaping (Result<[any SHAssetDescriptor], Error>) -> ()
    ) {
        guard after == nil else {
            completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
            return
        }
        
        guard let assetStore = SHDBManager.sharedInstance.assetStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        var descriptors = [SHGenericAssetDescriptor]()
        var globalIdentifiersToFetch = Set(globalIdentifiers)
        
        if globalIdentifiers.isEmpty == false, useCache {
            
            for globalIdentifier in globalIdentifiers {
                if let cachedValue = assetDescriptorInMemoryCache.value(forKey: globalIdentifier) as? SHGenericAssetDescriptorClass {
                    descriptors.append(SHGenericAssetDescriptor.from(cachedValue))
                    globalIdentifiersToFetch.remove(globalIdentifier)
                }
            }
            
            guard globalIdentifiersToFetch.isEmpty == false else {
                completionHandler(.success(descriptors))
                return
            }
        }
        
        var senderInfoDict = [GlobalIdentifier: UserIdentifier]()
        var groupInfoByIdByAssetGid = [GlobalIdentifier: [String: SHAssetGroupInfo]]()
        var sharedWithUsersInGroupByAssetGid = [GlobalIdentifier: [UserIdentifier: String]]()
        
        ///
        /// Retrieve all information from the asset store for all assets and `.lowResolution` versions.
        /// **We can safely assume all versions are shared using the same group id, and will have same sender and receiver info*
        ///
        do {
            let senderCondition = KBGenericCondition(
                .beginsWith, value: "sender::"
            ).and(KBGenericCondition(
                .contains, value: "::low::"
            ))
            
            let senderKeys = try assetStore.keys(matching: senderCondition)
            
            for senderKey in senderKeys {
                let components = senderKey.components(separatedBy: "::")
                /// Components:
                /// 0) "sender"
                /// 1) sender user identifier
                /// 2) version quality
                /// 3) asset identifier
                
                if components.count == 4 {
                    let sharedByUserId = components[1]
                    let globalIdentifier = components[3]
                    senderInfoDict[globalIdentifier] = sharedByUserId
                } else {
                    log.error("invalid sender info key in DB: \(senderKey)")
                }
            }
            
            let receiverCondition = KBGenericCondition(
                .beginsWith, value: "receiver::"
            ).and(KBGenericCondition(
                .contains, value: "::low::") // Can safely assume all versions are shared using the same group id
            )
            
            let receiverKeysAndValues: KBKVPairs
            do {
                receiverKeysAndValues = try assetStore.dictionaryRepresentation(forKeysMatching: receiverCondition)
            } catch KBError.serializationError {
                log.critical("Serialization error when pulling asset recipient information from local DB. Descriptors will be fetched from server again and overwritten")
                do {
                    let _ = try assetStore.removeValues(forKeysMatching: receiverCondition)
                } catch {
                    log.error("failed to remove keys matching \(receiverCondition) from DB. \(error.localizedDescription)")
                }
                receiverKeysAndValues = [:]
            }
            
            for (key, value) in receiverKeysAndValues {
                guard let value = value as? [String: String] else {
                    log.error("invalid sharing information key found in DB: \(String(describing: value))")
                    continue
                }
                
                let components = key.components(separatedBy: "::")
                /// Components:
                /// 0) "receiver"
                /// 1) receiver user public identifier
                /// 2) version quality
                /// 3) asset identifier
                
                let assetGid: GlobalIdentifier
                
                if components.count == 4, let groupId = value["groupId"] {
                    assetGid = components[3]
                    if filteringGroupIds == nil || filteringGroupIds!.contains(groupId) {
                        let receiverUser = components[1]
                        
                        if sharedWithUsersInGroupByAssetGid[assetGid] == nil {
                            sharedWithUsersInGroupByAssetGid[assetGid] = [receiverUser: groupId]
                        } else {
                            sharedWithUsersInGroupByAssetGid[assetGid]![receiverUser] = groupId
                        }
                    }
                } else {
                    log.error("failed to retrieve sharing information. Invalid entry format: \(key) -> \(value)")
                    continue
                }
                
                if let groupId = value["groupId"] {
                    let groupName = value["groupName"]
                    let groupCreationDate = value["groupCreationDate"]
                    if filteringGroupIds == nil || filteringGroupIds!.contains(groupId) {
                        let groupInfo = SHGenericAssetGroupInfo(
                            name: groupName,
                            createdAt: groupCreationDate?.iso8601withFractionalSeconds
                        )
                        
                        if groupInfoByIdByAssetGid[assetGid] == nil {
                            groupInfoByIdByAssetGid[assetGid] = [groupId: groupInfo]
                        } else {
                            groupInfoByIdByAssetGid[assetGid]![groupId] = groupInfo
                        }
                    }
                }
            }
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        
        ///
        /// Retrieve all information from the asset store for all assets and matching versions.
        ///
        
        do {
            var versionUploadStateByIdentifierQuality = [GlobalIdentifier: [SHAssetQuality: SHAssetDescriptorUploadState]]()
            var localInfoByGlobalIdentifier = [GlobalIdentifier: (phAssetId: LocalIdentifier?, creationDate: Date?)]()
            
            var condition = KBGenericCondition(value: false)
            
            for quality in SHAssetQuality.all {
                condition = condition.or(KBGenericCondition(.beginsWith, value: "\(quality.rawValue)::"))
            }
            
            let keyValues: KBKVPairs
            do {
                keyValues = try assetStore.dictionaryRepresentation(forKeysMatching: condition)
            } catch KBError.serializationError {
                log.critical("Serialization error when reading from the assets DB. Descriptors will be fetched from server again and overwritten. Removing")
                do {
                    let _ = try assetStore.removeValues(forKeysMatching: condition)
                } catch {
                    log.error("failed to remove keys matching \(condition) from DB. \(error.localizedDescription)")
                }
                keyValues = [:]
            }
            
            for (k, v) in keyValues {
                guard let value = v as? [String: Any],
                      let phAssetIdentifier = value["applePhotosAssetIdentifier"] as? String?,
                      let creationDate = value["creationDate"] as? Date? else {
                    continue
                }
                
                let doProcessState = { (globalIdentifier: GlobalIdentifier, quality: SHAssetQuality) in
                    /// If caller requested all assets or the retrieved asset is not in the set to retrieve
                    guard globalIdentifiers.isEmpty || globalIdentifiersToFetch.contains(globalIdentifier) else {
                        return
                    }
                    
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
                
                /// If caller requested all assets or the retrieved asset is not in the set to retrieve
                guard globalIdentifiers.isEmpty || globalIdentifiersToFetch.contains(globalIdentifier) else {
                    continue
                }
                
                localInfoByGlobalIdentifier[globalIdentifier] = (
                    phAssetId: localInfoByGlobalIdentifier[globalIdentifier]?.phAssetId ?? phAssetIdentifier,
                    creationDate: localInfoByGlobalIdentifier[globalIdentifier]?.creationDate ?? creationDate
                )
            }
            
            var invalidGlobalIdentifiersInDB = Set<GlobalIdentifier>()
            
            for globalIdentifier in versionUploadStateByIdentifierQuality.keys {
                guard let sharedBy = senderInfoDict[globalIdentifier] else {
                    log.error("failed to retrieve sender information for asset \(globalIdentifier)")
                    invalidGlobalIdentifiersInDB.insert(globalIdentifier)
                    continue
                }
                
                guard let groupInfoById = groupInfoByIdByAssetGid[globalIdentifier],
                      let sharedWithUsersInGroup = sharedWithUsersInGroupByAssetGid[globalIdentifier]
                else {
                    log.error("failed to retrieve group information for asset \(globalIdentifier)")
                    invalidGlobalIdentifiersInDB.insert(globalIdentifier)
                    continue
                }
                
                if Set(sharedWithUsersInGroup.values).count != groupInfoById.count || groupInfoById.values.contains(where: { $0.createdAt == nil }) {
                    log.error("some group information (or the creation date of such groups) is missing. \(groupInfoById.map({ ($0.key, $0.value.name, $0.value.createdAt) }))")
                }
                
                if groupInfoById.isEmpty {
                    ///
                    /// If there is no info about any group, don't add the descriptor
                    /// Because the filtering on groups returned no results for this asset,
                    /// the asset will not be included.
                    ///
                    continue
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
                
                self.assetDescriptorInMemoryCache.set(
                    descriptor.serialized(),
                    forKey: globalIdentifier
                )
            }
            
            if invalidGlobalIdentifiersInDB.isEmpty == false {
                let _ = self.deleteAssets(withGlobalIdentifiers: Array(invalidGlobalIdentifiersInDB)) { res in
                    if case .failure(let error) = res {
                        log.error("failed to remove assets \(invalidGlobalIdentifiersInDB) from DB. \(error.localizedDescription)")
                    }
                }
            }
            
            completionHandler(.success(descriptors))
        } catch {
            completionHandler(.failure(error))
        }
    }
    
    func updateGroupIds(_ groupInfoById: [String: SHAssetGroupInfo],
                        completionHandler: @escaping (Result<Void, Error>) -> Void) {
        
        guard let assetStore = SHDBManager.sharedInstance.assetStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        var keysToUpdate = [String: [String: Any?]]()
        
        do {
            /// 
            /// Get all the keys to determine which values need update
            ///
            let versionsDetailsDict = try assetStore.dictionaryRepresentation(
                forKeysMatching: KBGenericCondition(
                    .beginsWith, value: "receiver::"
                )
            )
            guard let versionsDetailsDict = versionsDetailsDict as? [String: [String: Any?]] else {
                throw KBError.unexpectedData(versionsDetailsDict)
            }
            
            for (key, versionDetails) in versionsDetailsDict {
                ///
                /// If the groupId matches, then collect the key and the updated value
                ///
                guard let groupId = versionDetails["groupId"] as? String,
                      let update = groupInfoById[groupId]
                else {
                    continue
                }
                
                var updatedValue = versionDetails
                updatedValue["groupName"] = update.name
                updatedValue["groupCreationDate"] = update.createdAt?.iso8601withFractionalSeconds
                
                keysToUpdate[key] = updatedValue
            }
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        let writeBatch = assetStore.writeBatch()
        
        for (key, update) in keysToUpdate {
            writeBatch.set(value: update, for: key)
        }
        
        writeBatch.write(completionHandler: completionHandler)
    }
    
    func removeGroupIds(_ groupIds: [String],
                        completionHandler: @escaping (Result<Void, Error>) -> Void) {
        guard let assetStore = SHDBManager.sharedInstance.assetStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        var keysToRemove = [String]()
        
        do {
            let versionsDetailsDict = try assetStore.dictionaryRepresentation(
                forKeysMatching: KBGenericCondition(
                    .beginsWith, value: "receiver::"
                )
            )
            guard let versionsDetailsDict = versionsDetailsDict as? [String: [String: Any?]] else {
                throw KBError.unexpectedData(versionsDetailsDict)
            }
            
            for (key, versionDetails) in versionsDetailsDict {
                guard let groupId = versionDetails["groupId"] as? String,
                      groupIds.contains(groupId) == false
                else {
                    continue
                }
                      
                keysToRemove.append(key)
            }
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        assetStore.removeValues(for: keysToRemove, completionHandler: completionHandler)
    }
    
    func getAssets(withGlobalIdentifiers assetIdentifiers: [GlobalIdentifier],
                   versions: [SHAssetQuality]? = nil,
                   completionHandler: @escaping (Result<[GlobalIdentifier: any SHEncryptedAsset], Error>) -> ()) {
        guard assetIdentifiers.count > 0 else {
            completionHandler(.success([:]))
            return
        }
        
        guard let assetStore = SHDBManager.sharedInstance.assetStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        self.getAssetsChunks(
            from: assetStore,
            assetIdentifiersChunks: assetIdentifiers.chunked(into: 20),
            versions: versions,
            initialValue: [:],
            completionHandler: completionHandler
        )
    }
    
    private func getAssetsChunks(
        from assetStore: KBKVStore,
        assetIdentifiersChunks: [[GlobalIdentifier]],
        versions: [SHAssetQuality]?,
        index: Int = 0,
        initialValue partialResult: [GlobalIdentifier: any SHEncryptedAsset],
        completionHandler: @escaping (Result<[GlobalIdentifier: any SHEncryptedAsset], Error>) -> Void
    ) {
        guard index < assetIdentifiersChunks.count else {
            completionHandler(.success(partialResult))
            return
        }
        
        let assetIdentifiersChunk = assetIdentifiersChunks[index]
        
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
        
        var newResult = partialResult
        
        assetStore.dictionaryRepresentation(forKeysMatching: prefixCondition.and(assetCondition)) {
            (result: Result) in
            switch result {
            case .success(let keyValues):
                guard let keyValues = keyValues as? [String: [String: Any]] else {
                    completionHandler(.failure(KBError.unexpectedData(keyValues)))
                    return
                }
                
                do {
                    newResult.merge(
                        try SHGenericEncryptedAsset.fromDicts(keyValues),
                        uniquingKeysWith: { (_, b) in return b }
                    )
                    
                    self.getAssetsChunks(
                        from: assetStore,
                        assetIdentifiersChunks: assetIdentifiersChunks,
                        versions: versions,
                        index: index+1,
                        initialValue: newResult,
                        completionHandler: completionHandler
                    )
                    
                } catch {
                    completionHandler(.failure(KBError.unexpectedData(keyValues)))
                    return
                }
            case .failure(let error):
                completionHandler(.failure(error))
                return
            }
        }
    }
    
    func create(assets: [any SHEncryptedAsset],
                groupId: String,
                filterVersions: [SHAssetQuality]?,
                force: Bool = true,
                completionHandler: @escaping (Result<[SHServerAsset], Error>) -> ()) {
        
        var assets = assets
        if let filterVersions, filterVersions.isEmpty == false {
            assets = assets.map({
                var newVersions = $0.encryptedVersions
                for (versionKey, versionValue) in $0.encryptedVersions {
                    if filterVersions.contains(versionKey) {
                        newVersions[versionKey] = versionValue
                    }
                }
                return SHGenericEncryptedAsset(
                    globalIdentifier: $0.globalIdentifier,
                    localIdentifier: $0.localIdentifier,
                    creationDate: $0.creationDate,
                    encryptedVersions: newVersions
                )
            })
        }
        
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
                    groupInfoById: [
                        groupId: SHGenericAssetGroupInfo(name: nil, createdAt: Date())
                    ]
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
    
    internal func createAssetDataFile(
        globalIdentifier: GlobalIdentifier,
        quality: SHAssetQuality,
        content encryptedData: Data
    ) throws -> URL {
        let assetFolderURL: URL, versionDataURL: URL
        
        if #available(iOS 16.0, macOS 13.0, *) {
            assetFolderURL = Self.dataFolderURL
                .appending(path: globalIdentifier)
            versionDataURL = assetFolderURL
                .appending(path: quality.rawValue)
        } else {
            assetFolderURL = Self.dataFolderURL
                .appendingPathComponent(globalIdentifier)
            versionDataURL = assetFolderURL
                .appendingPathComponent(quality.rawValue)
        }
        
        let fileManager = FileManager.default
        
        if fileManager.fileExists(atPath: versionDataURL.relativePath) {
            log.warning("a file exists at \(versionDataURL.absoluteString). Overriding")
            try? fileManager.removeItem(at: versionDataURL)
        }
        
        do {
            try fileManager.createDirectory(at: assetFolderURL, withIntermediateDirectories: true)
        } catch {
            log.error("failed to create directory at \(assetFolderURL.absoluteString). \(error.localizedDescription)")
            throw error
        }
        
        let created = fileManager.createFile(
            atPath: versionDataURL.relativePath,
            contents: encryptedData
        )
        guard created else {
            log.error("failed to create file at \(versionDataURL.absoluteString)")
            throw SHLocalServerError.failedToCreateFile
        }
        
        return versionDataURL
    }
    
    func create(assets: [any SHEncryptedAsset],
                descriptorsByGlobalIdentifier: [GlobalIdentifier: any SHAssetDescriptor],
                uploadState: SHAssetDescriptorUploadState,
                completionHandler: @escaping (Result<[SHServerAsset], Error>) -> ()) {
        guard assets.isEmpty == false else {
            return completionHandler(.success([]))
        }
        
        guard let assetStore = SHDBManager.sharedInstance.assetStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        let writeBatch = assetStore.writeBatch()
        
        for asset in assets {
            guard let descriptor = descriptorsByGlobalIdentifier[asset.globalIdentifier] else {
                log.error("no descriptor provided for asset to create with global identifier \(asset.globalIdentifier)")
                continue
            }
            
            guard let senderUploadGroupId = descriptor.sharingInfo.sharedWithUserIdentifiersInGroup[descriptor.sharingInfo.sharedByUserIdentifier] else {
                log.error("No groupId specified in descriptor for asset to create for sender user: userId=\(descriptor.sharingInfo.sharedByUserIdentifier)")
                continue
            }
            let senderGroupIdInfo = descriptor.sharingInfo.groupInfoById[senderUploadGroupId]
            let senderGroupCreatedAt = senderGroupIdInfo?.createdAt
            
            if senderGroupCreatedAt == nil {
                log.error("No groupId info or creation date set for sender group id \(senderUploadGroupId)")
            }
            
            for encryptedVersion in asset.encryptedVersions.values {
                
                if asset.encryptedVersions.count == 1,
                   asset.encryptedVersions.first?.key == .lowResolution {
                    
                    ///
                    /// Every time a `.lowResolution` (and only  such resolution) is created
                    /// remove the `.midResolution` and the `.hiResolution`
                    /// from the cache
                    ///
                    
                    for quality in [SHAssetQuality.midResolution, SHAssetQuality.hiResolution] {
                        writeBatch.set(value: nil, for: "\(quality)::" + asset.globalIdentifier)
                        writeBatch.set(value: nil, for: "data::" + "\(quality)::" + asset.globalIdentifier)
                        writeBatch.set(value: nil, for: [
                            "sender",
                            descriptor.sharingInfo.sharedByUserIdentifier,
                            quality.rawValue,
                            asset.globalIdentifier
                        ].joined(separator: "::"))
                        
                        for (recipientUserId, _) in descriptor.sharingInfo.sharedWithUserIdentifiersInGroup {
                            writeBatch.set(value: nil, for: [
                                "receiver",
                                recipientUserId,
                                quality.rawValue,
                                asset.globalIdentifier
                            ].joined(separator: "::"))
                        }
                    }
                }
                else if encryptedVersion.quality == .midResolution,
                   asset.encryptedVersions.keys.contains(.hiResolution) {
                    
                    ///
                    /// `.midResolution` is a surrogate for high-resolution when sharing assets to speed up the delivery.
                    /// When adding a `.hiResolution` version locally, remove the `.midResolution`.
                    /// If both a `.midResolution` and a `.hiResolution` are in the list of versions to create,
                    /// only store the `.hiResolution`.
                    ///
                    
                    continue
                }
                else if encryptedVersion.quality == .hiResolution {
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
                
                let versionDataURL: URL
                
                do {
                    versionDataURL = try self.createAssetDataFile(
                        globalIdentifier: asset.globalIdentifier,
                        quality: encryptedVersion.quality,
                        content: encryptedVersion.encryptedData
                    )
                } catch {
                    continue
                }
                    
                let dataPath: [String: Any?] = [
                    "assetIdentifier": asset.globalIdentifier,
                    "encryptedDataPath": versionDataURL.absoluteString
                ]
                
                writeBatch.set(value: versionMetadata, for: "\(encryptedVersion.quality.rawValue)::" + asset.globalIdentifier)
                writeBatch.set(value: dataPath, for: "data::" + "\(encryptedVersion.quality.rawValue)::" + asset.globalIdentifier)
                writeBatch.set(value: true,
                               for: [
                                "sender",
                                descriptor.sharingInfo.sharedByUserIdentifier,
                                encryptedVersion.quality.rawValue,
                                asset.globalIdentifier
                               ].joined(separator: "::")
                )
                
                var sharedVersionDetails: [String: String] = [
                    "senderEncryptedSecret": encryptedVersion.encryptedSecret.base64EncodedString(),
                    "ephemeralPublicKey": encryptedVersion.publicKeyData.base64EncodedString(),
                    "publicSignature": encryptedVersion.publicSignatureData.base64EncodedString(),
                    "groupId": senderUploadGroupId
                ]
                if let groupName = senderGroupIdInfo?.name {
                    sharedVersionDetails["groupName"] = groupName
                }
                if let groupCreationDate = senderGroupIdInfo?.createdAt?.iso8601withFractionalSeconds {
                    sharedVersionDetails["groupCreationDate"] = groupCreationDate
                }
                
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
                    
                    let recipientGroupInfo = descriptor.sharingInfo.groupInfoById[recipientGroupId]
                    let recipientShareDate = recipientGroupInfo?.createdAt
                    
                    if recipientShareDate == nil {
                        log.critical("No groupId info or creation date set for recipient \(recipientUserId) group id \(recipientGroupId)")
                    }
                    
                    var receiverDetails: [String: String] = [
                        "groupId": recipientGroupId
                    ]
                    if let groupName = recipientGroupInfo?.name {
                        receiverDetails["groupName"] = groupName
                    }
                    if let groupCreationDate = recipientGroupInfo?.createdAt?.iso8601withFractionalSeconds {
                        receiverDetails["groupCreationDate"] = groupCreationDate
                    }
                    
                    writeBatch.set(
                        value: receiverDetails,
                        for: [
                            "receiver",
                            recipientUserId,
                            encryptedVersion.quality.rawValue,
                            asset.globalIdentifier
                        ].joined(separator: "::"))
                }
            }
        }
        
        do {
            try SHKGQuery.ingest(
                Array(descriptorsByGlobalIdentifier.values),
                receiverUserId: self.requestor.identifier
            )
        } catch {
            completionHandler(.failure(error))
            return
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
                    
                    self.assetDescriptorInMemoryCache.set(
                        descriptor.serialized(),
                        forKey: asset.globalIdentifier
                    )
                }
                
                completionHandler(.success(serverAssets))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    func addAssetRecipients(basedOn userIdsToAddToAssetGids: [GlobalIdentifier: ShareSenderReceivers],
                            versions: [SHAssetQuality]? = nil,
                            completionHandler: @escaping (Result<Void, Error>) -> ()) {
        
        guard let assetStore = SHDBManager.sharedInstance.assetStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        do {
            try SHKGQuery.ingestShares(userIdsToAddToAssetGids)
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        let versions = versions ?? SHAssetQuality.all
        
        let writeBatch = assetStore.writeBatch()
        
        for (globalIdentifier, shareDiff) in userIdsToAddToAssetGids {
            for (recipientUserId, groupId) in shareDiff.groupIdByRecipientId {
                guard let groupInfo = shareDiff.groupInfoById[groupId] else {
                    log.critical("group information missing for group \(groupId) when calling addAssetRecipients(basedOn:versions:completionHandler:)")
                    continue
                }
                for version in versions {
                    writeBatch.set(
                        value: [
                            "groupId": groupId,
                            "groupName": groupInfo.name,
                            "groupCreationDate": groupInfo.createdAt?.iso8601withFractionalSeconds
                        ],
                        for: [
                            "receiver",
                            recipientUserId,
                            version.rawValue,
                            globalIdentifier
                        ].joined(separator: "::")
                    )
                }
            }
        }
        
        writeBatch.write(completionHandler: completionHandler)
    }
    
    func removeAssetRecipients(basedOn userIdsToRemoveFromAssetGid: [GlobalIdentifier: ShareSenderReceivers],
                               versions: [SHAssetQuality]? = nil,
                               completionHandler: @escaping (Result<Void, Error>) -> ()) {
        guard let assetStore = SHDBManager.sharedInstance.assetStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        do {
            try SHKGQuery.removeSharingInformation(basedOn: userIdsToRemoveFromAssetGid)
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        ///
        /// Only after the Graph is updated, remove the recipients from the DB
        /// This ensures that if the graph update fails is attempted again (as the descriptors from local haven't been updated yet)
        ///
        let versions = versions ?? SHAssetQuality.all
        
        let writeBatch = assetStore.writeBatch()
        
        for (globalIdentifier, shareDiff) in userIdsToRemoveFromAssetGid {
            for (recipientUserId, _) in shareDiff.groupIdByRecipientId {
                for version in versions {
                    writeBatch.set(value: nil, for: [
                        "receiver",
                        recipientUserId,
                        version.rawValue,
                        globalIdentifier
                    ].joined(separator: "::"))
                }
            }
        }
        
        writeBatch.write(completionHandler: completionHandler)
    }
    
    func upload(
        serverAsset: SHServerAsset,
        asset: any SHEncryptedAsset,
        filterVersions: [SHAssetQuality]?
    ) async throws {
        
        for encryptedAssetVersion in asset.encryptedVersions.values {
            guard filterVersions == nil || filterVersions!.contains(encryptedAssetVersion.quality) else {
                continue
            }
            
            try await withUnsafeThrowingContinuation { continuation in
                self.markAsset(
                    with: asset.globalIdentifier,
                    quality: encryptedAssetVersion.quality,
                    as: .completed
                ) { result in
                    switch result {
                    case .success:
                        continuation.resume()
                    case .failure(let error):
                        continuation.resume(throwing: error)
                    }
                }
            }   
        }
    }
    
    func markAsset(with assetGlobalIdentifier: GlobalIdentifier,
                   quality: SHAssetQuality,
                   as state: SHAssetDescriptorUploadState,
                   completionHandler: @escaping (Result<Void, Error>) -> ()) {
        guard let assetStore = SHDBManager.sharedInstance.assetStore else {
            completionHandler(.failure(KBError.databaseNotReady))
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
               isPhotoMessage: Bool = false,
               suppressNotification: Bool = true,
               completionHandler: @escaping (Result<Void, Error>) -> ()) {
        guard let assetStore = SHDBManager.sharedInstance.assetStore else {
            completionHandler(.failure(KBError.databaseNotReady))
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
        guard let assetStore = SHDBManager.sharedInstance.assetStore else {
            completionHandler(.failure(KBError.databaseNotReady))
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
        guard let assetStore = SHDBManager.sharedInstance.assetStore else {
            completionHandler(.failure(KBError.databaseNotReady))
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
        guard let assetStore = SHDBManager.sharedInstance.assetStore else {
            completionHandler(.failure(KBError.databaseNotReady))
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
        
        globalIdentifiers.forEach {
            globalIdentifier in
            self.assetDescriptorInMemoryCache.removeValue(forKey: globalIdentifier)
        }
        
        guard let assetStore = SHDBManager.sharedInstance.assetStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        do {
            try SHKGQuery.removeAssets(with: globalIdentifiers)
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
            
            usleep(useconds_t(10 * 1000)) // sleep 10ms
        }
        
        group.notify(queue: .global()) {
            guard err == nil else {
                completionHandler(.failure(err!))
                return
            }
            
            completionHandler(.success(Array(removedGlobalIdentifiers)))
        }
    }
    
    @available(*, deprecated, renamed: "createOrUpdateThread(serverThread:completionHandler:)", message: "Do not use the protocol method when storing a thread locally. Information from server should be provided.")
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
        guard serverThread.encryptionDetails.recipientUserIdentifier == self.requestor.identifier
        else {
            completionHandler(.failure(SHHTTPError.ClientError.badRequest("encryption details don't match the requestor")))
            return
        }
        
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        let writeBatch = userStore.writeBatch()
        
        writeBatch.set(value: serverThread.membersPublicIdentifier, for: "\(SHInteractionAnchor.thread.rawValue)::\(serverThread.threadId)::membersPublicIdentifiers")
        
        writeBatch.set(value: serverThread.encryptionDetails.encryptedSecret, for: "\(SHInteractionAnchor.thread.rawValue)::\(serverThread.threadId)::encryptedSecret")
        writeBatch.set(value: serverThread.encryptionDetails.ephemeralPublicKey, for: "\(SHInteractionAnchor.thread.rawValue)::\(serverThread.threadId)::ephemeralPublicKey")
        writeBatch.set(value: serverThread.encryptionDetails.secretPublicSignature, for: "\(SHInteractionAnchor.thread.rawValue)::\(serverThread.threadId)::secretPublicSignature")
        writeBatch.set(value: serverThread.encryptionDetails.senderPublicSignature, for: "\(SHInteractionAnchor.thread.rawValue)::\(serverThread.threadId)::senderPublicSignature")
        
        writeBatch.set(value: serverThread.name, for: "\(SHInteractionAnchor.thread.rawValue)::\(serverThread.threadId)::name")
        writeBatch.set(value: serverThread.creatorPublicIdentifier, for: "\(SHInteractionAnchor.thread.rawValue)::\(serverThread.threadId)::creatorPublicIdentifier")
        writeBatch.set(value: serverThread.createdAt.iso8601withFractionalSeconds?.timeIntervalSince1970, for: "\(SHInteractionAnchor.thread.rawValue)::\(serverThread.threadId)::createdAt")
        writeBatch.set(value: serverThread.lastUpdatedAt?.iso8601withFractionalSeconds?.timeIntervalSince1970, for: "\(SHInteractionAnchor.thread.rawValue)::\(serverThread.threadId)::lastUpdatedAt")
        
        writeBatch.write { result in
            switch result {
            case .failure(let error):
                completionHandler(.failure(error))
            case .success:
                completionHandler(.success(serverThread))
            }
        }
    }
    
    func updateLastUpdatedAt(
        with remoteThreads: [ConversationThreadOutputDTO],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        let writeBatch = userStore.writeBatch()
        
        for remoteThread in remoteThreads {
            writeBatch.set(value: remoteThread.lastUpdatedAt?.iso8601withFractionalSeconds?.timeIntervalSince1970, for: "\(SHInteractionAnchor.thread.rawValue)::\(remoteThread.threadId)::lastUpdatedAt")
        }
        
        writeBatch.write { result in
            switch result {
            case .failure(let error):
                completionHandler(.failure(error))
            case .success:
                completionHandler(.success(()))
            }
        }
    }
    
    func listThreads(
        completionHandler: @escaping (Result<[ConversationThreadOutputDTO], Error>) -> ()
    ) {
        self.listThreads(withIdentifiers: nil, completionHandler: completionHandler)
    }
    
    func listThreads(
        withIdentifiers: [String]?,
        completionHandler: @escaping (Result<[ConversationThreadOutputDTO], Error>) -> ()
    ) {
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        var condition: KBGenericCondition
        if let withIdentifiers {
            condition = KBGenericCondition(value: false)
            for identifier in withIdentifiers {
                condition = condition.or(
                    KBGenericCondition(
                        .beginsWith,
                        value: "\(SHInteractionAnchor.thread.rawValue)::\(identifier)"
                    )
                )
            }
        } else {
            condition = KBGenericCondition(
                .beginsWith, 
                value: "\(SHInteractionAnchor.thread.rawValue)::"
            )
        }
        
        condition = condition.and(KBGenericCondition(
            .contains,
            value: "::assets::",
            negated: true
        ))
        
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
            var creatorPublicId: String? = nil
            var membersPublicIdentifiers: [String]? = nil
            var createdAt: Date? = nil
            var lastUpdatedAt: Date? = nil
            var encryptedSecret: String? = nil
            var ephemeralPublicKey: String? = nil
            var secretPublicSignature: String? = nil
            var senderPublicSignature: String? = nil
            
            switch components[2] {
            case "creatorPublicIdentifier":
                creatorPublicId = value as? String
            case "membersPublicIdentifiers":
                membersPublicIdentifiers = value as? [String]
            case "encryptedSecret":
                encryptedSecret = value as? String
            case "ephemeralPublicKey":
                ephemeralPublicKey = value as? String
            case "secretPublicSignature":
                secretPublicSignature = value as? String
            case "senderPublicSignature":
                senderPublicSignature = value as? String
            case "name":
                name = value as? String
            case "createdAt":
                if let createdAtInterval = value as? Double {
                    createdAt = Date(timeIntervalSince1970: createdAtInterval)
                }
            case "lastUpdatedAt":
                if let lastUpdatedInterval = value as? Double {
                    lastUpdatedAt = Date(timeIntervalSince1970: lastUpdatedInterval)
                }
            default:
                break
            }
            
            /// From the partial result …
            var result = partialResult
            
            /// … update the field corresponding to the KV pair just processed
            /// in the existing conversation thread, or create a new one with the empty value
            result[threadId] = ConversationThreadOutputDTO(
                threadId: threadId,
                name: name ?? result[threadId]?.name,
                creatorPublicIdentifier: creatorPublicId ?? result[threadId]?.creatorPublicIdentifier,
                membersPublicIdentifier: membersPublicIdentifiers ?? result[threadId]?.membersPublicIdentifier ?? [],
                createdAt: createdAt?.iso8601withFractionalSeconds ?? result[threadId]?.createdAt ?? Date().iso8601withFractionalSeconds,
                lastUpdatedAt: lastUpdatedAt?.iso8601withFractionalSeconds ?? result[threadId]?.lastUpdatedAt,
                encryptionDetails: RecipientEncryptionDetailsDTO(
                    recipientUserIdentifier: self.requestor.identifier,
                    ephemeralPublicKey: ephemeralPublicKey ?? result[threadId]?.encryptionDetails.ephemeralPublicKey ?? "",
                    encryptedSecret: encryptedSecret ?? result[threadId]?.encryptionDetails.encryptedSecret ?? "",
                    secretPublicSignature: secretPublicSignature ?? result[threadId]?.encryptionDetails.secretPublicSignature ?? "",
                    senderPublicSignature: senderPublicSignature ?? result[threadId]?.encryptionDetails.senderPublicSignature ?? ""
                )
            )
            
            return result
        })
            .values
        
        var filteredList = [ConversationThreadOutputDTO]()
        for element in list {
            if (
                element.membersPublicIdentifier.isEmpty
                || element.encryptionDetails.ephemeralPublicKey.isEmpty
                || element.encryptionDetails.encryptedSecret.isEmpty
                || element.encryptionDetails.secretPublicSignature.isEmpty
                || element.encryptionDetails.senderPublicSignature.isEmpty
            ) {
                // Do not append threads with missing information
            } else {
                filteredList.append(element)
            }
        }
        
        completionHandler(.success(filteredList))
    }
    
    func setGroupEncryptionDetails(
        groupId: String,
        recipientsEncryptionDetails: [RecipientEncryptionDetailsDTO],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        guard let selfEncryptionDetails = recipientsEncryptionDetails.first(where: { $0.recipientUserIdentifier == self.requestor.identifier })
        else {
            completionHandler(.failure(SHHTTPError.ClientError.badRequest("encryption details don't match the requestor")))
            return
        }
        
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        let writeBatch = userStore.writeBatch()

        writeBatch.set(value: selfEncryptionDetails.encryptedSecret, for: "\(SHInteractionAnchor.group.rawValue)::\(groupId)::encryptedSecret")
        writeBatch.set(value: selfEncryptionDetails.ephemeralPublicKey, for: "\(SHInteractionAnchor.group.rawValue)::\(groupId)::ephemeralPublicKey")
        writeBatch.set(value: selfEncryptionDetails.secretPublicSignature, for: "\(SHInteractionAnchor.group.rawValue)::\(groupId)::secretPublicSignature")
        writeBatch.set(value: selfEncryptionDetails.senderPublicSignature, for: "\(SHInteractionAnchor.group.rawValue)::\(groupId)::senderPublicSignature")
        
        writeBatch.write(completionHandler: completionHandler)
    }
    
    func deleteGroup(
        groupId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        self.delete(anchor: .group, anchorId: groupId, completionHandler: completionHandler)
    }
    
    private func delete(
        anchor: SHInteractionAnchor,
        anchorId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        guard let reactionStore = SHDBManager.sharedInstance.reactionStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        guard let messagesQueue = SHDBManager.sharedInstance.messagesQueue else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        do {
            let condition = KBGenericCondition(.beginsWith, value: "\(anchor.rawValue)::\(anchorId)::")
            let _ = try userStore.removeValues(forKeysMatching: condition)
            let _ = try reactionStore.removeValues(forKeysMatching: condition)
            let _ = try messagesQueue.removeValues(forKeysMatching: condition)
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
        self.listThreads(withIdentifiers: [threadId]) { result in
            switch result {
            case .success(let threads):
                completionHandler(.success(threads.first))
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    func getThread(
        withUsers users: [any SHServerUser],
        completionHandler: @escaping (Result<ConversationThreadOutputDTO?, Error>) -> ()
    ) {
        self.listThreads { result in
            switch result {
            case .failure(let error):
                completionHandler(.failure(error))
            case .success(let threads):
                let userIdsToMatch = Set(users.map({ $0.identifier }))
                for thread in threads {
                    if Set(thread.membersPublicIdentifier) == userIdsToMatch {
                        completionHandler(.success(thread))
                        return
                    }
                }
                completionHandler(.success(nil))
            }
        }
    }
    
    private func getGroupId(for userIdentifiers: [UserIdentifier], in descriptor: any SHAssetDescriptor) -> String? {
        var groupId: String? = nil
        for userIdentifier in userIdentifiers {
            if let groupForUser = descriptor.sharingInfo.sharedWithUserIdentifiersInGroup[userIdentifier] {
                if groupId == nil {
                    groupId = groupForUser
                } else if groupId != groupForUser {
                    ///
                    /// This asset should not be displayed in this thread
                    /// cause it was shared with the thread's users via a different group id.
                    ///
                    return nil
                }
            }
        }
        return groupId
    }
    
    internal func cache(
        _ threadAssets: ConversationThreadAssetsDTO,
        in threadId: String
    ) async throws {
        try await withUnsafeThrowingContinuation { continuation in
            
            self.getThread(withId: threadId) { threadResult in
                switch threadResult {
                case .success(let serverThread):
                    if let serverThread {
                        do {
                            try SHKGQuery.ingest(
                                threadAssets,
                                in: serverThread
                            )
                            continuation.resume()
                        } catch {
                            continuation.resume(throwing: error)
                        }
                    } else {
                        log.warning("failed to cache assets in non-existing local thread \(threadId)")
                        continuation.resume()
                    }
                case .failure(let error):
                    continuation.resume(throwing: error)
                }
            }
        }
    }
    
    func getAssets(
        inThread threadId: String,
        completionHandler: @escaping (Result<ConversationThreadAssetsDTO, Error>) -> ()
    ) {
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        do {
            ///
            /// Get the photo messages in this thread,
            /// previously synced by the method `LocalServer::cache(_:in)`
            ///
            let photoMessages = try userStore
                .values(
                    forKeysMatching: KBGenericCondition(
                        .beginsWith,
                        value: "\(SHInteractionAnchor.thread.rawValue)::\(threadId)::assets::photoMessage"
                    )
                )
                .compactMap { (value: Any) -> ConversationThreadAssetDTO? in
                    guard let data = value as? Data else {
                        log.critical("unexpected non-data photo message in thread \(threadId)")
                        return nil
                    }
                    guard let photoMessage = try? ConversationThreadAssetClass.fromData(data) else {
                        log.critical("failed to decode photo message in thread \(threadId)")
                        return nil
                    }
                    return photoMessage.toDTO()
                }
            
            ///
            /// Retrieve all assets shared with the people in this thread
            /// (regardless if they are photo messages)
            /// then filter out the photo messages
            ///
            let otherAssets = try userStore
                .values(
                    forKeysMatching: KBGenericCondition(
                        .beginsWith,
                        value: "\(SHInteractionAnchor.thread.rawValue)::\(threadId)::assets::nonPhotoMessage"
                    )
                )
                .compactMap { (value: Any) -> UsersGroupAssetDTO? in
                    guard let data = value as? Data else {
                        log.critical("unexpected non-data non-photo-message in thread \(threadId)")
                        return nil
                    }
                    guard let otherAsset = try? UsersGroupAssetClass.fromData(data) else {
                        log.critical("failed to decode non-photo-message in thread \(threadId)")
                        return nil
                    }
                    return otherAsset.toDTO()
                }
            
            let result = ConversationThreadAssetsDTO(
                photoMessages: photoMessages,
                otherAssets: otherAssets
            )
            completionHandler(.success(result))
            
        } catch {
            completionHandler(.failure(error))
        }
    }
    
    private func retrieveUserEncryptionDetails(
        anchorType: SHInteractionAnchor,
        anchorId: String,
        completionHandler: @escaping (Result<RecipientEncryptionDetailsDTO?, Error>) -> ()
    ) {
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        let keysToRetrieve = [
            "\(anchorType.rawValue)::\(anchorId)::encryptedSecret",
            "\(anchorType.rawValue)::\(anchorId)::ephemeralPublicKey",
            "\(anchorType.rawValue)::\(anchorId)::secretPublicSignature",
            "\(anchorType.rawValue)::\(anchorId)::senderPublicSignature"
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
                
                let recipientUserIdentifier = requestor.identifier
                var ephemeralPublicKey = ""
                var encryptedSecret = ""
                var secretPublicSignature = ""
                var senderPublicSignature = ""
                
                for (k, v) in keyValues {
                    switch k {
                    case let str where str.contains("encryptedSecret"):
                        encryptedSecret = v as? String ?? encryptedSecret
                    case let str where str.contains("ephemeralPublicKey"):
                        ephemeralPublicKey = v as? String ?? ephemeralPublicKey
                    case let str where str.contains("secretPublicSignature"):
                        secretPublicSignature = v as? String ?? secretPublicSignature
                    case let str where str.contains("senderPublicSignature"):
                        senderPublicSignature = v as? String ?? senderPublicSignature
                    default:
                        break
                    }
                }
                guard !encryptedSecret.isEmpty,
                      !ephemeralPublicKey.isEmpty,
                      !secretPublicSignature.isEmpty,
                      !senderPublicSignature.isEmpty
                else {
                    completionHandler(.failure(KBError.unexpectedData(keyValues)))
                    return
                }
                let encryptionDetails = RecipientEncryptionDetailsDTO(
                    recipientUserIdentifier: recipientUserIdentifier,
                    ephemeralPublicKey: ephemeralPublicKey,
                    encryptedSecret: encryptedSecret,
                    secretPublicSignature: secretPublicSignature,
                    senderPublicSignature: senderPublicSignature
                )
                completionHandler(.success(encryptionDetails))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    func deleteThread(withId threadId: String, completionHandler: @escaping (Result<Void, Error>) -> ()) {
        self.delete(anchor: .thread, anchorId: threadId, completionHandler: completionHandler)
    }
    
    func topLevelInteractionsSummary(completionHandler: @escaping (Result<InteractionsSummaryDTO, Error>) -> ()) {
        self.topLevelThreadsInteractionsSummary { threadsResult in
            switch threadsResult {
            case .failure(let error):
                completionHandler(.failure(error))
            case .success(let threadsSummary):
                self.topLevelGroupsInteractionsSummary { groupsResult in
                    switch groupsResult {
                    case .failure(let error):
                        completionHandler(.failure(error))
                    case .success(let groupsSummary):
                        
                        completionHandler(.success(InteractionsSummaryDTO(
                            summaryByThreadId: threadsSummary,
                            summaryByGroupId: groupsSummary
                        )))
                    }
                }
            }
        }
    }
    
    func topLevelThreadsInteractionsSummary(completionHandler: @escaping (Result<[String: InteractionsThreadSummaryDTO], Error>) -> ()) {
        guard let messagesQueue = SHDBManager.sharedInstance.messagesQueue else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        ///
        /// List all threads
        ///
        self.listThreads { result in
            switch result {
            case .failure(let error):
                completionHandler(.failure(error))
            case .success(let threads):
                
                var threadsById = [String: ConversationThreadOutputDTO]()
                var lastMessageByThreadId = [String: MessageOutputDTO]()
                var interactionIdsByThreadId = [String: Set<String>]()
                
                let dispatchGroup = DispatchGroup()
                
                ///
                /// For each thread run 2 queries:
                /// 1. One to retrieve the last message (encrypted)
                /// 2. One to retrieve the number of messages in the thread
                ///
                for threadsChunk in threads.chunked(into: 10) {
                    for thread in threadsChunk {
                        let threadId = thread.threadId
                        threadsById[threadId] = thread
                        
                        dispatchGroup.enter()
                        
                        let threadCondition = KBGenericCondition(
                            .beginsWith, value: "\(SHInteractionAnchor.thread.rawValue)::\(threadId)"
                        )
                        messagesQueue.keyValuesAndTimestamps(
                            forKeysMatching: threadCondition,
                            timestampMatching: nil,
                            paginate: KBPaginationOptions(limit: 1, offset: 0),
                            sort: .descending
                        ) { messagesResult in
                            
                            let lastCreatedMessageKvts: KBKVPairWithTimestamp
                            
                            switch messagesResult {
                            case .success(let messagesKvts):
                                if let firstMessagesKvts = messagesKvts.first {
                                    lastCreatedMessageKvts = firstMessagesKvts
                                } else {
                                    dispatchGroup.leave()
                                    return
                                }
                            case .failure(let error):
                                log.error("failed to fetch messages for condition \(threadCondition): \(error.localizedDescription)")
                                dispatchGroup.leave()
                                return
                            }
                            
                            let keyComponents = lastCreatedMessageKvts.key.components(separatedBy: "::")
                            guard keyComponents.count == 6 else {
                                log.warning("invalid reaction key in local DB: \(lastCreatedMessageKvts.key). Expected `<anchorType>::<anchorId>::<senderId>::<inReplyToInteractionId>::<inReplyToAssetId>::<interactionId>")
                                dispatchGroup.leave()
                                return
                            }
                            guard let messageOutput = try? LocalServer.toMessageOutput(lastCreatedMessageKvts) else {
                                dispatchGroup.leave()
                                return
                            }
                            
                            lastMessageByThreadId[threadId] = messageOutput
                            
                            dispatchGroup.leave()
                        }
                        
                        dispatchGroup.enter()
                        messagesQueue.keys(
                            matching: threadCondition
                        ) { messagesResult in
                            
                            let keys: [String]
                            
                            switch messagesResult {
                            case .success(let ks):
                                keys = ks
                            case .failure(let error):
                                log.error("failed to fetch messages for condition nil. \(error.localizedDescription)")
                                dispatchGroup.leave()
                                return
                            }
                            
                            for key in keys {
                                let keyComponents = key.components(separatedBy: "::")
                                guard keyComponents.count == 6 else {
                                    log.warning("invalid message key in local DB: \(key). Expected `<anchorType>::<anchorId>::<senderId>::<inReplyToInteractionId>::<inReplyToAssetId>::<interactionId>")
                                    continue
                                }
                                let threadId = keyComponents[1]
                                let interactionId = keyComponents[5]
                                if interactionIdsByThreadId[threadId] == nil {
                                    interactionIdsByThreadId[threadId] = Set()
                                }
                                interactionIdsByThreadId[threadId]!.insert(interactionId)
                            }
                            
                            dispatchGroup.leave()
                        }
                    }
                    
                    usleep(useconds_t(10 * 1000)) // sleep 10ms
                }
                
                dispatchGroup.notify(queue: .global()) {
                    var threadSummaryById = [String: InteractionsThreadSummaryDTO]()
                    
                    for (threadId, thread) in threadsById {
                        let assetIdsCount: Int
                        do {
                            let assetIds = try SHKGQuery.assetGlobalIdentifiers(
                                amongst: thread.membersPublicIdentifier,
                                requestingUserId: self.requestor.identifier
                            )
                            assetIdsCount = assetIds.count
                        } catch {
                            assetIdsCount = 0
                        }
                        let threadSummary = InteractionsThreadSummaryDTO(
                            thread: thread,
                            lastEncryptedMessage: lastMessageByThreadId[threadId],
                            numMessages: interactionIdsByThreadId[threadId]?.count ?? 0,
                            numAssets: assetIdsCount
                        )
                        threadSummaryById[threadId] = threadSummary
                    }
                    completionHandler(.success(threadSummaryById))
                }
            }
        }
    }
    
    func topLevelGroupsInteractionsSummary(completionHandler: @escaping (Result<[String: InteractionsGroupSummaryDTO], Error>) -> ()) {
        guard let reactionStore = SHDBManager.sharedInstance.reactionStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        guard let messagesQueue = SHDBManager.sharedInstance.messagesQueue else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        var allGroupIds = Set<String>()
        
        var numCommentsByGroupId = [String: Int]()
        var reactionsByGroupId = [String: [ReactionOutputDTO]]()
        
        let allGroupsCondition = KBGenericCondition(.beginsWith, value: "\(SHInteractionAnchor.group.rawValue)::")
        
        let dispatchGroup = DispatchGroup()
        
        ///
        /// Retrieve all reactions across all groupIds, and store them by groupId in
        /// `reactionsByGroupId`
        ///
        dispatchGroup.enter()
        reactionStore.keyValuesAndTimestamps(
            forKeysMatching: allGroupsCondition,
            timestampMatching: nil
        ) { reactionsResult in
            
            switch reactionsResult {
            case .success(let reactionKvts):
                reactionKvts.forEach({
                    let keyComponents = $0.key.components(separatedBy: "::")
                    guard keyComponents.count == 6 else {
                        log.warning("invalid reaction key in local DB: \($0.key). Expected `<anchorType>::<anchorId>::<senderId>::<inReplyToInteractionId>::<inReplyToAssetId>::<interactionId>")
                        return
                    }
                    guard let reactionOutput = LocalServer.toReactionOutput($0) else {
                        return
                    }
                    
                    let groupId = keyComponents[1]
                    reactionsByGroupId[groupId] = (reactionsByGroupId[groupId] ?? [])
                    reactionsByGroupId[groupId]?.append(reactionOutput)
                    
                    allGroupIds.insert(groupId)
                })
            case .failure(let error):
                log.critical("failed to retrieve group reactions: \(error.localizedDescription)")
            }
            
            dispatchGroup.leave()
        }
        
        ///
        /// Retrieve all messages (comments) in all groups to update `numCommentsByGroupId`
        ///
        dispatchGroup.enter()
        messagesQueue.keys(matching: allGroupsCondition) { messagesResult in
            
            let keys: [String]
            
            switch messagesResult {
            case .success(let ks):
                keys = ks
            case .failure(let error):
                log.error("failed to fetch messages for condition nil. \(error.localizedDescription)")
                dispatchGroup.leave()
                return
            }
            
            for key in keys {
                let keyComponents = key.components(separatedBy: "::")
                guard keyComponents.count == 6 else {
                    log.warning("invalid message key in local DB: \(key). Expected `<anchorType>::<anchorId>::<senderId>::<inReplyToInteractionId>::<inReplyToAssetId>::<interactionId>")
                    continue
                }
                let anchorId = keyComponents[1]
                numCommentsByGroupId[anchorId] = (numCommentsByGroupId[anchorId] ?? 0) + 1
                allGroupIds.insert(anchorId)
            }
            
            dispatchGroup.leave()
        }
        
        dispatchGroup.notify(queue: .global()) {
            var groupSummaryById = [String: InteractionsGroupSummaryDTO]()
            
            for groupId in allGroupIds {
                let groupSummary = InteractionsGroupSummaryDTO(
                    numComments: numCommentsByGroupId[groupId] ?? 0,
                    firstEncryptedMessage: nil, // TODO: Retrieve earliest message for each group easily
                    reactions: reactionsByGroupId[groupId] ?? []
                )
                
                groupSummaryById[groupId] = groupSummary
            }
            
            completionHandler(.success(groupSummaryById))
        }
    }
    
    func topLevelInteractionsSummary(
        inGroup groupId: String,
        completionHandler: @escaping (Result<InteractionsGroupSummaryDTO, Error>) -> ()
    ) {
        guard let reactionStore = SHDBManager.sharedInstance.reactionStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        guard let messagesQueue = SHDBManager.sharedInstance.messagesQueue else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        var reactions = [ReactionOutputDTO]()
        var numMessages = 0
        
        let condition = KBGenericCondition(.beginsWith, value: "\(SHInteractionAnchor.group.rawValue)::\(groupId)::")
        reactionStore.keyValuesAndTimestamps(
            forKeysMatching: condition,
            timestampMatching: nil
        ) { reactionsResult in
            switch reactionsResult {
            case .success(let reactionKvts):
                reactionKvts.forEach({
                    if let output = LocalServer.toReactionOutput($0) {
                        reactions.append(output)
                    }
                })
            case .failure(let error):
                log.critical("failed to retrieve reactions for group \(groupId): \(error.localizedDescription)")
            }
            
            messagesQueue.keys(matching: condition) { messagesResult in
                switch messagesResult {
                case .success(let messagesKeys):
                    numMessages = messagesKeys.count
                case .failure(let error):
                    log.critical("failed to retrieve messages for group \(groupId): \(error.localizedDescription)")
                }
                
                let response = InteractionsGroupSummaryDTO(
                    numComments: numMessages,
                    firstEncryptedMessage: nil, // TODO: Retrieve earliest message for each group easily
                    reactions: reactions
                )
                completionHandler(.success(response))
            }
        }
    }
    
    func addReactions(
        _ reactions: [ReactionInput],
        toGroup groupId: String,
        completionHandler: @escaping (Result<[ReactionOutputDTO], Error>) -> ()
    ) {
        self.addReactions(reactions, anchorType: .group, anchorId: groupId, completionHandler: completionHandler)
    }
    
    func addReactions(
        _ reactions: [ReactionInput],
        toThread threadId: String,
        completionHandler: @escaping (Result<[ReactionOutputDTO], Error>) -> ()
    ) {
        self.addReactions(reactions, anchorType: .thread, anchorId: threadId, completionHandler: completionHandler)
    }
    
    private func addReactions(
        _ reactions: [ReactionInput],
        anchorType: SHInteractionAnchor,
        anchorId: String,
        completionHandler: @escaping (Result<[ReactionOutputDTO], Error>) -> ()
    ) {
        guard let reactionStore = SHDBManager.sharedInstance.reactionStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        var deleteCondition = KBGenericCondition(value: false)
        for reaction in reactions {
            guard let senderPublicIdentifier = reaction.senderPublicIdentifier else {
                log.warning("[LocalServer] failed to remove reaction from \(anchorType.rawValue) \(anchorId): sender information is missing")
                continue
            }
            
            deleteCondition = deleteCondition
                .or(
                    KBGenericCondition(.beginsWith, value: "\(anchorType.rawValue)::\(anchorId)::\(senderPublicIdentifier)")
                )
        }
        reactionStore.removeValues(forKeysMatching: deleteCondition) { result in
            if case .failure(let error) = result {
                completionHandler(.failure(error))
                return
            }
            
            let writeBatch = reactionStore.writeBatch()
            
            for reaction in reactions {
                guard let interactionId = reaction.interactionId else {
                    log.warning("[LocalServer] failed to save reaction in \(anchorType.rawValue) \(anchorId): interaction identifier is missing")
                    continue
                }
                guard let senderPublicIdentifier = reaction.senderPublicIdentifier else {
                    log.warning("[LocalServer] failed to save reaction: sender information is missing")
                    continue
                }
                var key = "\(anchorType.rawValue)::\(anchorId)::\(senderPublicIdentifier)"
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
                key += "::\(interactionId)"
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
    }
    
    func removeReaction(
        _ reactionType: ReactionType,
        senderPublicIdentifier: UserIdentifier,
        inReplyToAssetGlobalIdentifier: GlobalIdentifier?,
        inReplyToInteractionId: String?,
        fromGroup groupId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        self.removeReaction(
            reactionType,
            senderPublicIdentifier: senderPublicIdentifier,
            inReplyToAssetGlobalIdentifier: inReplyToAssetGlobalIdentifier,
            inReplyToInteractionId: inReplyToInteractionId,
            anchorType: .group,
            anchorId: groupId,
            completionHandler: completionHandler
        )
    }
    
    func removeReaction(
        _ reactionType: ReactionType,
        senderPublicIdentifier: UserIdentifier,
        inReplyToAssetGlobalIdentifier: GlobalIdentifier?,
        inReplyToInteractionId: String?,
        fromThread threadId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        self.removeReaction(
            reactionType,
            senderPublicIdentifier: senderPublicIdentifier,
            inReplyToAssetGlobalIdentifier: inReplyToAssetGlobalIdentifier,
            inReplyToInteractionId: inReplyToInteractionId,
            anchorType: .thread,
            anchorId: threadId,
            completionHandler: completionHandler
        )
    }
    
    private func removeReaction(
        _ reactionType: ReactionType,
        senderPublicIdentifier: UserIdentifier,
        inReplyToAssetGlobalIdentifier: GlobalIdentifier?,
        inReplyToInteractionId: String?,
        anchorType: SHInteractionAnchor,
        anchorId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        guard let reactionStore = SHDBManager.sharedInstance.reactionStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        var keyStart = "\(anchorType.rawValue)::\(anchorId)::\(senderPublicIdentifier)"
        if let interactionId = inReplyToInteractionId {
            keyStart += "::\(interactionId)"
        } else {
            keyStart += "::"
        }
        if let assetGid = inReplyToAssetGlobalIdentifier {
            keyStart += "::\(assetGid)"
        } else {
            keyStart += "::"
        }
        
        let condition = KBGenericCondition(.beginsWith, value: keyStart)
        
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
        inGroup groupId: String,
        ofType type: InteractionType?,
        underMessage messageId: String?,
        before: Date?,
        limit: Int,
        completionHandler: @escaping (Result<InteractionsGroupDTO, Error>) -> ()
    ) {
        self.retrieveInteractions(
            anchorType: .group,
            anchorId: groupId,
            ofType: type,
            underMessage: messageId,
            before: before,
            limit: limit,
            completionHandler: completionHandler
        )
    }
    
    func retrieveInteractions(
        inThread threadId: String,
        ofType type: InteractionType?,
        underMessage messageId: String?,
        before: Date?,
        limit: Int,
        completionHandler: @escaping (Result<InteractionsGroupDTO, Error>) -> ()
    ) {
        self.retrieveInteractions(
            anchorType: .thread,
            anchorId: threadId,
            ofType: type,
            underMessage: messageId,
            before: before,
            limit: limit,
            completionHandler: completionHandler
        )
    }
    
    func retrieveInteraction(
        anchorType: SHInteractionAnchor,
        anchorId: String,
        withId interactionIdentifier: String,
        completionHandler: @escaping (Result<InteractionsGroupDTO, Error>) -> ()
    ) {
        guard let reactionStore = SHDBManager.sharedInstance.reactionStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        guard let messagesQueue = SHDBManager.sharedInstance.messagesQueue else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        ///
        /// KEY FORMAT:
        /// `{anchor_type}::{anchor_id}::{sender_id}::{ref_interaction_id}::{ref_asset_id}::{interaction_id}`
        /// - `anchor_type`: either "thread' or "group", for threads and shares, respectively
        /// - `anchor_id`: either the `threadId` or the `groupId`, to identify a thread or a share, respectively
        /// - `sender_id`: the user public identifier, author of the interaction
        /// - `ref_interaction_id`: a pointer to the interaction this interaction references (for replies to messages the origin message id)
        /// - `ref_asset_id`: a pointer to the global asset identifer this interaction references
        /// - `interaction_id`: the unique interaction identifier as provided by the server
        ///
        
        let condition = KBGenericCondition(.endsWith, value: interactionIdentifier)
            .and(KBGenericCondition(.beginsWith, value: "\(anchorType.rawValue)::\(anchorId)"))
        
        let retrieveReactions = {
            (callback: @escaping (Result<[ReactionOutputDTO], Error>) -> Void) in
            
            log.debug("retrieving reactions with id \(interactionIdentifier)")
            
            reactionStore.keyValuesAndTimestamps(
                forKeysMatching: condition,
                timestampMatching: nil,
                paginate: KBPaginationOptions(limit: 1, offset: 0)
            ) { reactionsResult in
                switch reactionsResult {
                case .success(let reactionKvts):
                    var reactions = [ReactionOutputDTO]()
                    reactionKvts.forEach({
                        if let output = LocalServer.toReactionOutput($0) {
                            reactions.append(output)
                        }
                    })
                    callback(.success(reactions))
                    
                case .failure(let err):
                    callback(.failure(err))
                }
            }
        }
        
        let retrieveMessages = {
            (callback: @escaping (Result<[MessageOutputDTO], Error>) -> Void) in
            
            log.debug("retrieving messagewith id \(interactionIdentifier)")
            
            messagesQueue.keyValuesAndTimestamps(
                forKeysMatching: condition,
                timestampMatching: nil,
                paginate: KBPaginationOptions(limit: 1, offset: 0)
            ) { messagesResult in
                switch messagesResult {
                case .success(let messageKvts):
                    var messages = [MessageOutputDTO]()
                    
                    for messageKvt in messageKvts {
                        do {
                            messages.append(
                                try LocalServer.toMessageOutput(messageKvt)
                            )
                        } catch {
                            log.error("failed to parse message with key \(messageKvt.key)")
                            continue
                        }
                    }
                    callback(.success(messages))
                    
                case .failure(let err):
                    callback(.failure(err))
                }
            }
        }
        
        let dispatchGroup = DispatchGroup()
        var error: Error? = nil
        var encryptionDetails: RecipientEncryptionDetailsDTO? = nil
        
        dispatchGroup.enter()
        self.retrieveUserEncryptionDetails(anchorType: anchorType, anchorId: anchorId) {
            encryptionDetailsResult in
            switch encryptionDetailsResult {
            case .failure(let err):
                error = err
            case .success(let e2eeResult):
                encryptionDetails = e2eeResult
            }
            dispatchGroup.leave()
        }
        
        dispatchGroup.notify(queue: .global()) {
            guard error == nil else {
                completionHandler(.failure(error!))
                return
            }
            
            guard let encryptionDetails else {
                switch anchorType {
                case .group:
                    completionHandler(.failure(SHBackgroundOperationError.missingE2EEDetailsForGroup(anchorId)))
                case .thread:
                    completionHandler(.failure(SHBackgroundOperationError.missingE2EEDetailsForThread(anchorId)))
                }
                return
            }
            
            var reactions = [ReactionOutputDTO]()
            var messages = [MessageOutputDTO]()
            
            dispatchGroup.enter()
            retrieveMessages { result in
                switch result {
                case .success(let m):
                    messages = m
                case .failure(let err):
                    error = err
                }
                dispatchGroup.leave()
            }
            
            dispatchGroup.enter()
            retrieveReactions { result in
                switch result {
                case .success(let r):
                    reactions = r
                case .failure(let err):
                    error = err
                }
                dispatchGroup.leave()
            }
            
            dispatchGroup.notify(queue: .global()) {
                guard error == nil else {
                    completionHandler(.failure(error!))
                    return
                }
                
                let result = InteractionsGroupDTO(
                    messages: messages,
                    reactions: reactions,
                    ephemeralPublicKey: encryptionDetails.ephemeralPublicKey,
                    encryptedSecret: encryptionDetails.encryptedSecret,
                    secretPublicSignature: encryptionDetails.secretPublicSignature,
                    senderPublicSignature: encryptionDetails.senderPublicSignature
                )
                completionHandler(.success(result))
            }
        }
    }
    
    private static func toMessageOutput(_ messageKvt: KBKVPairWithTimestamp) throws -> MessageOutputDTO {
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
            throw SHBackgroundOperationError.unexpectedData(serializedMessage)
        }
        
        return MessageOutputDTO(
            interactionId: message.interactionId,
            senderPublicIdentifier: message.senderPublicIdentifier,
            inReplyToAssetGlobalIdentifier: message.inReplyToAssetGlobalIdentifier,
            inReplyToInteractionId: message.inReplyToInteractionId,
            encryptedMessage: message.encryptedMessage,
            createdAt: message.createdAt
        )
    }
    
    private static func toReactionOutput(_ reactionKvt: KBKVPairWithTimestamp) -> ReactionOutputDTO? {
        let key = reactionKvt.key
        var interactionId: String? = nil
        var senderId: String? = nil
        var inReplyToAssetGid: String? = nil
        var inReplyToInteractionGid: String? = nil
        
        let keyComponents = key.components(separatedBy: "::")
        guard keyComponents.count == 6 else {
            log.warning("invalid reaction key in local DB: \(key). Expected `<anchorType>::<anchorId>::<senderId>::<inReplyToInteractionId>::<inReplyToAssetId>::<interactionId>")
            return nil
        }
        guard let reactionTypeInt = reactionKvt.value as? Int,
              let reactionType = ReactionType(rawValue: reactionTypeInt) else {
            log.warning("unexpected value in reactions DB: \(String(describing: reactionKvt.value))")
            return nil
        }
        
        senderId = keyComponents[2]
        interactionId = keyComponents[5]
        
        guard let senderId = senderId,
              let interactionId = interactionId else {
            log.warning("invalid key format in reactions DB: \(key)")
            return nil
        }
        
        if !keyComponents[3].isEmpty {
            inReplyToInteractionGid = keyComponents[3]
        }
        if !keyComponents[4].isEmpty {
            inReplyToAssetGid = keyComponents[4]
        }
        
        return ReactionOutputDTO(
            interactionId: interactionId,
            senderPublicIdentifier: senderId,
            inReplyToAssetGlobalIdentifier: inReplyToAssetGid,
            inReplyToInteractionId: inReplyToInteractionGid,
            reactionType: reactionType,
            addedAt: reactionKvt.timestamp.iso8601withFractionalSeconds
        )
    }
    
    private func retrieveInteractions(
        anchorType: SHInteractionAnchor,
        anchorId: String,
        ofType type: InteractionType?,
        underMessage refMessageId: String?,
        before: Date?,
        limit: Int,
        completionHandler: @escaping (Result<InteractionsGroupDTO, Error>) -> ()
    ) {
        guard let reactionStore = SHDBManager.sharedInstance.reactionStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        guard let messagesQueue = SHDBManager.sharedInstance.messagesQueue else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        ///
        /// KEY FORMAT:
        /// `{anchor_type}::{anchor_id}::{sender_id}::{ref_interaction_id}::{ref_asset_id}::{interaction_id}`
        /// - `anchor_type`: either "thread' or "group", for threads and shares, respectively
        /// - `anchor_id`: either the `threadId` or the `groupId`, to identify a thread or a share, respectively
        /// - `sender_id`: the user public identifier, author of the interaction
        /// - `ref_interaction_id`: a pointer to the interaction this interaction references (for replies to messages the origin message id)
        /// - `ref_asset_id`: a pointer to the global asset identifer this interaction references
        /// - `interaction_id`: the unique interaction identifier as provided by the server
        ///
        
        var condition = KBGenericCondition(.beginsWith, value: "\(anchorType.rawValue)::\(anchorId)::")
        if let refMessageId {
            condition = condition.and(
                KBGenericCondition(.contains, value: "::\(refMessageId)::")
                    .and(KBGenericCondition(.endsWith, value: "::\(refMessageId)", negated: true))
            )
        }
        
        let timeCondition: KBTimestampCondition?
        if let before {
            timeCondition = KBTimestampCondition(.before, value: before)
        } else {
            timeCondition = nil
        }
        
        let dispatchGroup = DispatchGroup()
        var error: Error? = nil
        var encryptionDetails: RecipientEncryptionDetailsDTO? = nil
        
        dispatchGroup.enter()
        self.retrieveUserEncryptionDetails(anchorType: anchorType, anchorId: anchorId) {
            encryptionDetailsResult in
            switch encryptionDetailsResult {
            case .failure(let err):
                error = err
            case .success(let e2eeResult):
                encryptionDetails = e2eeResult
            }
            dispatchGroup.leave()
        }
        
        dispatchGroup.notify(queue: .global()) {
            guard error == nil else {
                completionHandler(.failure(error!))
                return
            }
            
            guard let encryptionDetails else {
                switch anchorType {
                case .group:
                    completionHandler(.failure(SHBackgroundOperationError.missingE2EEDetailsForGroup(anchorId)))
                case .thread:
                    completionHandler(.failure(SHBackgroundOperationError.missingE2EEDetailsForThread(anchorId)))
                }
                return
            }
            
            let retrieveReactions = {
                (callback: @escaping (Result<[ReactionOutputDTO], Error>) -> Void) in
                
                log.debug("retrieving reactions (before=\(before?.iso8601withFractionalSeconds ?? "nil"), limit=\(limit)) in descending order for \(anchorType.rawValue) with id \(anchorId)")
                
                reactionStore.keyValuesAndTimestamps(
                    forKeysMatching: condition,
                    timestampMatching: timeCondition,
                    paginate: KBPaginationOptions(limit: limit, offset: 0),
                    sort: .descending
                ) { reactionsResult in
                    switch reactionsResult {
                    case .success(let reactionKvts):
                        var reactions = [ReactionOutputDTO]()
                        reactionKvts.forEach({
                            if let output = LocalServer.toReactionOutput($0) {
                                reactions.append(output)
                            }
                        })
                        callback(.success(reactions))
                        
                    case .failure(let err):
                        callback(.failure(err))
                    }
                }
            }
            
            let retrieveMessages = { 
                (callback: @escaping (Result<[MessageOutputDTO], Error>) -> Void) in
                
                log.debug("retrieving messages (before=\(before?.iso8601withFractionalSeconds ?? "nil"), limit=\(limit)) in descending order for \(anchorType.rawValue) with id \(anchorId)")
                
                messagesQueue.keyValuesAndTimestamps(
                    forKeysMatching: condition,
                    timestampMatching: timeCondition,
                    paginate: KBPaginationOptions(limit: limit, offset: 0),
                    sort: .descending
                ) { messagesResult in
                    switch messagesResult {
                    case .success(let messageKvts):
                        var messages = [MessageOutputDTO]()
                        
                        log.debug("found \(messageKvts.count) messages for \(anchorType.rawValue) with id \(anchorId)")
                        
                        for messageKvt in messageKvts {
                            do {
                                messages.append(
                                    try LocalServer.toMessageOutput(messageKvt)
                                )
                            } catch {
                                log.error("failed to retrieve message with key \(messageKvt.key)")
                                continue
                            }
                        }
                        callback(.success(messages))
                        
                    case .failure(let err):
                        callback(.failure(err))
                    }
                }
            }
            
            var reactions = [ReactionOutputDTO]()
            var messages = [MessageOutputDTO]()
            
            if type == nil || type == .message {
                dispatchGroup.enter()
                retrieveMessages { result in
                    switch result {
                    case .success(let m):
                        messages = m
                    case .failure(let err):
                        error = err
                    }
                    dispatchGroup.leave()
                }
            }
                
            if type == nil || type == .reaction {
                dispatchGroup.enter()
                retrieveReactions { result in
                    switch result {
                    case .success(let r):
                        reactions = r
                    case .failure(let err):
                        error = err
                    }
                    dispatchGroup.leave()
                }
            }
                
            dispatchGroup.notify(queue: .global()) {
                guard error == nil else {
                    completionHandler(.failure(error!))
                    return
                }
                
                let result = InteractionsGroupDTO(
                    messages: messages,
                    reactions: reactions,
                    ephemeralPublicKey: encryptionDetails.ephemeralPublicKey,
                    encryptedSecret: encryptionDetails.encryptedSecret,
                    secretPublicSignature: encryptionDetails.secretPublicSignature,
                    senderPublicSignature: encryptionDetails.senderPublicSignature
                )
                completionHandler(.success(result))
            }
        }
    }
    
    func addMessages(
        _ messages: [MessageInput],
        toGroup groupId: String,
        completionHandler: @escaping (Result<[MessageOutputDTO], Error>) -> ()
    ) {
        self.addMessages(messages, anchorType: .group, anchorId: groupId, completionHandler: completionHandler)
    }
    
    func addMessages(
        _ messages: [MessageInput],
        toThread threadId: String,
        completionHandler: @escaping (Result<[MessageOutputDTO], Error>) -> ()
    ) {
        self.addMessages(messages, anchorType: .thread, anchorId: threadId, completionHandler: completionHandler)
    }
    
    private func addMessages(
        _ messages: [MessageInput],
        anchorType: SHInteractionAnchor,
        anchorId: String,
        completionHandler: @escaping (Result<[MessageOutputDTO], Error>) -> ()
    ) {
        guard let messagesQueue = SHDBManager.sharedInstance.messagesQueue else {
            completionHandler(.failure(KBError.databaseNotReady))
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
                senderPublicIdentifier: message.senderPublicIdentifier!,
                inReplyToAssetGlobalIdentifier: message.inReplyToInteractionId,
                inReplyToInteractionId: message.inReplyToInteractionId,
                encryptedMessage: message.encryptedMessage,
                createdAt: message.createdAt!
            )
            
            do {
                var key = "\(anchorType.rawValue)::\(anchorId)::\(message.senderPublicIdentifier!)"
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
                key += "::\(interactionId)"
                let value = DBSecureSerializableUserMessage(
                    interactionId: message.interactionId!,
                    senderPublicIdentifier: message.senderPublicIdentifier!,
                    inReplyToAssetGlobalIdentifier: message.inReplyToInteractionId,
                    inReplyToInteractionId: message.inReplyToInteractionId,
                    encryptedMessage: message.encryptedMessage,
                    createdAt: message.createdAt!
                )
                
                let data = try NSKeyedArchiver.archivedData(withRootObject: value, requiringSecureCoding: true)
                try messagesQueue.insert(
                    data,
                    withIdentifier: key,
                    timestamp: message.createdAt!.iso8601withFractionalSeconds!
                )
                
                result.append(messageOutput)
            } catch {
                if firstError == nil {
                    firstError = error
                }
                switch anchorType {
                case .group:
                    log.error("failed to locally add message with id \(message.interactionId!) to group \(anchorId): \(error)")
                case .thread:
                    log.error("failed to locally add message with id \(message.interactionId!) to thread \(anchorId): \(error)")
                }
            }
        }
        
        if let firstError {
            completionHandler(.failure(firstError))
        } else {
            completionHandler(.success(result))
        }
    }
    
}
