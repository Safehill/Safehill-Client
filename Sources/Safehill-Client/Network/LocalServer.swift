import Foundation
import KnowledgeBase
import Contacts

public let SHDefaultDBTimeoutInMilliseconds = 15000 // 15 seconds

public enum SHLocalServerError: Error, LocalizedError {
    case failedToCreateFile
    case threadNotPresent(String)
    
    public var errorDescription: String? {
        switch self {
        case .failedToCreateFile:
            "Failed to create asset file on disk"
        case .threadNotPresent(let threadId):
            "There is no such thread with id \(threadId)"
        }
    }
}

struct LocalServer : SHLocalServerAPI {
    
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
        if !fileManager.fileExists(atPath: encryptedDataURL.path) {
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
    
    func updateUser(name: String?,
                    phoneNumber: SHPhoneNumber? = nil,
                    forcePhoneNumberLinking: Bool = false,
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
            var value = [
                "identifier": user.identifier,
                "name": user.name,
                "publicKey": user.publicKeyData,
                "publicSignature": user.publicSignatureData
            ] as [String : Any]
            
            if let phoneNumber = user.phoneNumber {
                value["phoneNumber"] = phoneNumber
            }
            
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
        
        let condition: KBGenericCondition
        if identifiers.isEmpty {
            condition = KBGenericCondition(value: true)
        }
        else {
            var c = KBGenericCondition(value: false)
            for userIdentifier in identifiers {
                c = c.or(KBGenericCondition(.equal, value: userIdentifier))
            }
            condition = c
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
        
        try? self.deleteAvatarImages()
        
        group.notify(queue: .global()) {
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
    }
    
    private func serializeUser(_ res: Any?) -> (any SHServerUser)? {
        var serialized: (any SHServerUser)? = nil
        
        if let res = res as? [String: Any] {
            if let identifier = res["identifier"] as? String,
               let name = res["name"] as? String,
               let publicKeyData = res["publicKey"] as? Data,
               let publicSignatureData = res["publicSignature"] as? Data {

                let phoneNumber = res["phoneNumber"] as? String
                
                let remoteUser: SHServerUser
                if let systemContactId = res["systemContactId"] as? String {
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
                        phoneNumber: phoneNumber,
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
    
    func countUploaded(
        completionHandler: @escaping (Swift.Result<Int, Error>) -> ()
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
        let globalIdentifiers = Array(Set(globalIdentifiers))
        var gidsCondition = KBGenericCondition(value: true)
        
        if globalIdentifiers.isEmpty == false, useCache {
            
            var _globalIdentifiersToFetch = Set(globalIdentifiers)
            
            for globalIdentifier in globalIdentifiers {
                if let cachedValue = assetDescriptorInMemoryCache.value(forKey: globalIdentifier) as? SHGenericAssetDescriptorClass {
                    descriptors.append(SHGenericAssetDescriptor.from(cachedValue))
                    _globalIdentifiersToFetch.remove(globalIdentifier)
                }
            }
            
            guard _globalIdentifiersToFetch.isEmpty == false else {
                completionHandler(.success(descriptors))
                return
            }
            
            globalIdentifiers.forEach({ gid in
                gidsCondition = gidsCondition.or(KBGenericCondition(.contains, value: gid))
            })
        }
        
        var groupCreatorById = [String: UserIdentifier]()
        var groupInfoByIdByAssetGid = [GlobalIdentifier: [String: SHAssetGroupInfo]]()
        var groupIdsByRecipientUserIdentifierByAssetId = [GlobalIdentifier: [UserIdentifier: [String]]]()
        
        ///
        /// Retrieve all information from the asset store for all assets and `.lowResolution` versions.
        /// **We can safely assume all versions are shared using the same group id, and will have same sender and receiver info*
        ///
        let senderCondition = KBGenericCondition(
            .beginsWith, value: "sender::"
        ).and(KBGenericCondition(
            .contains, value: "::low::"
        ).and(gidsCondition))
        
        let senderKeys: [String]
        do {
            senderKeys = try assetStore.keys(matching: senderCondition)
        } catch {
            log.critical("error reading from the assets DB. \(error.localizedDescription)")
            completionHandler(.failure(error))
            return
        }
        
        for senderKey in senderKeys {
            let components = senderKey.components(separatedBy: "::")
            /// Components:
            /// 0) "sender"
            /// 1) sender user identifier
            /// 2) version quality
            /// 3) asset identifier
            /// 4) group identifier
            
            if components.count == 5 {
                let groupCreatorId = components[1]
                let groupId = components[4]
                groupCreatorById[groupId] = groupCreatorId
            } else {
                log.error("invalid sender info key in DB: \(senderKey)")
            }
        }
        
        let receiverCondition = KBGenericCondition(
            .beginsWith, value: "receiver::"
        ).and(KBGenericCondition(
            .contains, value: "::low::") // Can safely assume all versions are shared using the same group id
        ).and(gidsCondition)
        
        let recipientDetailsDict: [String: DBSecureSerializableAssetRecipientSharingDetails?]
        let groupPhoneNumberInvitations: [String: [String: String]]
        var groupIdsToThreadIds = [String: String]()
        do {
            groupIdsToThreadIds = try self.groupIdToThreadIdMapping()
        } catch {
            log.critical("failed to retrieve group id to thread id mapping")
        }
        
        do {
            recipientDetailsDict = try assetStore
                .dictionaryRepresentation(forKeysMatching: receiverCondition)
                .mapValues { try? DBSecureSerializableAssetRecipientSharingDetails.from($0) }
            
            groupPhoneNumberInvitations = (try? self.groupPhoneNumberInvitations()) ?? [:]
        } catch {
            log.critical("error reading from DB. \(error.localizedDescription)")
            completionHandler(.failure(error))
            return
        }
        
        let encryptedTitles: [String: String]
        
        do {
            encryptedTitles = try self.getEncryptedTitles()
        } catch {
            log.critical("error retrieving group titles. \(error.localizedDescription)")
            completionHandler(.failure(error))
            return
        }
        
        let permissionsById: [String: Int]
        do {
            permissionsById = try self.permissions(for: recipientDetailsDict.values.compactMap({ $0?.groupId }))
        } catch {
            log.critical("error retrieving group permissions. \(error.localizedDescription)")
            completionHandler(.failure(error))
            return
        }
        
        for (key, value) in recipientDetailsDict {
            
            guard let value else {
                log.error("invalid value for key \(key) in DB")
                continue
            }
            
            let components = key.components(separatedBy: "::")
            /// Components:
            /// 0) "receiver"
            /// 1) receiver user public identifier
            /// 2) version quality
            /// 3) asset identifier
            /// 4) group identifier
            
            let groupId = value.groupId
            let assetGid: GlobalIdentifier
            
            if components.count == 5 {
                assetGid = components[3]
                if filteringGroupIds == nil || filteringGroupIds!.contains(groupId) {
                    let receiverUser = components[1]
                    
                    if groupIdsByRecipientUserIdentifierByAssetId[assetGid] == nil {
                        groupIdsByRecipientUserIdentifierByAssetId[assetGid] = [receiverUser: [groupId]]
                    } else {
                        if groupIdsByRecipientUserIdentifierByAssetId[assetGid]![receiverUser] == nil {
                            groupIdsByRecipientUserIdentifierByAssetId[assetGid]![receiverUser] = [groupId]
                        } else {
                            groupIdsByRecipientUserIdentifierByAssetId[assetGid]![receiverUser]!.append(groupId)
                        }
                    }
                }
            } else {
                log.error("failed to retrieve sharing information. Invalid entry format: \(key) -> \(value)")
                continue
            }
            
            if filteringGroupIds == nil || filteringGroupIds!.contains(groupId) {
                let thisGroupPhoneNumberInvitation = groupPhoneNumberInvitations[groupId]
                let createdBy = groupCreatorById[groupId]
                let encryptedTitle = encryptedTitles[groupId]
                let groupInfo = SHGenericAssetGroupInfo(
                    encryptedTitle: encryptedTitle,
                    createdBy: createdBy,
                    createdAt: value.groupCreationDate,
                    createdFromThreadId: groupIdsToThreadIds[value.groupId],
                    invitedUsersPhoneNumbers: thisGroupPhoneNumberInvitation ?? [:],
                    permissions: permissionsById[value.groupId]
                )
                
                if groupInfoByIdByAssetGid[assetGid] == nil {
                    groupInfoByIdByAssetGid[assetGid] = [groupId: groupInfo]
                } else {
                    groupInfoByIdByAssetGid[assetGid]![groupId] = groupInfo
                }
            }
        }
        
        ///
        /// Retrieve all information from the asset store for all assets and matching versions.
        ///
        var versionUploadStateByIdentifierQuality = [GlobalIdentifier: [SHAssetQuality: SHAssetDescriptorUploadState]]()
        var localInfoByGlobalIdentifier = [GlobalIdentifier: (phAssetId: LocalIdentifier?, creationDate: Date?)]()
        
        var condition = KBGenericCondition(value: false)
        for quality in SHAssetQuality.all {
            condition = condition.or(KBGenericCondition(.beginsWith, value: "\(quality.rawValue)::"))
        }
        
        let keyValues: [String: DBSecureSerializableAssetVersionMetadata?]
        do {
            keyValues = try assetStore
                .dictionaryRepresentation(forKeysMatching: condition)
                .mapValues { try? DBSecureSerializableAssetVersionMetadata.from($0) }
        } catch {
            log.critical("error reading from the assets DB. \(error.localizedDescription)")
            completionHandler(.failure(error))
            return
        }
        
        for (k, v) in keyValues {
            
            guard let v else {
                log.error("invalid value for key \(k) in DB")
                continue
            }
            
            /// If caller requested all assets or the retrieved asset is not in the set to retrieve, skip processing
            guard globalIdentifiers.isEmpty || globalIdentifiers.contains(v.globalIdentifier) else {
                continue
            }
            
            /// If the descriptor for the asset was already pulled from the cache, skip processing
            guard descriptors.contains(where: { $0.globalIdentifier == v.globalIdentifier }) == false else {
                continue
            }
            
            if versionUploadStateByIdentifierQuality[v.globalIdentifier] == nil {
                versionUploadStateByIdentifierQuality[v.globalIdentifier] = [v.quality: v.uploadState]
            } else {
                versionUploadStateByIdentifierQuality[v.globalIdentifier]![v.quality] = v.uploadState
            }
            
            localInfoByGlobalIdentifier[v.globalIdentifier] = (
                phAssetId: localInfoByGlobalIdentifier[v.globalIdentifier]?.phAssetId ?? v.localIdentifier,
                creationDate: localInfoByGlobalIdentifier[v.globalIdentifier]?.creationDate ?? v.creationDate
            )
        }
        
        for globalIdentifier in versionUploadStateByIdentifierQuality.keys {
            guard let groupInfoById = groupInfoByIdByAssetGid[globalIdentifier],
                  let sharedWithUsersInGroups = groupIdsByRecipientUserIdentifierByAssetId[globalIdentifier]
            else {
                log.warning("failed to retrieve group information for asset \(globalIdentifier)")
                continue
            }
            
            guard let sharedBy = try? assetStore.values(for: ["creator::\(globalIdentifier)"]).first as? UserIdentifier else {
                log.warning("failed to retrieve creator for asset \(globalIdentifier)")
                continue
            }
            
            if Set(sharedWithUsersInGroups.values.flatMap({ $0 })).count > groupInfoById.count
                || groupInfoById.values.contains(where: { $0.createdAt == nil }) {
                log.warning("some group information (or the creation date of such groups) is missing. \(groupInfoById.map({ ($0.key, $0.value.createdAt) }))")
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
                groupIdsByRecipientUserIdentifier: sharedWithUsersInGroups,
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
            
            var combinedUploadState: SHAssetDescriptorUploadState = .started
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
        
        completionHandler(.success(descriptors))
    }
    
    func getEncryptedTitles(
        for groupIds: [String]? = nil
    ) throws -> [String: String] {
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            throw KBError.databaseNotReady
        }
        
        let encryptedTitles: [String: String]
        
        var condition = KBGenericCondition(
            .beginsWith, value: "\(SHInteractionAnchor.group.rawValue)::"
        )
        
        if groupIds == nil {
            condition = condition.and(KBGenericCondition(
                .endsWith, value: "::encryptedTitle"
            ))
        } else {
            var groupCondition = KBGenericCondition(value: false)
            for groupId in groupIds! {
                groupCondition = groupCondition.or(KBGenericCondition(
                    .endsWith, value: "::\(groupId)::encryptedTitle"
                ))
            }
            condition = condition.and(groupCondition)
        }
        
        encryptedTitles = try userStore.dictionaryRepresentation(
            forKeysMatching: condition
        ).reduce([String: String]()) { (partialResult, tuple) in
            let (key, value) = tuple
            let components = key.components(separatedBy: "::")
            guard components.count == 3 else {
                log.warning("invalid encryptedTitle key: \(key)")
                return partialResult
            }
            let groupId = components[1]

            guard let encryptedTitle = value as? String else {
                log.warning("invalid encryptedTitle value: \(key) -> \(String(describing: value))")
                return partialResult
            }
            
            var result = partialResult
            result[groupId] = encryptedTitle
            return result
        }
        
        return encryptedTitles
    }
    
    func setGroupTitle(
        encryptedTitle: String,
        groupId: String,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        userStore.set(
            value: encryptedTitle,
            for: "\(SHInteractionAnchor.group.rawValue)::\(groupId)::encryptedTitle",
            completionHandler: completionHandler
        )
    }
    
    func updateGroupIds(_ groupInfoById: [String: GroupInfoDiff],
                        completionHandler: @escaping (Result<Void, Error>) -> Void) {
        
        guard let assetStore = SHDBManager.sharedInstance.assetStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        var groupIdsCondition = KBGenericCondition(value: true)
        for groupId in groupInfoById.keys {
            groupIdsCondition = groupIdsCondition.or(KBGenericCondition(.endsWith, value: groupId))
        }
        
        ///
        /// Update the USER STORE
        /// - invitations
        /// - remove invalid keys
        /// - update group titles
        ///
        
        let userStoreWriteBatch = userStore.writeBatch()
        var userStoreInvalidKeys = Set<String>()
        
        do {
            let allInvitationKeys = try userStore.keys(
                    matching: KBGenericCondition(
                        .beginsWith, value: "invitations::"
                    ).and(
                        groupIdsCondition
                    )
                )
            
            for key in allInvitationKeys {
                let keyComponents = key.components(separatedBy: "::")
                guard keyComponents.count == 3
                else {
                    userStoreInvalidKeys.insert(key)
                    continue
                }
                
                let groupId = keyComponents[2]
                guard keyComponents[1] == SHInteractionAnchor.group.rawValue,
                      let newGroupInfo = groupInfoById[groupId]?.groupInfo
                else {
                    continue
                }
                
                if let newInvitedNumbers = newGroupInfo.invitedUsersPhoneNumbers,
                   newInvitedNumbers.isEmpty == false {
                    let data = try NSKeyedArchiver.archivedData(
                        withRootObject: newInvitedNumbers.map({
                            DBSecureSerializableInvitation(phoneNumber: $0.key, invitedAt: $0.value)
                        }),
                        requiringSecureCoding: true
                    )
                    userStoreWriteBatch.set(value: data, for: key)
                } else {
                    userStoreWriteBatch.set(value: nil, for: key)
                }
            }
        } catch {
            log.error("failed to update group invitations from remote to local server. \(error.localizedDescription)")
        }
        
        for (groupId, diff) in groupInfoById {
            if let encryptedTitle = diff.groupInfo.encryptedTitle {
                userStoreWriteBatch.set(
                    value: encryptedTitle,
                    for: "\(SHInteractionAnchor.group.rawValue)::\(groupId)::encryptedTitle"
                )
            }
        }
        
        if userStoreInvalidKeys.isEmpty == false {
            do {
                let _ = try userStore.removeValues(for: Array(userStoreInvalidKeys))
            } catch {
                log.error("failed to remove invalid keys \(userStoreInvalidKeys) from DB. \(error.localizedDescription)")
            }
        }
        
        do {
            try userStoreWriteBatch.write()
        } catch {
            log.error("failed to update invitations remote to local server. \(error.localizedDescription)")
        }
        
        ///
        /// Update the ASSET STORE
        /// - sender
        /// - recipients
        /// - thread assets (photo messages)
        ///
        
        let assetStoreWriteBatch = assetStore.writeBatch()
        
        var assetStoreKeysToUpdate = [String: DBSecureSerializableAssetRecipientSharingDetails]()
        var assetStoreInvalidKeys = Set<String>()
        
        do {
            // Remove all senders for the groups
            let senderCondition = KBGenericCondition(
                .beginsWith, value: "sender::"
            ).and(groupIdsCondition)
            
            let _ = try assetStore.removeValues(forKeysMatching: senderCondition)
            
            // Add senders for the groups for each asset
            for (groupId, diff) in groupInfoById {
                guard let groupCreator = diff.groupInfo.createdBy else {
                    continue
                }
                for assetId in diff.descriptorByAssetId.keys {
                    assetStoreWriteBatch.set(value: true, for: [
                        "sender",
                        groupCreator,
                        SHAssetQuality.lowResolution.rawValue,
                        assetId,
                        groupId
                    ].joined(separator: "::"))
                }
            }
            
            ///
            /// Get all the `receiver::` keys to determine which values need update
            ///
            let recipientDetails = try assetStore
                .dictionaryRepresentation(
                    forKeysMatching:
                        KBGenericCondition(
                            .beginsWith, value: "receiver::"
                        )
                        .and(groupIdsCondition)
                    )
                
            for (key, rawVersionDetails) in recipientDetails {
                do {
                    let versionDetails = try DBSecureSerializableAssetRecipientSharingDetails.from(rawVersionDetails)
                    
                    ///
                    /// If the groupId matches, then collect the key and the updated value.
                    /// If not move on
                    ///
                    guard let update = groupInfoById[versionDetails.groupId]?.groupInfo else {
                        continue
                    }
                                        
                    let updatedValue = DBSecureSerializableAssetRecipientSharingDetails(
                        groupId: versionDetails.groupId,
                        groupCreationDate: update.createdAt ?? versionDetails.groupCreationDate,
                        quality: versionDetails.quality,
                        senderEncryptedSecret: versionDetails.senderEncryptedSecret,
                        ephemeralPublicKey: versionDetails.ephemeralPublicKey,
                        publicSignature: versionDetails.publicSignature
                    )
                    
                    assetStoreKeysToUpdate[key] = updatedValue
                } catch {
                    assetStoreInvalidKeys.insert(key)
                }
            }
        } catch {
            log.error("failed to update groupIds from remote to local server. \(error.localizedDescription)")
        }
        
        for (key, update) in assetStoreKeysToUpdate {
            do {
                let serializedData = try NSKeyedArchiver.archivedData(withRootObject: update, requiringSecureCoding: true)
                assetStoreWriteBatch.set(value: serializedData, for: key)
            } catch {
                log.critical("failed to serialize recipient details update for key \(key): \(update). \(error.localizedDescription)")
            }
        }
        
        for (groupId, diff) in groupInfoById {
            
            if let threadId = diff.groupInfo.createdFromThreadId {
                for descriptor in diff.descriptorByAssetId.values {
                    guard let addedAt = diff.groupInfo.createdAt?.iso8601withFractionalSeconds else {
                        log.error("Group information doesn't include creation data. Skipping")
                        continue
                    }
                    
                    let key = "\(SHInteractionAnchor.thread.rawValue)::\(threadId)::\(groupId)::\(descriptor.globalIdentifier)::photoMessage"
                    let assetCreatorId = descriptor.sharingInfo.sharedByUserIdentifier
                    let groupCreatorId = descriptor.sharingInfo.groupInfoById[groupId]?.createdBy
                    let value = DBSecureSerializableConversationThreadAsset(
                        globalIdentifier: descriptor.globalIdentifier,
                        addedByUserIdentifier: groupCreatorId ?? assetCreatorId,
                        addedAt: addedAt,
                        groupId: groupId
                    )
                    do {
                        let serializedData = try NSKeyedArchiver.archivedData(withRootObject: value, requiringSecureCoding: true)
                        assetStoreWriteBatch.set(value: serializedData, for: key)
                    } catch {
                        log.critical("error serializing photoMessage in DB. \(error.localizedDescription)")
                    }
                }
            } else {
                var condition = KBGenericCondition(value: false)
                for assetId in diff.descriptorByAssetId.keys {
                    condition = condition.or(
                        KBGenericCondition(.endsWith, value: "\(groupId)::\(assetId)::photoMessage")
                    )
                }
                
                do {
                    let _ = try assetStore.removeValues(forKeysMatching: condition)
                } catch {
                    log.error("failed to update group id thread id mapping from remote to local server for group \(groupId). \(error.localizedDescription)")
                }
            }
            
            assetStoreWriteBatch.set(value: diff.groupInfo.permissions, for: "\(SHInteractionAnchor.group.rawValue)::\(groupId)::permissions")
        }
        
        if assetStoreInvalidKeys.isEmpty == false {
            do {
                let _ = try assetStore.removeValues(for: Array(assetStoreInvalidKeys))
            } catch {
                log.error("failed to remove invalid keys \(assetStoreInvalidKeys) from DB. \(error.localizedDescription)")
            }
        }
        
        do {
            try assetStoreWriteBatch.write()
        } catch {
            log.error("failed to update groupIds from remote to local server. \(error.localizedDescription)")
        }
        
        completionHandler(.success(()))
    }
    
    func removeGroupIds(_ groupIds: [String],
                        completionHandler: @escaping (Result<Void, Error>) -> Void) {
        guard groupIds.isEmpty == false else {
            completionHandler(.failure(SHHTTPError.ClientError.badRequest("asked to remove groupIds but no groups provided")))
            return
        }
        
        guard let assetStore = SHDBManager.sharedInstance.assetStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        var groupIdCondition = KBGenericCondition(value: false)
        for groupId in groupIds {
            groupIdCondition = groupIdCondition.or(KBGenericCondition(.endsWith, value: groupId))
        }
        
        /// Remove from the **USER STORE**:
        /// 1. Invitations info (start with "invitation::", ends with groupId)
        /// 2. group titles (is "assets-groups::\(groupId)::encryptedTitle")
        let userStoreWriteBatch = userStore.writeBatch()
        for groupId in groupIds {
            userStoreWriteBatch.set(value: nil, for: "\(SHInteractionAnchor.group.rawValue)::\(groupId)::encryptedTitle")
        }
        do {
            try userStoreWriteBatch.write()
            let _ = try userStore.removeValues(forKeysMatching: KBGenericCondition(
                .beginsWith, value: "invitations::"
            ).and(groupIdCondition))
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        /// Remove from the **ASSET STORE**:
        /// 1. Sender info (start with "sender::", ends with groupId)
        /// 2. Recipient info (start with "receiver::", ends with groupId)
        /// 3. Info about thread-groupId linkage ("user-threads::", ends with groupId)
        
        let shareCondition = KBGenericCondition(
            .beginsWith, value: "receiver::"
        ).or(KBGenericCondition(
            .beginsWith, value: "sender::"
        )).or(KBGenericCondition(
            .beginsWith, value: "\(SHInteractionAnchor.thread.rawValue)::"
        ))
        
        let condition = shareCondition.and(groupIdCondition)
        assetStore.removeValues(forKeysMatching: condition) { result in
            if case .failure(let error) = result {
                completionHandler(.failure(error))
            } else {
                completionHandler(.success(()))
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
        
        let prefixCondition: KBGenericCondition
        let assetCondition: KBGenericCondition
        
        let versions = versions ?? SHAssetQuality.all
        if versions.isEmpty {
            prefixCondition = KBGenericCondition(value: true)
        } else {
            var c = KBGenericCondition(value: false)
            for quality in versions {
                c = c
                    .or(KBGenericCondition(.beginsWith, value: quality.rawValue + "::"))
                    .or(KBGenericCondition(.beginsWith, value: "data::" + quality.rawValue + "::"))
            }
            prefixCondition = c
        }
        
        if assetIdentifiersChunk.isEmpty {
            assetCondition = KBGenericCondition(value: true)
        } else {
            var c = KBGenericCondition(value: false)
            for assetIdentifier in assetIdentifiersChunk {
                c = c.or(KBGenericCondition(.contains, value: "::" + assetIdentifier))
            }
            assetCondition = c
        }
        
        log.debug("feching local server asset versions \(versions.map({ $0.rawValue })) chunk \(index) \(String(describing: assetIdentifiersChunk))")
        
        assetStore.dictionaryRepresentation(forKeysMatching: prefixCondition.and(assetCondition)) {
            (result: Result) in
            switch result {
            case .success(let keyValues):
                do {
                    var newResult = partialResult
                    newResult.merge(
                        try SHGenericEncryptedAsset.fromDicts(keyValues),
                        uniquingKeysWith: { (_, newValue) in return newValue }
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
    
    private static func assetVersionDataFile(
        for globalIdentifier: GlobalIdentifier,
        quality: SHAssetQuality
    ) -> URL {
        let assetVersionFolderURL: URL
        
        if #available(iOS 16.0, macOS 13.0, *) {
            assetVersionFolderURL = Self.dataFolderURL
                .appending(path: globalIdentifier)
                .appending(path: quality.rawValue)
        } else {
            assetVersionFolderURL = Self.dataFolderURL
                .appendingPathComponent(globalIdentifier)
                .appendingPathComponent(quality.rawValue)
        }
        
        return assetVersionFolderURL
    }
    
    private func createAssetDataFile(
        globalIdentifier: GlobalIdentifier,
        quality: SHAssetQuality,
        content encryptedData: Data,
        overwriteIfExists: Bool
    ) throws -> URL {
        let versionDataURL = Self.assetVersionDataFile(for: globalIdentifier, quality: quality)
        let assetFolderURL = versionDataURL.deletingLastPathComponent()
        
        let fileManager = FileManager.default
        
        if fileManager.fileExists(atPath: versionDataURL.path) {
            if overwriteIfExists {
                log.debug("a file exists at \(versionDataURL.path). overwriting it")
                try? fileManager.removeItem(at: versionDataURL)
            } else {
                log.debug("a file exists at \(versionDataURL.path). returning it")
                return versionDataURL
            }
        }
        
        do {
            try fileManager.createDirectory(at: assetFolderURL, withIntermediateDirectories: true)
        } catch {
            log.error("failed to create directory at \(assetFolderURL.path). \(error.localizedDescription)")
            throw error
        }
        
        let created = fileManager.createFile(
            atPath: versionDataURL.path,
            contents: encryptedData
        )
        guard created else {
            log.error("failed to create file at \(versionDataURL.path)")
            throw SHLocalServerError.failedToCreateFile
        }
        
        return versionDataURL
    }
    
    private func avatarImageURL(for userId: UserIdentifier) -> (folder: URL, full: URL) {
        let (folderURL, fullURL): (URL, URL)
        if #available(iOS 16.0, macOS 13.0, *) {
            folderURL = Self.dataFolderURL
                .appending(path: "avatars")
            fullURL = folderURL.appending(path: userId)
        } else {
            folderURL = Self.dataFolderURL
                .appendingPathComponent("avatars")
            fullURL = folderURL.appendingPathComponent(userId)
        }
        return (folder: folderURL, full: fullURL)
    }
    
    internal func avatarImage(for user: any SHServerUser) async throws -> Data? {
        let urls = self.avatarImageURL(for: user.identifier)
        return try? Data(contentsOf: urls.full, options: .mappedIfSafe)
    }
    
    internal func saveAvatarImage(data: Data, for user: any SHServerUser) async throws {
        let urls = self.avatarImageURL(for: user.identifier)
        
        let fileManager = FileManager.default
        try? await self.deleteAvatarImage(for: user)
        
        do {
            try fileManager.createDirectory(at: urls.folder, withIntermediateDirectories: true)
        } catch {
            log.error("failed to create directory at \(urls.folder.path). \(error.localizedDescription)")
            throw error
        }
        
        let created = fileManager.createFile(
            atPath: urls.full.path,
            contents: data
        )
        
        guard created else {
            log.error("failed to create file at \(urls.full.path)")
            throw SHLocalServerError.failedToCreateFile
        }
    }
    
    internal func deleteAvatarImages() throws {
        let folderURL: URL
        if #available(iOS 16.0, macOS 13.0, *) {
            folderURL = Self.dataFolderURL.appending(path: "avatars")
        } else {
            folderURL = Self.dataFolderURL.appendingPathComponent("avatars")
        }
        
        try FileManager.default.removeItem(at: folderURL)
    }
    
    internal func deleteAvatarImage(for user: any SHServerUser) async throws {
        let urls = self.avatarImageURL(for: user.identifier)
        
        let fileManager = FileManager.default
        try fileManager.removeItem(at: urls.full)
    }
    
    @available(*, deprecated, message: "force create only makes sense for Remote server")
    func create(assets: [any SHEncryptedAsset],
                fingerprintsById: [GlobalIdentifier: AssetFingerprint],
                groupId: String,
                filterVersions: [SHAssetQuality]?,
                force: Bool,
                completionHandler: @escaping (Result<[SHServerAsset], Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func create(assets: [any SHEncryptedAsset],
                groupId: String,
                createdBy: any SHServerUser,
                createdAt: Date,
                createdFromThreadId: String?,
                permissions: Int?,
                filterVersions: [SHAssetQuality]?,
                overwriteFileIfExists: Bool,
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
                uploadState: .started,
                sharingInfo: SHGenericDescriptorSharingInfo(
                    sharedByUserIdentifier: self.requestor.identifier,
                    groupIdsByRecipientUserIdentifier: [self.requestor.identifier: [groupId]],
                    groupInfoById: [
                        groupId: SHGenericAssetGroupInfo(
                            encryptedTitle: nil,
                            createdBy: createdBy.identifier,
                            createdAt: createdAt,
                            createdFromThreadId: createdFromThreadId,
                            invitedUsersPhoneNumbers: nil,
                            permissions: permissions
                        )
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
                    uploadState: .started,
                    overwriteFileIfExists: overwriteFileIfExists,
                    completionHandler: completionHandler)
    }
    
    func create(assets: [any SHEncryptedAsset],
                descriptorsByGlobalIdentifier: [GlobalIdentifier: any SHAssetDescriptor],
                uploadState: SHAssetDescriptorUploadState,
                overwriteFileIfExists: Bool = false,
                completionHandler: @escaping (Result<[SHServerAsset], Error>) -> ()) {
        guard assets.isEmpty == false else {
            return completionHandler(.success([]))
        }
        
        for asset in assets {
            guard let descriptor = descriptorsByGlobalIdentifier[asset.globalIdentifier],
                  let _ = descriptor.sharingInfo.groupIdsByRecipientUserIdentifier[self.requestor.identifier]
            else {
                completionHandler(.failure(SHHTTPError.ClientError.badRequest("Mismatched assets and descriptors in paramters")))
                return
            }
        }
        
        guard let assetStore = SHDBManager.sharedInstance.assetStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        let writeBatch = assetStore.writeBatch()
        
        /// Thread id to thread asset to save to local db
        var threadAssets = [String: [DBSecureSerializableConversationThreadAsset]]()
        
        for asset in assets {
            guard let descriptor = descriptorsByGlobalIdentifier[asset.globalIdentifier] else {
                log.error("no descriptor provided for asset to create with global identifier \(asset.globalIdentifier)")
                continue
            }
            
            let assetCreatorId = descriptor.sharingInfo.sharedByUserIdentifier
            writeBatch.set(
                value: assetCreatorId,
                for: "creator::" + asset.globalIdentifier
            )
            
            for encryptedVersion in asset.encryptedVersions.values {
                
                if encryptedVersion.quality == .midResolution,
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
                    writeBatch.set(
                        value: nil,
                        for: "\(SHAssetQuality.midResolution.rawValue)::" + asset.globalIdentifier
                    )
                    writeBatch.set(
                        value: nil,
                        for: "data::" + "\(SHAssetQuality.midResolution.rawValue)::" + asset.globalIdentifier
                    )
                    for (groupId, groupInfo) in descriptor.sharingInfo.groupInfoById {
                        let groupCreatorId = groupInfo.createdBy
                        writeBatch.set(
                            value: nil,
                            for: [
                                "sender",
                                groupCreatorId ?? assetCreatorId,
                                SHAssetQuality.midResolution.rawValue,
                                asset.globalIdentifier,
                                groupId
                            ].joined(separator: "::")
                        )
                    }
                    
                    for (recipientUserId, groupIds) in descriptor.sharingInfo.groupIdsByRecipientUserIdentifier {
                        for groupId in groupIds {
                            writeBatch.set(
                                value: nil,
                                for: [
                                    "receiver",
                                    recipientUserId,
                                    SHAssetQuality.midResolution.rawValue,
                                    asset.globalIdentifier,
                                    groupId
                                ].joined(separator: "::")
                            )
                        }
                    }
                    
                    let versionDataURL = Self.assetVersionDataFile(
                        for: asset.globalIdentifier,
                        quality: .midResolution
                    )
                    try? FileManager.default.removeItem(at: versionDataURL)
                }
                
                let versionMetadata = DBSecureSerializableAssetVersionMetadata(
                    globalIdentifier: asset.globalIdentifier,
                    localIdentifier: asset.localIdentifier,
                    quality: encryptedVersion.quality,
                    senderEncryptedSecret: encryptedVersion.encryptedSecret,
                    publicKey: encryptedVersion.publicKeyData,
                    publicSignature: encryptedVersion.publicSignatureData,
                    creationDate: asset.creationDate,
                    uploadState: uploadState
                )
                
                do {
                    let serializedVersionMetadata = try NSKeyedArchiver.archivedData(withRootObject: versionMetadata, requiringSecureCoding: true)
                    writeBatch.set(
                        value: serializedVersionMetadata,
                        for: "\(encryptedVersion.quality.rawValue)::" + asset.globalIdentifier
                    )
                    
                } catch {
                    log.critical("failed to serialize version metadata for asset \(asset.globalIdentifier): \(versionMetadata). \(error.localizedDescription)")
                }
                
                let versionDataURL: URL
                
                do {
                    versionDataURL = try self.createAssetDataFile(
                        globalIdentifier: asset.globalIdentifier,
                        quality: encryptedVersion.quality,
                        content: encryptedVersion.encryptedData,
                        overwriteIfExists: overwriteFileIfExists
                    )
                    log.debug("saved asset data to path \(versionDataURL.path)")
                } catch {
                    continue
                }
                    
                writeBatch.set(
                    value: versionDataURL.absoluteString,
                    for: "data::" + "\(encryptedVersion.quality.rawValue)::" + asset.globalIdentifier
                )
                
                for (groupId, groupInfo) in descriptor.sharingInfo.groupInfoById {
                    let assetCreatorId = descriptor.sharingInfo.sharedByUserIdentifier
                    let groupCreatorId = groupInfo.createdBy
                    writeBatch.set(
                        value: true,
                        for: [
                            "sender",
                            groupCreatorId ?? assetCreatorId,
                            encryptedVersion.quality.rawValue,
                            asset.globalIdentifier,
                            groupId
                        ].joined(separator: "::")
                    )
                }
                
                for (recipientUserId, groupIds) in descriptor.sharingInfo.groupIdsByRecipientUserIdentifier {
                    for groupId in groupIds {
                        let key = [
                            "receiver",
                            recipientUserId,
                            encryptedVersion.quality.rawValue,
                            asset.globalIdentifier,
                            groupId
                        ].joined(separator: "::")
                        
                        
                        let value: DBSecureSerializableAssetRecipientSharingDetails
                        
                        if recipientUserId == self.requestor.identifier {
                            value = DBSecureSerializableAssetRecipientSharingDetails(
                                groupId: groupId,
                                groupCreationDate: descriptor.sharingInfo.groupInfoById[groupId]?.createdAt,
                                quality: encryptedVersion.quality,
                                senderEncryptedSecret: encryptedVersion.encryptedSecret,
                                ephemeralPublicKey: encryptedVersion.publicKeyData,
                                publicSignature: encryptedVersion.publicSignatureData
                            )
                        } else {
                            value = DBSecureSerializableAssetRecipientSharingDetails(
                                groupId: groupId,
                                groupCreationDate: descriptor.sharingInfo.groupInfoById[groupId]?.createdAt,
                                quality: encryptedVersion.quality,
                                senderEncryptedSecret: nil,
                                ephemeralPublicKey: nil,
                                publicSignature: nil
                            )
                        }
                        
                        do {
                            let serializedData = try NSKeyedArchiver.archivedData(
                                withRootObject: value,
                                requiringSecureCoding: true
                            )
                            
                            writeBatch.set(
                                value: serializedData,
                                for: key
                            )
                        } catch {
                            log.critical("failed to serialize recipient info for key \(key): \(value). \(error.localizedDescription)")
                        }
                    }
                }
            }
            
            for (groupId, groupInfo) in descriptor.sharingInfo.groupInfoById {
                if let permissions = groupInfo.permissions {
                    writeBatch.set(value: permissions, for: "\(SHInteractionAnchor.group.rawValue)::\(groupId)::permissions")
                }
                
                if let threadId = groupInfo.createdFromThreadId {
                    let groupCreatorId = descriptor.sharingInfo.groupInfoById[groupId]?.createdBy
                    let assetCreatorId = descriptor.sharingInfo.sharedByUserIdentifier
                    let threadAsset = DBSecureSerializableConversationThreadAsset(
                        globalIdentifier: asset.globalIdentifier,
                        addedByUserIdentifier: groupCreatorId ?? assetCreatorId,
                        addedAt: (groupInfo.createdAt ?? Date()).iso8601withFractionalSeconds,
                        groupId: groupId
                    )
                    if threadAssets[threadId] == nil {
                        threadAssets[threadId] = [threadAsset]
                    } else {
                        threadAssets[threadId]!.append(threadAsset)
                    }
                }
            }
        }
        
        for (threadId, dbValues) in threadAssets {
            guard dbValues.isEmpty else {
                continue
            }
            
            for dbValue in dbValues {
                let key = "\(SHInteractionAnchor.thread.rawValue)::\(threadId)::\(dbValue.groupId)::\(dbValue.globalIdentifier)::photoMessage"
                writeBatch.set(value: dbValue, for: key)
            }
        }
        
        writeBatch.write { (result: Result) in
            switch result {
            case .success():
                var serverAssets = [SHServerAsset]()
                for asset in assets {
                    let descriptor = descriptorsByGlobalIdentifier[asset.globalIdentifier]!
                    for thisUserGroupId in descriptor.sharingInfo.groupIdsByRecipientUserIdentifier[self.requestor.identifier]! {
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
                        
                        let serverAsset = SHServerAsset(
                            globalIdentifier: asset.globalIdentifier,
                            localIdentifier: asset.localIdentifier,
                            createdBy: descriptor.sharingInfo.sharedByUserIdentifier,
                            creationDate: asset.creationDate,
                            groupId: thisUserGroupId,
                            versions: serverAssetVersions
                        )
                        serverAssets.append(serverAsset)
                    }
                }
                
                completionHandler(.success(serverAssets))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    func updateUserGroupInfo(
        basedOn sharingInfoByAssetId: [GlobalIdentifier: any SHDescriptorSharingInfo],
        versions: [SHAssetQuality]? = nil,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        
        guard let assetStore = SHDBManager.sharedInstance.assetStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        self.removeAssetRecipients(
            basedOn: sharingInfoByAssetId.mapValues({ Array($0.groupIdsByRecipientUserIdentifier.keys) }),
            versions: versions
        ) {
            removeResult in
            switch removeResult {
            case .failure(let error):
                completionHandler(.failure(error))
                
            case .success:
            
                do {
                    try SHKGQuery.ingestShareChanges(sharingInfoByAssetId)
                } catch {
                    completionHandler(.failure(error))
                    return
                }
                
                let versions = versions ?? SHAssetQuality.all
                
                let writeBatch = assetStore.writeBatch()
                
                for (globalIdentifier, sharingInfo) in sharingInfoByAssetId {
                    for (recipientUserId, groupIds) in sharingInfo.groupIdsByRecipientUserIdentifier {
                        for groupId in groupIds {
                            guard let groupInfo = sharingInfo.groupInfoById[groupId] else {
                                log.critical("group information missing for group \(groupId) when calling addAssetRecipients(basedOn:versions:completionHandler:)")
                                continue
                            }
                            for version in versions {
                                
                                let key = [
                                    "receiver",
                                    recipientUserId,
                                    version.rawValue,
                                    globalIdentifier,
                                    groupId
                                ].joined(separator: "::")
                                
                                let value = DBSecureSerializableAssetRecipientSharingDetails(
                                    groupId: groupId,
                                    groupCreationDate: groupInfo.createdAt,
                                    quality: version,
                                    senderEncryptedSecret: nil,
                                    ephemeralPublicKey: nil,
                                    publicSignature: nil
                                )
                                
                                do {
                                    let serializedData = try NSKeyedArchiver.archivedData(withRootObject: value, requiringSecureCoding: true)
                                    
                                    writeBatch.set(
                                        value: serializedData,
                                        for: key
                                    )
                                } catch {
                                    log.critical("failed to serialize recipient info for key \(key): \(value). \(error.localizedDescription)")
                                }
                            }
                        }
                    }
                }
                
                writeBatch.write(completionHandler: completionHandler)
            }
        }
    }
    
    func removeAssetRecipients(basedOn userIdsToRemoveFromAssetGid: [GlobalIdentifier: [UserIdentifier]],
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
        
        var removeCondition = KBGenericCondition(value: false)
        
        for (globalIdentifier, recipientIds) in userIdsToRemoveFromAssetGid {
            for recipientId in recipientIds {
                for version in versions ?? SHAssetQuality.all {
                    let partialKey = [
                        "receiver",
                        recipientId,
                        version.rawValue,
                        globalIdentifier
                    ].joined(separator: "::")
                    removeCondition = removeCondition.or(KBGenericCondition(.beginsWith, value: partialKey))
                }
            }
        }
        
        assetStore.removeValues(forKeysMatching: removeCondition) { result in
            switch result {
            case .success:
                completionHandler(.success(()))
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    func uploadAsset(
        with globalIdentifier: GlobalIdentifier,
        versionsDataManifest: [SHAssetQuality: (URL, Data)],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        var errors = [Error]()
        let dispatchGroup = DispatchGroup()
        
        for quality in versionsDataManifest.keys {
            
            dispatchGroup.enter()
            self.markAsset(
                with: globalIdentifier,
                quality: quality,
                as: .completed
            ) { result in
                if case .failure(let error) = result {
                    errors.append(error)
                }
                dispatchGroup.leave()
            }
        }
        
        dispatchGroup.notify(queue: .global()) {
            if errors.isEmpty {
                completionHandler(.success(()))
            } else {
                completionHandler(.failure(errors.first!))
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
        
        let condition = KBGenericCondition(.equal, value: "\(quality.rawValue)::\(assetGlobalIdentifier)")
        assetStore.values(forKeysMatching: condition) {
            
            result in
            switch result {
            
            case .success(let values):
                
                guard values.count == 1,
                      let v = values.first,
                      let rawMetadata = v as? Data
                else {
                    completionHandler(.failure(SHAssetStoreError.noEntries))
                    return
                }
                
                let value: DBSecureSerializableAssetVersionMetadata
                do {
                    value = try DBSecureSerializableAssetVersionMetadata.from(rawMetadata)
                } catch {
                    completionHandler(.failure(error))
                    return
                }
                
                let newValue = DBSecureSerializableAssetVersionMetadata(
                    globalIdentifier: value.globalIdentifier,
                    localIdentifier: value.localIdentifier,
                    quality: value.quality,
                    senderEncryptedSecret: value.senderEncryptedSecret,
                    publicKey: value.publicKey,
                    publicSignature: value.publicSignature,
                    creationDate: value.creationDate,
                    uploadState: state
                )
                
                do {
                    let serializedData = try NSKeyedArchiver.archivedData(withRootObject: newValue, requiringSecureCoding: true)
                    
                    assetStore.set(
                        value: serializedData,
                        for: "\(quality.rawValue)::\(assetGlobalIdentifier)",
                        completionHandler: completionHandler
                    )
                } catch {
                    log.critical("failed to serialize asset metadata \(value). \(error.localizedDescription)")
                }

            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    func share(asset: SHShareableEncryptedAsset,
               asPhotoMessageInThreadId: String?,
               permissions: Int?,
               suppressNotification: Bool = true,
               completionHandler: @escaping (Result<Void, Error>) -> ()) {
        guard let assetStore = SHDBManager.sharedInstance.assetStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        let writeBatch = assetStore.writeBatch()
        
        for sharedVersion in asset.sharedVersions {
            
            let key = [
                "receiver",
                sharedVersion.userPublicIdentifier,
                sharedVersion.quality.rawValue,
                asset.globalIdentifier,
                asset.groupId
           ].joined(separator: "::")
            
            let value = DBSecureSerializableAssetRecipientSharingDetails(
                groupId: asset.groupId,
                groupCreationDate: Date(),
                quality: sharedVersion.quality,
                senderEncryptedSecret: nil,
                ephemeralPublicKey: nil,
                publicSignature: nil
            )
            
            do {
                let serializedData = try NSKeyedArchiver.archivedData(withRootObject: value, requiringSecureCoding: true)
                
                writeBatch.set(
                    value: serializedData,
                    for: key
                )
            } catch {
                log.critical("failed to serialize recipient info for key \(key): \(value). \(error.localizedDescription)")
                completionHandler(.failure(error))
                return
            }
        }
        
        if let asPhotoMessageInThreadId {
            let key = "\(SHInteractionAnchor.thread.rawValue)::\(asPhotoMessageInThreadId)::\(asset.groupId)::\(asset.globalIdentifier)::photoMessage"
            
            let value = DBSecureSerializableConversationThreadAsset(
                globalIdentifier: asset.globalIdentifier,
                addedByUserIdentifier: self.requestor.identifier,
                addedAt: Date().iso8601withFractionalSeconds,
                groupId: asset.groupId
            )
            
            do {
                let serializedData = try NSKeyedArchiver.archivedData(
                    withRootObject: value,
                    requiringSecureCoding: true
                )
                writeBatch.set(value: serializedData, for: key)
            } catch {
                log.critical("failed to serialize thread asset info for key \(key): \(value). \(error.localizedDescription)")
                completionHandler(.failure(error))
                return
            }
            
        }
        
        writeBatch.set(value: permissions, for: "\(SHInteractionAnchor.group.rawValue)::\(asset.groupId)::permissions")
        
        writeBatch.write(completionHandler: completionHandler)
    }
    
    func unshareAll(with userIdentifiers: [UserIdentifier],
                    completionHandler: @escaping (Result<Void, Error>) -> ()) {
        guard let assetStore = SHDBManager.sharedInstance.assetStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        guard userIdentifiers.isEmpty == false else {
            completionHandler(.failure(SHHTTPError.ClientError.badRequest("asked to unshare with users but no users provided")))
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
    
    func unshare(
        assetIdsWithUsers: [GlobalIdentifier: [UserIdentifier]],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        guard let assetStore = SHDBManager.sharedInstance.assetStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        guard assetIdsWithUsers.isEmpty == false else {
            completionHandler(.failure(SHHTTPError.ClientError.badRequest("asked to unshare assets with users but no assets and users provided")))
            return
        }
        
        let dispatchGroup = DispatchGroup()
        var errors = [GlobalIdentifier: Error]()
        
        for (assetId, userPublicIdentifiers) in assetIdsWithUsers {
            
            guard userPublicIdentifiers.isEmpty == false else {
                continue
            }
            
            var condition = KBGenericCondition(value: false)
            for userPublicIdentifier in userPublicIdentifiers {
                for quality in SHAssetQuality.all {
                    condition = condition.or(KBGenericCondition(.beginsWith, value: [
                        "receiver",
                        userPublicIdentifier,
                        quality.rawValue,
                        assetId
                    ].joined(separator: "::")))
                }
            }
            
            assetStore.removeValues(forKeysMatching: condition) { result in
                switch result {
                case .success: break
                case .failure(let err):
                    errors[assetId] = err
                }
            }
        }
        
        dispatchGroup.notify(queue: .global()) {
            if errors.isEmpty {
                completionHandler(.success(()))
            } else {
                log.error("some errors unsharing: \(errors)")
                completionHandler(.failure(SHAssetStoreError.failedToUnshareSomeAssets))
            }
        }
    }
    
    func changeGroupPermission(
        groupId: String,
        permission: Int,
        completionHandler: @escaping (Result<Void, any Error>) -> ()
    ) {
        guard let assetStore = SHDBManager.sharedInstance.assetStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        assetStore.set(
            value: permission,
            for: "\(SHInteractionAnchor.group.rawValue)::\(groupId)::permissions",
            completionHandler: completionHandler
        )
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
                        .or(KBGenericCondition(
                            .equal, value: "\(quality.rawValue)::\(globalIdentifier)"
                        ))
                        .or(KBGenericCondition(
                            .equal, value: "data::\(quality.rawValue)::\(globalIdentifier)"
                        ))
                }
                condition = condition.or(
                    KBGenericCondition(
                        .beginsWith, value: "sender::"
                    ).and(KBGenericCondition(
                        .contains, value: "::" + globalIdentifier
                    ))
                ).or(
                    KBGenericCondition(
                        .beginsWith, value: "receiver::"
                    ).and(KBGenericCondition(
                        .contains, value: "::" + globalIdentifier
                    ))
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
        invitedPhoneNumbers: [String]?,
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
        
        let invitationsKey = "invitations::\(SHInteractionAnchor.thread.rawValue)::\(serverThread.threadId)"
        
        var invitations = [DBSecureSerializableInvitation]()
        for (phoneNumber, timestamp) in serverThread.invitedUsersPhoneNumbers {
            let invitation = DBSecureSerializableInvitation(phoneNumber: phoneNumber, invitedAt: timestamp)
            invitations.append(invitation)
        }
        
        if invitations.isEmpty {
            writeBatch.set(value: nil, for: invitationsKey)
        } else {
            do {
                let data = try NSKeyedArchiver.archivedData(withRootObject: invitations, requiringSecureCoding: true)
                writeBatch.set(value: data, for: invitationsKey)
            } catch {
                writeBatch.set(value: nil, for: invitationsKey)
            }
        }
        
        writeBatch.write { result in
            switch result {
            case .failure(let error):
                completionHandler(.failure(error))
            case .success:
                completionHandler(.success(serverThread))
            }
        }
    }
    
    func updateThread(
        _ threadId: String,
        newName: String?,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        userStore.set(
            value: newName,
            for: "\(SHInteractionAnchor.thread.rawValue)::\(threadId)::name",
            completionHandler: completionHandler
        )
    }
    
    func updateThreadMembers(for threadId: String, _ update: ConversationThreadMembersUpdateDTO, completionHandler: @escaping (Result<Void, Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func updateThreads(
        from remoteThreads: [ConversationThreadUpdate],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        let writeBatch = userStore.writeBatch()
        
        for remoteThread in remoteThreads {
            if let name = remoteThread.name {
                writeBatch.set(value: name, for: "\(SHInteractionAnchor.thread.rawValue)::\(remoteThread.threadId)::name")
            }
            writeBatch.set(value: remoteThread.lastUpdatedAt?.iso8601withFractionalSeconds?.timeIntervalSince1970, for: "\(SHInteractionAnchor.thread.rawValue)::\(remoteThread.threadId)::lastUpdatedAt")
            
            writeBatch.set(value: remoteThread.membersPublicIdentifier, for: "\(SHInteractionAnchor.thread.rawValue)::\(remoteThread.threadId)::membersPublicIdentifiers")
            
            let invitations = remoteThread.invitedUsersPhoneNumbers.map {
                DBSecureSerializableInvitation(phoneNumber: $0.key, invitedAt: $0.value)
            }
            do {
                let data = try NSKeyedArchiver.archivedData(withRootObject: invitations, requiringSecureCoding: true)
                writeBatch.set(value: data, for: "invitations::\(SHInteractionAnchor.thread.rawValue)::\(remoteThread.threadId)")
            } catch {
                log.critical("failed to archive thread invitations data for thread \(remoteThread.threadId) invitations \(remoteThread.invitedUsersPhoneNumbers). \(error.localizedDescription)")
                writeBatch.set(value: nil, for: "invitations::\(SHInteractionAnchor.thread.rawValue)::\(remoteThread.threadId)")
            }
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
        var invitationsCondition: KBGenericCondition
        if let withIdentifiers, withIdentifiers.isEmpty == false {
            var c = KBGenericCondition(value: false)
            for identifier in withIdentifiers {
                c = c.or(
                    KBGenericCondition(
                        .contains,
                        value: "::\(identifier)"
                    )
                )
            }
            condition = KBGenericCondition(
                .beginsWith,
                value: "\(SHInteractionAnchor.thread.rawValue)::"
            ).and(c)
            invitationsCondition = KBGenericCondition(
                .beginsWith,
                value: "invitations::\(SHInteractionAnchor.thread.rawValue)::"
            ).and(c)
        } else {
            condition = KBGenericCondition(
                .beginsWith,
                value: "\(SHInteractionAnchor.thread.rawValue)::"
            )
            invitationsCondition = KBGenericCondition(
                .beginsWith,
                value: "invitations::\(SHInteractionAnchor.thread.rawValue)::"
            )
        }
        
        let kvPairs: KBKVPairs
        let invitationsKVPairs: KBKVPairs
        do {
            kvPairs = try userStore
                .dictionaryRepresentation(forKeysMatching: condition)
            invitationsKVPairs = try userStore
                .dictionaryRepresentation(forKeysMatching: invitationsCondition)
        } catch {
            completionHandler(.failure(error))
            return
        }
        
        var invalidKeys = Set<String>()
        
        var invitationsByThreadId = [String: [DBSecureSerializableInvitation]]()
        for (key, value) in invitationsKVPairs {
            let components = key.components(separatedBy: "::")
            guard components.count == 3 else {
                invalidKeys.insert(key)
                continue
            }
            let threadId = components[2]
            do {
                let invitations = try DBSecureSerializableInvitation.deserializedList(from: value)
                invitationsByThreadId[threadId] = invitations
            } catch {
                invalidKeys.insert(key)
            }
        }
        
        let list = kvPairs.reduce([String: ConversationThreadOutputDTO](), { (partialResult, pair) in
            let (key, value) = pair
            
            let components = key.components(separatedBy: "::")
            guard components.count == 3 else {
                invalidKeys.insert(key)
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
            
            let threadInvitations = invitationsByThreadId[threadId] ?? []
            
            /// … update the field corresponding to the KV pair just processed
            /// in the existing conversation thread, or create a new one with the empty value
            result[threadId] = ConversationThreadOutputDTO(
                threadId: threadId,
                name: name ?? result[threadId]?.name,
                creatorPublicIdentifier: creatorPublicId ?? result[threadId]?.creatorPublicIdentifier,
                membersPublicIdentifier: membersPublicIdentifiers ?? result[threadId]?.membersPublicIdentifier ?? [],
                invitedUsersPhoneNumbers: threadInvitations.reduce([:]) { partialResult, invitation in
                    var result = partialResult
                    result[invitation.phoneNumber] = invitation.invitedAt
                    return result
                },
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
        
        if invalidKeys.isEmpty == false {
            do {
                let _ = try userStore.removeValues(for: Array(invalidKeys))
            } catch {
                log.error("failed to remove invalid keys \(invalidKeys) from DB. \(error.localizedDescription)")
            }
        }
        
        completionHandler(.success(filteredList))
    }
    
    func retrieveGroupDetails(
        forGroup groupId: String,
        completionHandler: @escaping (Result<InteractionsGroupDetailsResponseDTO?, Error>) -> Void
    ) {
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
    
        do {
            let encryptedTitle = try userStore.value(
                for: "\(SHInteractionAnchor.group.rawValue)::\(groupId)::encryptedTitle"
            )
            
            self.retrieveUserEncryptionDetails(
                anchorType: .group,
                anchorId: groupId
            ) {
                result in
                switch result {
                case .success(let details):
                    if let details {
                        completionHandler(.success(InteractionsGroupDetailsResponseDTO(
                            encryptedTitle: encryptedTitle as? String,
                            encryptionDetails: details
                        )))
                    } else {
                        completionHandler(.success(nil))
                    }
                case .failure(let error):
                    completionHandler(.failure(error))
                }
            }
        } catch {
            completionHandler(.failure(error))
        }
    }
    
    func setupGroup(
        groupId: String,
        encryptedTitle: String?,
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
        writeBatch.set(value: encryptedTitle, for: "\(SHInteractionAnchor.group.rawValue)::\(groupId)::encryptedTitle")
        
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
            
            if anchor == .thread {
                guard let assetsStore = SHDBManager.sharedInstance.userStore else {
                    completionHandler(.success(()))
                    return
                }
                
                let photoMessageCondition = KBGenericCondition(
                    .beginsWith,
                    value: "\(SHInteractionAnchor.thread.rawValue)::\(anchorId)::"
                ).and(KBGenericCondition(
                    .endsWith,
                    value: "::photoMessage")
                )
                let _ = try assetsStore.removeValues(forKeysMatching: photoMessageCondition)
            }
            
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
        withUserIds userIdsToMatch: [UserIdentifier],
        and phoneNumbers: [String],
        completionHandler: @escaping (Result<ConversationThreadOutputDTO?, Error>) -> ()
    ) {
        self.listThreads { result in
            switch result {
            case .failure(let error):
                completionHandler(.failure(error))
            case .success(let threads):
                for thread in threads {
                    if Set(thread.membersPublicIdentifier) == Set(userIdsToMatch),
                       Set(thread.invitedUsersPhoneNumbers.keys) == Set(phoneNumbers)
                    {
                        completionHandler(.success(thread))
                        return
                    }
                }
                completionHandler(.success(nil))
            }
        }
    }
    
    private func getGroupIds(for userIdentifiers: [UserIdentifier], in descriptor: any SHAssetDescriptor) -> [String] {
        var groupIds = Set<String>()
        for userIdentifier in userIdentifiers {
            if let groupsForUser = descriptor.sharingInfo.groupIdsByRecipientUserIdentifier[userIdentifier] {
                groupsForUser.forEach { groupIds.insert($0) }
            }
        }
        return Array(groupIds)
    }
    
    func groupIdToThreadIdMapping() throws -> [String: String] {
        guard let assetStore = SHDBManager.sharedInstance.assetStore else {
            throw KBError.databaseNotReady
        }
        
        return try assetStore
            .keys(
                matching: KBGenericCondition(
                    .beginsWith,
                    value: "\(SHInteractionAnchor.thread.rawValue)::"
                ).and(
                    KBGenericCondition(
                        .endsWith,
                        value: "::photoMessage"
                    )
                )
            )
            .reduce([String: String]()) { partialResult, key in
                let components = key.components(separatedBy: "::")
                guard components.count == 5 else {
                    return partialResult
                }
                let threadId = components[1]
                let groupId = components[2]
                
                var result = partialResult
                result[groupId] = threadId
                return result
            }
    }
    
    func permissions(for groupId: String) throws -> Int {
        guard let assetStore = SHDBManager.sharedInstance.assetStore else {
            throw KBError.databaseNotReady
        }
        
        return try assetStore.value(for: "\(SHInteractionAnchor.group.rawValue)::\(groupId)::permissions") as? Int ?? 0
    }
    
    func permissions(for groupIds: [String]) throws -> [String: Int] {
        guard let assetStore = SHDBManager.sharedInstance.assetStore else {
            throw KBError.databaseNotReady
        }
        
        return try assetStore.dictionaryRepresentation(
            forKeysMatching: KBGenericCondition(.endsWith, value: "::permissions")
        ).reduce([String: Int]()) { partialResult, dict in
            let components = dict.key.components(separatedBy: "::")
            guard components.count == 3 else {
                return partialResult
            }
            let groupId = components[1]
            guard let permissions = dict.value as? Int else {
                return partialResult
            }
            
            var result = partialResult
            result[groupId] = permissions
            return result
        }
    }
    
    func getAssets(
        inThread threadId: String,
        completionHandler: @escaping (Result<ConversationThreadAssetsDTO, Error>) -> ()
    ) {
        guard let assetStore = SHDBManager.sharedInstance.assetStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        let photoMessagesById: [String: ConversationThreadAssetDTO]
        var nonPhotoMessagesById = [String: UsersGroupAssetDTO]()
        
        do {
            ///
            /// Get the photo messages in this thread,
            /// previously synced by the method `LocalServer::cache(_:in)`
            ///
            photoMessagesById = try assetStore
                .values(
                    forKeysMatching: KBGenericCondition(
                        .beginsWith,
                        value: "\(SHInteractionAnchor.thread.rawValue)::\(threadId)"
                    ).and(
                        KBGenericCondition(
                            .endsWith,
                            value: "::photoMessage"
                        )
                    )
                )
                .reduce([String: ConversationThreadAssetDTO]()) { partialResult, value in
                    guard let data = value as? Data else {
                        log.critical("unexpected non-data photo message in thread \(threadId)")
                        return partialResult
                    }
                    guard let photoMessage = try? DBSecureSerializableConversationThreadAsset.deserialize(from: data) else {
                        log.critical("failed to decode photo message in thread \(threadId)")
                        return partialResult
                    }
                    
                    let value = photoMessage.toDTO()
                    var result = partialResult
                    result[value.globalIdentifier] = value
                    return result
                }
            
            ///
            /// Retrieve all assets shared with the people in this thread
            /// (regardless if they are photo messages)
            /// then filter out the photo messages
            ///
            let dispatchGroup = DispatchGroup()
            var nonPhotoMessagesError: Error? = nil
            
            dispatchGroup.enter()
            self.listThreads(withIdentifiers: [threadId]) {
                listThreadResult in
                switch listThreadResult {
                case .success(let threads):
                    guard let thread = threads.first else {
                        nonPhotoMessagesError = SHLocalServerError.threadNotPresent(threadId)
                        dispatchGroup.leave()
                        return
                    }
                    
                    do {
                        let assetIdsTuples = try SHKGQuery.assetGlobalIdentifiers(
                            amongst: thread.membersPublicIdentifier,
                            requestingUserId: self.requestor.identifier
                        )
                        
                        for (assetId, predRecipients) in assetIdsTuples {
                            if let senderId = predRecipients
                                .filter({ $0.0 == .shares })
                                .map({ $0.1 })
                                .first
                            {
                                let groupAsset = UsersGroupAssetDTO(
                                    globalIdentifier: assetId,
                                    addedByUserIdentifier: senderId,
                                    addedAt: Date().iso8601withFractionalSeconds
                                )
                                if photoMessagesById[assetId] == nil {
                                    nonPhotoMessagesById[assetId] = groupAsset
                                }
                            }
                        }
                    } catch {
                        log.error("failed to retrieve thread \(threadId) assets from graph. \(error.localizedDescription)")
                        nonPhotoMessagesError = error
                        dispatchGroup.leave()
                    }
                    
                case .failure(let error):
                    log.error("failed to retrieve thread \(threadId) from local server. Failed to calculate non-photomessage assets. \(error.localizedDescription)")
                    nonPhotoMessagesError = error
                    dispatchGroup.leave()
                }
            }
            
            dispatchGroup.notify(queue: .global()) {
                if let nonPhotoMessagesError {
                    completionHandler(.failure(nonPhotoMessagesError))
                } else {
                    let result = ConversationThreadAssetsDTO(
                        photoMessages: Array(photoMessagesById.values),
                        otherAssets: Array(nonPhotoMessagesById.values)
                    )
                    completionHandler(.success(result))
                }
            }
            
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
    
    private func groupPhoneNumberInvitations(from dict: [String: Any?]) throws -> [String: [String: String]] {
        var invitationsByGroupId = [String: [String: String]]()
        var malformedInvitationKeys = Set<String>()
        
        for (key, value) in dict {
            let keyComponents = key.components(separatedBy: "::")
            guard keyComponents.count == 3 else {
                malformedInvitationKeys.insert(key)
                continue
            }
            
            if let value {
                let dbInvitations = try DBSecureSerializableInvitation.deserializedList(from: value)
                
                let groupId = keyComponents[2]
                
                invitationsByGroupId[groupId] = dbInvitations.reduce([String: String]()) {
                    partialResult, item in
                    var result = partialResult
                    result[item.phoneNumber] = item.invitedAt
                    return result
                }
            }
        }
        
        if malformedInvitationKeys.isEmpty == false {
            do {
                try SHDBManager.sharedInstance.userStore?.removeValues(for: Array(malformedInvitationKeys))
            } catch {}
        }
        
        return invitationsByGroupId
    }
    
    private func groupPhoneNumberInvitations() throws -> [String: [String: String]] {
        
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            throw KBError.databaseNotReady
        }
        
        do {
            let dict = try userStore.dictionaryRepresentation(
                forKeysMatching: KBGenericCondition(.beginsWith, value: "invitations::\(SHInteractionAnchor.group.rawValue)")
            )
            
            return try groupPhoneNumberInvitations(from: dict)
        } catch {
            log.error("failed to fetch user invitations. \(error.localizedDescription)")
            throw error
        }
    }
    
    private func groupPhoneNumberInvitations(
        completionHandler: @escaping (Result<[String: [String: String]], Error>) -> ()
    ) {
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        userStore.dictionaryRepresentation(
            forKeysMatching: KBGenericCondition(.beginsWith, value: "invitations::\(SHInteractionAnchor.group.rawValue)")
        ) {
            invitationsResult in
            switch invitationsResult {
                
            case .success(let dict):
                do {
                    let invitationsByGroupId = try groupPhoneNumberInvitations(from: dict)
                    completionHandler(.success(invitationsByGroupId))
                } catch {
                    completionHandler(.failure(error))
                }
                
            case .failure(let error):
                log.error("failed to fetch user invitations. \(error.localizedDescription)")
                completionHandler(.failure(error))
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
        var invitationsByGroupId = [String: [String: String]]()
        
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
        
        dispatchGroup.enter()
        self.groupPhoneNumberInvitations { result in
            switch result {
            case .success(let dict):
                invitationsByGroupId = dict
                
                dict.keys.forEach { allGroupIds.insert($0) }
            case .failure:
                break
            }
            
            dispatchGroup.leave()
        }
        
        dispatchGroup.notify(queue: .global()) {
            var groupSummaryById = [String: InteractionsGroupSummaryDTO]()
            
            do {
                let groupEncryptedTitles = try self.getEncryptedTitles(for: Array(allGroupIds))
                
                for groupId in allGroupIds {
                    let groupSummary = InteractionsGroupSummaryDTO(
                        numComments: numCommentsByGroupId[groupId] ?? 0,
                        encryptedTitle: groupEncryptedTitles[groupId],
                        reactions: reactionsByGroupId[groupId] ?? [],
                        invitedUsersPhoneNumbers: invitationsByGroupId[groupId] ?? [:]
                    )
                    
                    groupSummaryById[groupId] = groupSummary
                }
                
                completionHandler(.success(groupSummaryById))
            } catch {
                log.critical("failed to retrieve encrypted titles for \(allGroupIds). \(error.localizedDescription)")
                completionHandler(.failure(error))
            }
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
        
        guard let userStore = SHDBManager.sharedInstance.userStore else {
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
                
                userStore.value(for: "invitations::\(SHInteractionAnchor.group.rawValue)::\(groupId)") {
                    invitationsResult in
                    
                    let invitedPhoneNumbers: [String: String]
                    
                    switch invitationsResult {
                    case .success(let maybeValue):
                        if let value = maybeValue,
                           let dbInvitations = try? DBSecureSerializableInvitation.deserializedList(from: value) {
                            invitedPhoneNumbers = dbInvitations.reduce([String: String]()) {
                                partialResult, item in
                                var result = partialResult
                                result[item.phoneNumber] = item.invitedAt
                                return result
                            }
                        } else {
                            invitedPhoneNumbers = [:]
                        }
                    case .failure(let error):
                        log.error("failed to fetch user invitations. \(error.localizedDescription)")
                        invitedPhoneNumbers = [:]
                    }
                    
                    do {
                        let encryptedTitle = try self.getEncryptedTitles(for: [groupId])[groupId]
                        
                        let response = InteractionsGroupSummaryDTO(
                            numComments: numMessages,
                            encryptedTitle: encryptedTitle,
                            reactions: reactions,
                            invitedUsersPhoneNumbers: invitedPhoneNumbers
                        )
                        completionHandler(.success(response))
                    } catch {
                        log.critical("failed to retrieve encrypted titles for \(groupId). \(error.localizedDescription)")
                        completionHandler(.failure(error))
                    }
                }
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
        
        let reactions = reactions.filter({
            $0.senderPublicIdentifier == nil
        })
        
        guard reactions.isEmpty == false else {
            completionHandler(.failure(SHHTTPError.ClientError.badRequest("requested to add reactions but no reactions provided, or no sender information in the reactions provided")))
            return
        }
        
        var deleteCondition = KBGenericCondition(value: false)
        for reaction in reactions {
            deleteCondition = deleteCondition
                .or(
                    KBGenericCondition(.beginsWith, value: "\(anchorType.rawValue)::\(anchorId)::\(reaction.senderPublicIdentifier!)")
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
    
    func invite(_ phoneNumbers: [String], to groupId: String, completionHandler: @escaping (Result<Void, Error>) -> ()) {
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        guard phoneNumbers.isEmpty == false else {
            completionHandler(.failure(SHHTTPError.ClientError.badRequest("empty phone number list to invite")))
            return
        }
        
        let key = "invitations::\(SHInteractionAnchor.group.rawValue)::\(groupId)"
        
        do {
            var newInvitations = [String: String]()
            
            let now = Date().iso8601withFractionalSeconds
            for phoneNumber in phoneNumbers {
                newInvitations[phoneNumber] = now
            }
            
            if let data = try userStore.value(for: key) {
                let dbInvitations = try DBSecureSerializableInvitation.deserializedList(from: data)
                
                for item in dbInvitations {
                    newInvitations[item.phoneNumber] = item.invitedAt
                }
            }
            
            let data = try NSKeyedArchiver.archivedData(
                withRootObject: newInvitations.map({ DBSecureSerializableInvitation(phoneNumber: $0.key, invitedAt: $0.value) }),
                requiringSecureCoding: true
            )
            try userStore.set(value: data, for: key)
            
            completionHandler(.success(()))
            
        } catch {
            completionHandler(.failure(error))
        }
    }
    
    func uninvite(_ phoneNumbers: [String], from groupId: String, completionHandler: @escaping (Result<Void, Error>) -> ()) {
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        guard phoneNumbers.isEmpty == false else {
            completionHandler(.failure(SHHTTPError.ClientError.badRequest("empty phone number list to uninvite")))
            return
        }
        
        let key = "invitations::\(SHInteractionAnchor.group.rawValue)::\(groupId)"
        
        do {
            var newInvitations = [String: String]()
            
            if let value = try userStore.value(for: key) {
                let dbInvitations = try DBSecureSerializableInvitation.deserializedList(from: value)
                
                for item in dbInvitations {
                    newInvitations[item.phoneNumber] = item.invitedAt
                }
            }
            
            for phoneNumber in phoneNumbers {
                newInvitations.removeValue(forKey: phoneNumber)
            }
            
            if newInvitations.count > 0 {
                let data = try NSKeyedArchiver.archivedData(
                    withRootObject: newInvitations.map({
                        DBSecureSerializableInvitation(phoneNumber: $0.key, invitedAt: $0.value)
                    }),
                    requiringSecureCoding: true
                )
                try userStore.set(value: data, for: key)
            } else {
                try userStore.removeValue(for: key)
            }
            
            completionHandler(.success(()))
            
        } catch {
            completionHandler(.failure(error))
        }
    }
}
