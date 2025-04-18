import KnowledgeBase
import Foundation

public enum SHKGPredicate: String {
    case attemptedShare = "attemptedShare"
    case localAssetIdEquivalent = "localAssetIdEquivalent"
    case shares = "shares"
    case sharedWith = "sharedWith"
}

var UserIdToAssetGidSharedByCache = [UserIdentifier: Set<GlobalIdentifier>]()
var UserIdToAssetGidSharedWithCache = [UserIdentifier: Set<GlobalIdentifier>]()

public struct SHKGQuery {
    
    private static let readWriteGraphQueue = DispatchQueue(label: "SHKGQuery.readWrite", attributes: .concurrent)
    
    internal static func ingest(_ descriptors: [any SHAssetDescriptor], receiverUserId: UserIdentifier) throws {
        var errors = [Error]()
        
        // TODO: We need support for writebatch (transaction) in KGGraph. DB writes in a for loop is never a good idea
        for descriptor in descriptors {
            do {
                var allReceivers = Set(descriptor.sharingInfo.groupIdsByRecipientUserIdentifier.keys)
                allReceivers.insert(receiverUserId)
                try self.ingestShare(
                    of: descriptor.globalIdentifier,
                    from: descriptor.sharingInfo.sharedByUserIdentifier,
                    to: Array(allReceivers)
                )
            } catch {
                errors.append(error)
            }
        }
        
        if errors.isEmpty == false {
            throw errors.first!
        }
    }
    
    internal static func ingest(
        _ conversationThreadAssets: ConversationThreadAssetsDTO,
        in thread: ConversationThreadOutputDTO
    ) throws {
        try readWriteGraphQueue.sync(flags: .barrier) {
            
            guard let graph = SHDBManager.sharedInstance.graph else {
                throw KBError.databaseNotReady
            }
            
            guard let userStore = SHDBManager.sharedInstance.userStore else {
                throw KBError.databaseNotReady
            }
            let writeBatch = userStore.writeBatch()
            
            for photoMessage in conversationThreadAssets.photoMessages {
                let data = try NSKeyedArchiver.archivedData(
                    withRootObject: DBSecureSerializableConversationThreadAsset.fromDTO(photoMessage),
                    requiringSecureCoding: true
                )
                writeBatch.set(
                    value: data,
                    for: "\(SHInteractionAnchor.thread.rawValue)::\(thread.threadId)::assets::photoMessage",
                    timestamp: photoMessage.addedAt.iso8601withFractionalSeconds ?? Date()
                )
                
                try self.ingestShare(
                    of: photoMessage.globalIdentifier,
                    from: photoMessage.addedByUserIdentifier,
                    to: thread.membersPublicIdentifier,
                    in: graph
                )
            }
            
            for otherAsset in conversationThreadAssets.otherAssets {
                let data = try NSKeyedArchiver.archivedData(
                    withRootObject: DBSecureSerializableUserGroupAsset.fromDTO(otherAsset),
                    requiringSecureCoding: true
                )
                writeBatch.set(
                    value: data,
                    for: "\(SHInteractionAnchor.thread.rawValue)::\(thread.threadId)::assets::nonPhotoMessage",
                    timestamp: otherAsset.addedAt.iso8601withFractionalSeconds ?? Date()
                )
                try self.ingestShare(
                    of: otherAsset.globalIdentifier,
                    from: otherAsset.addedByUserIdentifier,
                    to: thread.membersPublicIdentifier,
                    in: graph
                )
            }
            
            try writeBatch.write()
        }
    }
    
    internal static func ingestProvisionalShare(
        of assetIdentifier: GlobalIdentifier,
        localIdentifier: LocalIdentifier?,
        from senderUserId: UserIdentifier,
        to receiverUserIds: [UserIdentifier]
    ) throws {
        let receiverUserIds = Array(Set(receiverUserIds))
        
        try readWriteGraphQueue.sync(flags: .barrier) {
            guard let graph = SHDBManager.sharedInstance.graph else {
                throw KBError.databaseNotReady
            }
            
            let kgSender = graph.entity(withIdentifier: senderUserId)
            let kgAsset = graph.entity(withIdentifier: assetIdentifier)
            
            log.debug("[sh-kg] adding triple <user=\(kgSender.identifier), \(SHKGPredicate.attemptedShare.rawValue), asset=\(kgAsset.identifier)>")
            try kgSender.link(to: kgAsset, withPredicate: SHKGPredicate.attemptedShare.rawValue)
            
            if let localIdentifier {
                let kgCorrespondingLocalAsset = graph.entity(withIdentifier: localIdentifier)
                log.debug("[sh-kg] adding triple <asset=\(kgAsset.identifier), \(SHKGPredicate.localAssetIdEquivalent.rawValue), localAsset=\(kgCorrespondingLocalAsset.identifier)>")
                try kgAsset.link(to: kgCorrespondingLocalAsset, withPredicate: SHKGPredicate.localAssetIdEquivalent.rawValue)
            }
            
            for userId in receiverUserIds {
                if userId == senderUserId {
                    continue
                }
                let kgOtherUser = graph.entity(withIdentifier: userId)
                try kgAsset.link(to: kgOtherUser, withPredicate: SHKGPredicate.sharedWith.rawValue)
                log.debug("[sh-kg] adding triple <asset=\(kgAsset.identifier), \(SHKGPredicate.sharedWith.rawValue), user=\(kgOtherUser.identifier)>")
            }
        }
    }
    
    private static func ingestShare(
        of assetIdentifier: GlobalIdentifier,
        from senderUserId: UserIdentifier,
        to receiverUserIds: [UserIdentifier],
        in graph: KBKnowledgeStore
    ) throws {
        
        let receiverUserIds = Array(Set(receiverUserIds))
        
        let kgSender = graph.entity(withIdentifier: senderUserId)
        let kgAsset = graph.entity(withIdentifier: assetIdentifier)
        
        log.debug("[sh-kg] adding triple <user=\(kgSender.identifier), \(SHKGPredicate.shares.rawValue), asset=\(kgAsset.identifier)>")
        try kgSender.link(to: kgAsset, withPredicate: SHKGPredicate.shares.rawValue)

        let tripleCondition = KBTripleCondition(subject: kgSender.identifier, predicate: SHKGPredicate.attemptedShare.rawValue, object: kgAsset.identifier)
        log.debug("[sh-kg] removing triples matching <user=\(kgSender.identifier), \(SHKGPredicate.attemptedShare.rawValue), \(kgAsset.identifier)>")
        try graph.removeTriples(matching: tripleCondition)
        
        if let _ = UserIdToAssetGidSharedByCache[senderUserId] {
            UserIdToAssetGidSharedByCache[senderUserId]!.insert(assetIdentifier)
        } else {
            UserIdToAssetGidSharedByCache[senderUserId] = [assetIdentifier]
        }
        
        for userId in receiverUserIds {
            if userId == senderUserId {
                continue
            }
            let kgOtherUser = graph.entity(withIdentifier: userId)
            try kgAsset.link(to: kgOtherUser, withPredicate: SHKGPredicate.sharedWith.rawValue)
            log.debug("[sh-kg] adding triple <asset=\(kgAsset.identifier), \(SHKGPredicate.sharedWith.rawValue), user=\(kgOtherUser.identifier)>")
            
            if let _ = UserIdToAssetGidSharedWithCache[userId] {
                UserIdToAssetGidSharedWithCache[userId]!.insert(assetIdentifier)
            } else {
                UserIdToAssetGidSharedWithCache[userId] = [assetIdentifier]
            }
        }
    }
    
    internal static func ingestShareChanges(
        _ sharingInfoByAssetId: [GlobalIdentifier: any SHDescriptorSharingInfo]
    ) throws {
        var errors = [Error]()
        
        do {
            try SHKGQuery.removeSharingInformation(basedOn: sharingInfoByAssetId.mapValues({ Array($0.groupIdsByRecipientUserIdentifier.keys) }))
            
            try readWriteGraphQueue.sync(flags: .barrier) {
                guard let graph = SHDBManager.sharedInstance.graph else {
                    throw KBError.databaseNotReady
                }
                
                for (assetIdentifier, sharingInfo) in sharingInfoByAssetId {
                    
                    var senderReceiverIds = [UserIdentifier: [UserIdentifier]]()
                    
                    for recipientId in sharingInfo.groupIdsByRecipientUserIdentifier.keys {
                        let senderId = sharingInfo.sharedByUserIdentifier
                        if senderReceiverIds[senderId] == nil {
                            senderReceiverIds[senderId] = [recipientId]
                        } else {
                            senderReceiverIds[senderId]!.append(recipientId)
                        }
                    }
                    
                    for (senderId, recipientIds) in senderReceiverIds {
                        do {
                            try SHKGQuery.ingestShare(
                                of: assetIdentifier,
                                from: senderId,
                                to: recipientIds,
                                in: graph
                            )
                        } catch {
                            log.critical("[KG] failed to ingest descriptor for assetGid=\(assetIdentifier) into the graph")
                            errors.append(error)
                        }
                    }
                }
            }
        } catch {
            log.critical("[KG] graph DB not ready")
        }
        
        if errors.isEmpty == false {
            throw errors.first!
        }
    }
    
    internal static func ingestShare(
        of assetIdentifier: GlobalIdentifier,
        from senderUserId: UserIdentifier,
        to receiverUserIds: [UserIdentifier]
    ) throws {
        var errors = [Error]()
        
        do {
            try readWriteGraphQueue.sync(flags: .barrier) {
                guard let graph = SHDBManager.sharedInstance.graph else {
                    throw KBError.databaseNotReady
                }
                
                try SHKGQuery.ingestShare(
                    of: assetIdentifier,
                    from: senderUserId,
                    to: receiverUserIds,
                    in: graph
                )
            }
        } catch {
            log.critical("[KG] failed to ingest descriptor for assetGid=\(assetIdentifier) into the graph")
            errors.append(error)
        }
        
        if errors.isEmpty == false {
            throw errors.first!
        }
    }
    
    internal static func removeAssets(with globalIdentifiers: [GlobalIdentifier]) throws {
        let globalIdentifiers = Array(Set(globalIdentifiers))
        
        let removeGidsFromCache = {
            (cache: inout [UserIdentifier: Set<GlobalIdentifier>]) in
            let userIds = Array(cache.keys)
            for userId in userIds {
                if let cachedValue = cache[userId] {
                    let newAssetsGids = cachedValue.subtracting(globalIdentifiers)
                    if newAssetsGids.isEmpty {
                        cache.removeValue(forKey: userId)
                    } else {
                        cache[userId] = newAssetsGids
                    }
                }
            }
        }
        
        // TODO: Support writebatch in KnowledgeGraph
        try readWriteGraphQueue.sync(flags: .barrier) {
            
            /// Invalidate caches
            removeGidsFromCache(&UserIdToAssetGidSharedByCache)
            removeGidsFromCache(&UserIdToAssetGidSharedWithCache)
            
            guard let graph = SHDBManager.sharedInstance.graph else {
                throw KBError.databaseNotReady
            }
            try globalIdentifiers.forEach({
                let assetEntity = graph.entity(withIdentifier: $0)
                try assetEntity.remove()
                log.debug("[sh-kg] removing entity <asset=\(assetEntity.identifier)>")
            })
        }
    }
    
    internal static func removeUsers(with userIdentifiers: [UserIdentifier]) throws {
        let userIdentifiers = Array(Set(userIdentifiers))
     
        // TODO: Support writebatch in KnowledgeGraph
        try readWriteGraphQueue.sync(flags: .barrier) {
            
            /// Invalidate cache
            userIdentifiers.forEach({ uid in
                UserIdToAssetGidSharedByCache.removeValue(forKey: uid)
                UserIdToAssetGidSharedWithCache.removeValue(forKey: uid)
            })
            
            guard let graph = SHDBManager.sharedInstance.graph else {
                throw KBError.databaseNotReady
            }
            for userId in userIdentifiers {
                try graph.removeEntity(userId)
                log.debug("[sh-kg] removing entity <user=\(userId)>")
            }
        }
    }
    
    internal static func deepClean() throws {
        try readWriteGraphQueue.sync(flags: .barrier) {
            
            /// Invalidate cache
            UserIdToAssetGidSharedByCache.removeAll()
            UserIdToAssetGidSharedWithCache.removeAll()
            
            guard let graph = SHDBManager.sharedInstance.graph else {
                throw KBError.databaseNotReady
            }
            let _ = try graph.removeAll()
            log.debug("[sh-kg] removing all triples")
        }
    }
    
    /// Retrieve the asset identifiers and their sender, for senders that match the first set of users, and (optionally) recipients that match the second set.
    /// - Parameters:
    ///   - userIdentifiers: the set of senders to match when retrieving assets
    ///   - recipientIdentifiers: if not nil, returns only the asset that are shared with at least one of these users
    ///   - filterOutInProgress: whether or not to consider the ones in progress
    /// - Returns: the asset identifiers and their sender
    public static func assetGlobalIdentifiers(
        sharedBy userIdentifiers: [UserIdentifier],
        with recipientIdentifiers: [UserIdentifier]?,
        filterOutInProgress: Bool = true
    ) throws -> [GlobalIdentifier: UserIdentifier] {
        guard let graph = SHDBManager.sharedInstance.graph else {
            throw KBError.databaseNotReady
        }
        
        let userIdentifiers = Array(Set(userIdentifiers))
        let recipientIdentifiers = recipientIdentifiers == nil ? nil : Set(recipientIdentifiers!)
        
        // TODO: Support this query with variables in the KB framework, instead of merging in memory
        
        let filterAssetsRecipients: ([GlobalIdentifier: UserIdentifier]) throws -> [GlobalIdentifier: UserIdentifier] = {
            dict in
            
            guard let recipientIdentifiers, recipientIdentifiers.isEmpty == false else {
                return dict
            }
            
            // TODO: Filtering with complex triple conditions throws an SQL parser error
            let triplesSharedWith = try graph.triples(matching: KBTripleCondition(
                subject: nil, predicate: SHKGPredicate.sharedWith.rawValue, object: nil
            ))
            
            var filteredDict = [GlobalIdentifier: UserIdentifier]()
            for (gid, senderId) in dict {
                let recipientIdsForThisAsset = triplesSharedWith.filter({ $0.subject == gid }).map({ $0.object })
                if recipientIdentifiers.intersection(recipientIdsForThisAsset).isEmpty == false {
                    filteredDict[gid] = senderId
                }
            }
            return filteredDict
        }
        
        var assetsToUser = [GlobalIdentifier: UserIdentifier]()
        var sharedByUsersCondition = KBTripleCondition(value: false)
        
        var usersIdsToSearch = [UserIdentifier]()
        for userId in userIdentifiers {
            if let assetIdsSharedBy = UserIdToAssetGidSharedByCache[userId] {
                assetIdsSharedBy.forEach({
                    if let existing = assetsToUser[$0], userId != existing {
                        log.critical("found an asset marked as shared by more than one user: \([existing, userId])")
                    }
                    assetsToUser[$0] = userId
                })
            } else {
                usersIdsToSearch.append(userId)
            }
        }
        
        guard usersIdsToSearch.count > 0 else {
            return try filterAssetsRecipients(assetsToUser)
        }
        
        for userId in usersIdsToSearch {
            sharedByUsersCondition = sharedByUsersCondition.or(
                KBTripleCondition(
                    subject: userId,
                    predicate: SHKGPredicate.shares.rawValue,
                    object: nil
                )
            )
            if filterOutInProgress == false {
                sharedByUsersCondition = sharedByUsersCondition.or(
                    KBTripleCondition(
                        subject: userId,
                        predicate: SHKGPredicate.attemptedShare.rawValue,
                        object: nil
                    )
                )
            }
        }
        
        var triples = [KBTriple]()
        try readWriteGraphQueue.sync(flags: .barrier) {
            triples = try graph.triples(matching: sharedByUsersCondition)
            
            for triple in triples {
                let assetId = triple.object
                if let existing = assetsToUser[assetId] {
                    if existing != triple.subject {
                        log.critical("found an asset marked as shared by more than one user: \([existing, triple.subject])")
                    }
                }
                assetsToUser[assetId] = triple.subject
                
                if let _ = UserIdToAssetGidSharedByCache[triple.subject] {
                    UserIdToAssetGidSharedByCache[triple.subject]!.insert(triple.object)
                } else {
                    UserIdToAssetGidSharedByCache[triple.subject] = [triple.object]
                }
            }
        }
        
        return try filterAssetsRecipients(assetsToUser)
    }
    
    /// Retrieve the asset identifiers and their recipients, for recipients that match the first set of users, and (optionally) recipients that match the second set.
    /// - Parameters:
    ///   - userIdentifiers: the set of recipients to match when retrieving assets
    ///   - by: if not nil, returns only the asset that are shared by one of these users
    /// - Returns: the asset identifiers and their sender
    public static func assetGlobalIdentifiers(
        sharedWith userIdentifiers: [UserIdentifier],
        by senderIdentifier: UserIdentifier?
    ) throws -> [GlobalIdentifier: Set<UserIdentifier>] {
        guard let graph = SHDBManager.sharedInstance.graph else {
            throw KBError.databaseNotReady
        }
        
        let userIdentifiers = Array(Set(userIdentifiers))
        
        // TODO: Support this query with variables in the KB framework, instead of merging in memory
        
        let filterAssetsSender: ([GlobalIdentifier: Set<UserIdentifier>]) throws -> [GlobalIdentifier: Set<UserIdentifier>] = {
            dict in
            
            guard let senderIdentifier else {
                return dict
            }
            
            let senderCondition = KBTripleCondition(
                subject: senderIdentifier,
                predicate: SHKGPredicate.shares.rawValue,
                object: nil
            )
            
            let triplesMatchingSender = try graph.triples(matching: senderCondition)
            var filteredDict = [GlobalIdentifier: Set<UserIdentifier>]()
            for (gid, usersSet) in dict {
                let allSendersForThisAsset = triplesMatchingSender
                    .filter({ $0.object == gid })
                    .map({ $0.subject })
                
                if allSendersForThisAsset.contains(senderIdentifier) {
                    filteredDict[gid] = usersSet
                }
            }
            return filteredDict
        }
        
        var assetsToUsers = [GlobalIdentifier: Set<UserIdentifier>]()
        var sharedWithUsersCondition = KBTripleCondition(value: false)
        
        var usersIdsToSearch = [UserIdentifier]()
        
        readWriteGraphQueue.sync(flags: .barrier) {
            for userId in Set(userIdentifiers) {
                if let assetIdsSharedWith = UserIdToAssetGidSharedWithCache[userId] {
                    assetIdsSharedWith.forEach({
                        if assetsToUsers[$0] == nil {
                            assetsToUsers[$0] = [userId]
                        } else {
                            assetsToUsers[$0]!.insert(userId)
                        }
                    })
                } else {
                    usersIdsToSearch.append(userId)
                }
            }
        }
        
        guard usersIdsToSearch.count > 0 else {
            return try filterAssetsSender(assetsToUsers)
        }
        
        for userId in usersIdsToSearch {
            sharedWithUsersCondition = sharedWithUsersCondition.or(
                KBTripleCondition(
                    subject: nil,
                    predicate: SHKGPredicate.sharedWith.rawValue,
                    object: userId
                )
            )
        }
        
        var triples = [KBTriple]()
        try readWriteGraphQueue.sync(flags: .barrier) {
            triples = try graph.triples(matching: sharedWithUsersCondition)
            
            for triple in triples {
                let assetId = triple.subject
                if let _ = assetsToUsers[assetId] {
                    assetsToUsers[assetId]!.insert(triple.object)
                } else {
                    assetsToUsers[assetId] = [triple.object]
                }
                
                if let _ = UserIdToAssetGidSharedWithCache[triple.object] {
                    UserIdToAssetGidSharedWithCache[triple.object]!.insert(triple.subject)
                } else {
                    UserIdToAssetGidSharedWithCache[triple.object] = [triple.subject]
                }
            }
        }
        
        return try filterAssetsSender(assetsToUsers)
    }
    
    /// Assets etiher shared by or shared with the provided users are added to the resulting map.
    /// **TODO: Support this query with variables in the KB framework, instead of merging in memory**
    ///
    /// - Parameters:
    ///   - userIdentifiers: the list of user identifiers to search in the index
    ///   - filterOutInProgress: whether or not the "in progress" shares should be considered
    /// - Returns: the map of global identifiers to the users involved
    /// 
    public static func assetGlobalIdentifiers(
        amongst userIdentifiers: [UserIdentifier],
        requestingUserId: UserIdentifier,
        filterOutInProgress: Bool = true
    ) throws -> [GlobalIdentifier: [(SHKGPredicate, UserIdentifier)]] {
        
        let userIdentifiers = Array(Set(userIdentifiers))
        
        var assetsToUsers = [GlobalIdentifier: [(SHKGPredicate, UserIdentifier)]]()
        
        // TODO: Support this query with variables in the KB framework, instead of merging in memory
        
        let sharedBy = try self.assetGlobalIdentifiers(
            sharedBy: userIdentifiers,
            with: [requestingUserId],
            filterOutInProgress: filterOutInProgress
        )
        let sharedWith = try self.assetGlobalIdentifiers(
            sharedWith: userIdentifiers,
            by: requestingUserId
        )
        
        for (gid, uids) in sharedWith {
            uids.forEach({ uid in
                if let _ = assetsToUsers[gid] {
                    assetsToUsers[gid]!.append((SHKGPredicate.sharedWith, uid))
                } else {
                    assetsToUsers[gid] = [(SHKGPredicate.sharedWith, uid)]
                }
            })
        }
        for (gid, uid) in sharedBy {
            if let _ = assetsToUsers[gid] {
                assetsToUsers[gid]!.append((SHKGPredicate.shares, uid))
            } else {
                assetsToUsers[gid] = [(SHKGPredicate.sharedWith, uid)]
            }
        }
        
        return assetsToUsers
    }
    
    public static func usersConnectedTo(
        assets globalIdentifiers: [GlobalIdentifier],
        filterOutInProgress: Bool = true
    ) throws -> [GlobalIdentifier: [(SHKGPredicate, UserIdentifier)]] {
        guard let graph = SHDBManager.sharedInstance.graph else {
            throw KBError.databaseNotReady
        }
        
        let globalIdentifiers = Array(Set(globalIdentifiers))
        
        var sharedWithCondition = KBTripleCondition(value: false)
        var sharedByCondition = KBTripleCondition(value: false)
        
        for assetId in globalIdentifiers {
            sharedWithCondition = sharedWithCondition.or(
                KBTripleCondition(
                    subject: assetId, predicate: SHKGPredicate.sharedWith.rawValue, object: nil
                )
            )
            sharedByCondition = sharedByCondition.or(
                KBTripleCondition(
                    subject: nil, predicate: SHKGPredicate.shares.rawValue, object: assetId
                )
            )
            
            if filterOutInProgress == false {
                sharedByCondition = sharedByCondition.or(
                    KBTripleCondition(
                        subject: nil, predicate: SHKGPredicate.attemptedShare.rawValue, object: assetId
                    )
                )
            }
        }
        
        var result = [GlobalIdentifier: [(SHKGPredicate, UserIdentifier)]]()
        
        var triplesShares = [KBTriple]()
        var triplesSharedWith = [KBTriple]()
        try readWriteGraphQueue.sync(flags: .barrier) {
            triplesShares = try graph.triples(matching: sharedByCondition)
            triplesSharedWith = try graph.triples(matching: sharedWithCondition)
            
            for triple in triplesShares {
                let userId = triple.subject
                let assetId = triple.object
                
                if result[assetId] == nil {
                    result[assetId] = [(SHKGPredicate.shares, userId)]
                } else {
                    result[assetId]!.append((SHKGPredicate.shares, userId))
                }
                
                if let _ = UserIdToAssetGidSharedByCache[userId] {
                    UserIdToAssetGidSharedByCache[userId]!.insert(assetId)
                } else {
                    UserIdToAssetGidSharedByCache[userId] = [assetId]
                }
            }
            
            for triple in triplesSharedWith {
                let userId = triple.object
                let assetId = triple.subject
                
                if result[assetId] == nil {
                    result[assetId] = [(SHKGPredicate.sharedWith, userId)]
                } else {
                    result[assetId]!.append((SHKGPredicate.sharedWith, userId))
                }
                
                if let _ = UserIdToAssetGidSharedWithCache[userId] {
                    UserIdToAssetGidSharedWithCache[userId]!.insert(assetId)
                } else {
                    UserIdToAssetGidSharedWithCache[userId] = [assetId]
                }
            }
        }
        
        return result
    }
    
    static internal func removeSharingInformation(
        basedOn usersToRemoveByAssetId: [GlobalIdentifier: [UserIdentifier]]
    ) throws {
        var condition = KBTripleCondition(value: false)
        for (globalIdentifier, recipientIds) in usersToRemoveByAssetId {
            for recipientId in recipientIds {
                condition = condition.or(KBTripleCondition(
                    subject: globalIdentifier,
                    predicate: SHKGPredicate.sharedWith.rawValue,
                    object: recipientId
                ))
            }
        }
        
        try readWriteGraphQueue.sync(flags: .barrier) {
            guard let graph = SHDBManager.sharedInstance.graph else {
                throw KBError.databaseNotReady
            }
            try graph.removeTriples(matching: condition)
            
            for recipientIds in usersToRemoveByAssetId.values {
                for recipientId in recipientIds {
                    if UserIdToAssetGidSharedWithCache[recipientId]?.isEmpty ?? false {
                        UserIdToAssetGidSharedWithCache.removeValue(forKey: recipientId)
                    }
                }
            }
        }
    }
    
    public static func assetGlobalIdentifiers(
        notIn globalIdentifiersToExclude: [GlobalIdentifier],
        filterOutInProgress: Bool = true
    ) throws -> [GlobalIdentifier] {
        // TODO: Implement negated triple conditions
        return []
    }
    
    public static func assetGlobalIdentifiers(
        correspondingTo localIdentifiers: [LocalIdentifier]
    ) throws -> [GlobalIdentifier] {
        var globalIdentifiers = [GlobalIdentifier]()
        
        try readWriteGraphQueue.sync(flags: .barrier) {
            guard let graph = SHDBManager.sharedInstance.graph else {
                throw KBError.databaseNotReady
            }
            
            var condition = KBTripleCondition(value: false)
            for localIdentifier in localIdentifiers {
                condition = condition.or(KBTripleCondition(
                    subject: nil,
                    predicate: SHKGPredicate.localAssetIdEquivalent.rawValue,
                    object: localIdentifier
                ))
            }
            
            globalIdentifiers = try graph.triples(matching: condition).map({ $0.subject })
        }
        
        return globalIdentifiers
    }
}
