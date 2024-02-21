import KnowledgeBase
import Foundation

public enum SHKGPredicates: String {
    case attemptedShare = "attemptedShare"
    case localAssetIdEquivalent = "localAssetIdEquivalent"
    case shares = "shares"
    case sharedWith = "sharedWith"
}

var UserIdToAssetGidSharedByCache = [UserIdentifier: Set<GlobalIdentifier>]()
var UserIdToAssetGidSharedWithCache = [UserIdentifier: Set<GlobalIdentifier>]()

public struct SHKGQuery {
    
    private static let readWriteGraphQueue = DispatchQueue(label: "SHKGQuery.readWrite", attributes: .concurrent)
    
    public static func areUsersKnown(withIdentifiers userIds: [UserIdentifier]) throws -> [UserIdentifier: Bool] {
        var result = [UserIdentifier: Bool]()
        
        let sharedBy = try SHKGQuery.assetGlobalIdentifiers(
            sharedBy: userIds,
            filterOutInProgress: false
        )
        let sharedWith = try SHKGQuery.assetGlobalIdentifiers(
            sharedWith: userIds
        )
        
        for (gid, uid) in sharedBy {
            if let _ = UserIdToAssetGidSharedByCache[uid] {
                UserIdToAssetGidSharedByCache[uid]!.insert(gid)
            } else {
                UserIdToAssetGidSharedByCache[uid] = [gid]
            }
            
            result[uid] = true
        }
        
        for (gid, uids) in sharedWith {
            for uid in uids {
                if let _ = UserIdToAssetGidSharedWithCache[uid] {
                    UserIdToAssetGidSharedWithCache[uid]?.insert(gid)
                } else {
                    UserIdToAssetGidSharedWithCache[uid] = [gid]
                }
                
                result[uid] = true
            }
        }
        
        for userId in userIds {
            if result[userId] == nil {
                result[userId] = false
            }
        }
        
        return result
    }
    
    internal static func ingest(_ descriptors: [any SHAssetDescriptor], receiverUserId: UserIdentifier) throws {
        var errors = [Error]()
        
        // TODO: We need support for writebatch (transaction) in KGGraph. DB writes in a for loop is never a good idea
        for descriptor in descriptors {
            do {
                var allReceivers = Set(descriptor.sharingInfo.sharedWithUserIdentifiersInGroup.keys)
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
    
    internal static func ingestProvisionalShare(
        of assetIdentifier: GlobalIdentifier,
        localIdentifier: String?,
        from senderUserId: UserIdentifier,
        to receiverUserIds: [UserIdentifier]
    ) throws {
        var errors = [Error]()
        
        do {
            try readWriteGraphQueue.sync(flags: .barrier) {
                guard let graph = SHDBManager.sharedInstance.graph else {
                    throw KBError.databaseNotReady
                }
                
                let allTriplesBefore = try graph.triples(matching: nil)
                log.debug("[sh-kg] graph before ingest \(allTriplesBefore)")
                
                let kgSender = graph.entity(withIdentifier: senderUserId)
                let kgAsset = graph.entity(withIdentifier: assetIdentifier)
                
                log.debug("[sh-kg] adding triple <user=\(kgSender.identifier), \(SHKGPredicates.attemptedShare.rawValue), asset=\(kgAsset.identifier)>")
                try kgSender.link(to: kgAsset, withPredicate: SHKGPredicates.attemptedShare.rawValue)
                
                if let localIdentifier {
                    let kgCorrespondingLocalAsset = graph.entity(withIdentifier: localIdentifier)
                    log.debug("[sh-kg] adding triple <asset=\(kgAsset.identifier), \(SHKGPredicates.localAssetIdEquivalent.rawValue), localAsset=\(kgCorrespondingLocalAsset.identifier)>")
                    try kgAsset.link(to: kgCorrespondingLocalAsset, withPredicate: SHKGPredicates.localAssetIdEquivalent.rawValue)
                }
                
                for userId in receiverUserIds {
                    if userId == senderUserId {
                        continue
                    }
                    let kgOtherUser = graph.entity(withIdentifier: userId)
                    try kgAsset.link(to: kgOtherUser, withPredicate: SHKGPredicates.sharedWith.rawValue)
                    log.debug("[sh-kg] adding triple <asset=\(kgAsset.identifier), \(SHKGPredicates.sharedWith.rawValue), user=\(kgOtherUser.identifier)>")
                }
                
                let allTriplesAfter = try graph.triples(matching: nil)
                log.debug("[sh-kg] graph after ingest \(allTriplesAfter)")
            }
        } catch {
            log.critical("[KG] failed to ingest descriptor for assetGid=\(assetIdentifier) into the graph")
            errors.append(error)
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
                
                let allTriplesBefore = try graph.triples(matching: nil)
                log.debug("[sh-kg] graph before ingest \(allTriplesBefore)")
                
                let kgSender = graph.entity(withIdentifier: senderUserId)
                let kgAsset = graph.entity(withIdentifier: assetIdentifier)
                
                log.debug("[sh-kg] adding triple <user=\(kgSender.identifier), \(SHKGPredicates.shares.rawValue), asset=\(kgAsset.identifier)>")
                try kgSender.link(to: kgAsset, withPredicate: SHKGPredicates.shares.rawValue)

                let tripleCondition = KBTripleCondition(subject: kgSender.identifier, predicate: SHKGPredicates.attemptedShare.rawValue, object: kgAsset.identifier)
                log.debug("[sh-kg] removing triples matching <user=\(kgSender.identifier), \(SHKGPredicates.attemptedShare.rawValue), \(kgAsset.identifier)>")
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
                    try kgAsset.link(to: kgOtherUser, withPredicate: SHKGPredicates.sharedWith.rawValue)
                    log.debug("[sh-kg] adding triple <asset=\(kgAsset.identifier), \(SHKGPredicates.sharedWith.rawValue), user=\(kgOtherUser.identifier)>")
                    
                    if let _ = UserIdToAssetGidSharedWithCache[userId] {
                        UserIdToAssetGidSharedWithCache[userId]!.insert(assetIdentifier)
                    } else {
                        UserIdToAssetGidSharedWithCache[userId] = [assetIdentifier]
                    }
                }
                
                let allTriplesAfter = try graph.triples(matching: nil)
                log.debug("[sh-kg] graph after ingest \(allTriplesAfter)")
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
        let removeGidsFromCache = {
            (cache: inout [UserIdentifier: Set<GlobalIdentifier>]) in
            let userIds = Array(cache.keys)
            for userId in userIds {
                if let cachedValue = cache[userId] {
                    let newAssetsGids = Set(globalIdentifiers).intersection(cachedValue)
                    if newAssetsGids.isEmpty {
                        cache.removeValue(forKey: userId)
                    } else {
                        cache[userId]!.subtract(globalIdentifiers)
                    }
                }
            }
        }
        
        /// Invalidate caches
        removeGidsFromCache(&UserIdToAssetGidSharedByCache)
        removeGidsFromCache(&UserIdToAssetGidSharedWithCache)
        
        // TODO: Support writebatch in KnowledgeGraph
        try readWriteGraphQueue.sync(flags: .barrier) {
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
        /// Invalidate cache
        userIdentifiers.forEach({ uid in
            UserIdToAssetGidSharedByCache.removeValue(forKey: uid)
            UserIdToAssetGidSharedWithCache.removeValue(forKey: uid)
        })
        
        // TODO: Support writebatch in KnowledgeGraph
        try readWriteGraphQueue.sync(flags: .barrier) {
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
        /// Invalidate cache
        UserIdToAssetGidSharedByCache.removeAll()
        UserIdToAssetGidSharedWithCache.removeAll()
        
        try readWriteGraphQueue.sync(flags: .barrier) {
            guard let graph = SHDBManager.sharedInstance.graph else {
                throw KBError.databaseNotReady
            }
            let _ = try graph.removeAll()
            log.debug("[sh-kg] removing all triples")
        }
    }
    
    public static func assetGlobalIdentifiers(
        sharedBy userIdentifiers: [UserIdentifier],
        filterOutInProgress: Bool = true
    ) throws -> [GlobalIdentifier: UserIdentifier] {
        guard let graph = SHDBManager.sharedInstance.graph else {
            throw KBError.databaseNotReady
        }
        var assetsToUser = [GlobalIdentifier: UserIdentifier]()
        var sharedByUsersCondition = KBTripleCondition(value: false)
        
        var usersIdsToSearch = [UserIdentifier]()
        for userId in Set(userIdentifiers) {
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
            return assetsToUser
        }
        
        for userId in usersIdsToSearch {
            sharedByUsersCondition = sharedByUsersCondition.or(
                KBTripleCondition(
                    subject: userId,
                    predicate: SHKGPredicates.shares.rawValue,
                    object: nil
                )
            )
            if filterOutInProgress == false {
                sharedByUsersCondition = sharedByUsersCondition.or(
                    KBTripleCondition(
                        subject: userId,
                        predicate: SHKGPredicates.attemptedShare.rawValue,
                        object: nil
                    )
                )
            }
        }
        
        var triples = [KBTriple]()
        try readWriteGraphQueue.sync(flags: .barrier) {
            triples = try graph.triples(matching: sharedByUsersCondition)
        }
        
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
        
        return assetsToUser
    }
    
    public static func assetGlobalIdentifiers(
        sharedWith userIdentifiers: [UserIdentifier]
    ) throws -> [GlobalIdentifier: Set<UserIdentifier>] {
        guard let graph = SHDBManager.sharedInstance.graph else {
            throw KBError.databaseNotReady
        }
        var assetsToUsers = [GlobalIdentifier: Set<UserIdentifier>]()
        var sharedWithUsersCondition = KBTripleCondition(value: false)
        
        var usersIdsToSearch = [UserIdentifier]()
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
        
        guard usersIdsToSearch.count > 0 else {
            return assetsToUsers
        }
        
        for userId in usersIdsToSearch {
            sharedWithUsersCondition = sharedWithUsersCondition.or(
                KBTripleCondition(
                    subject: nil,
                    predicate: SHKGPredicates.sharedWith.rawValue,
                    object: userId
                )
            )
        }
        
        var triples = [KBTriple]()
        try readWriteGraphQueue.sync(flags: .barrier) {
            triples = try graph.triples(matching: sharedWithUsersCondition)
        }
        
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
        
        return assetsToUsers
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
        filterOutInProgress: Bool = true
    ) throws -> [GlobalIdentifier: Set<UserIdentifier>] {
        var assetsToUsers = [GlobalIdentifier: Set<UserIdentifier>]()
        
        let sharedBy = try self.assetGlobalIdentifiers(sharedBy: userIdentifiers, filterOutInProgress: filterOutInProgress)
        let sharedWith = try self.assetGlobalIdentifiers(sharedWith: userIdentifiers)
        
        for (gid, uids) in sharedWith {
            uids.forEach({ uid in
                if let _ = assetsToUsers[gid] {
                    assetsToUsers[gid]!.insert(uid)
                } else {
                    assetsToUsers[gid] = [uid]
                }
            })
        }
        for (gid, uid) in sharedBy {
            if let _ = assetsToUsers[gid] {
                assetsToUsers[gid]!.insert(uid)
            } else {
                assetsToUsers[gid] = [uid]
            }
        }
        
        return assetsToUsers
    }
    
    public static func usersConnectedTo(assets globalIdentifiers: [GlobalIdentifier]) throws -> [UserIdentifier] {
        guard let graph = SHDBManager.sharedInstance.graph else {
            throw KBError.databaseNotReady
        }
        var sharedWithCondition = KBTripleCondition(value: false)
        var sharedByCondition = KBTripleCondition(value: false)
        
        for assetId in globalIdentifiers {
            sharedWithCondition = sharedWithCondition.or(
                KBTripleCondition(
                    subject: assetId, predicate: SHKGPredicates.sharedWith.rawValue, object: nil
                )
            )
            sharedByCondition = sharedByCondition.or(
                KBTripleCondition(
                    subject: nil, predicate: SHKGPredicates.shares.rawValue, object: assetId
                )
            ).or(
                KBTripleCondition(
                    subject: nil, predicate: SHKGPredicates.attemptedShare.rawValue, object: assetId
                )
            )
        }
        
        var userIds = [UserIdentifier]()
        try readWriteGraphQueue.sync(flags: .barrier) {
            userIds = try graph.triples(matching: sharedWithCondition).map({ $0.object })
            userIds.append(contentsOf: try graph.triples(matching: sharedByCondition).map({ $0.subject }))
        }
        
        return Array(Set(userIds))
    }
    
    static internal func removeSharingInformation(
        basedOn diff: [GlobalIdentifier: ShareSenderReceivers]
    ) throws {
        var condition = KBTripleCondition(value: false)
        for (globalIdentifier, shareDiff) in diff {
            for recipientId in shareDiff.groupIdByRecipientId.keys {
                condition = condition.or(KBTripleCondition(
                    subject: globalIdentifier,
                    predicate: SHKGPredicates.sharedWith.rawValue,
                    object: recipientId
                ))
            }
        }
        
        try readWriteGraphQueue.sync(flags: .barrier) {
            guard let graph = SHDBManager.sharedInstance.graph else {
                throw KBError.databaseNotReady
            }
            try graph.removeTriples(matching: condition)
        }
        
        for (globalIdentifier, shareDiff) in diff {
            for recipientId in shareDiff.groupIdByRecipientId.keys {
                UserIdToAssetGidSharedWithCache[recipientId]?.remove(globalIdentifier)
                if UserIdToAssetGidSharedWithCache[recipientId]?.isEmpty ?? false {
                    UserIdToAssetGidSharedWithCache.removeValue(forKey: recipientId)
                }
            }
        }
    }
}
