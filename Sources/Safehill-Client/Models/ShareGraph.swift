import KnowledgeBase
import Foundation

public enum SHKGPredicates: String {
    case attemptedShare = "attemptedShare"
    case shares = "shares"
    case sharedWith = "sharedWith"
}

var UserIdToAssetGidSharedByCache = [UserIdentifier: Set<GlobalIdentifier>]()
var UserIdToAssetGidSharedWithCache = [UserIdentifier: Set<GlobalIdentifier>]()

public struct SHKGQuery {
    
    private static let readWriteGraphQueue = DispatchQueue(label: "SHKGQuery.readWrite", attributes: .concurrent)
    
    public static func isKnownUser(withIdentifier userId: UserIdentifier) throws -> Bool {
        if let assetIdsSharedBy = UserIdToAssetGidSharedByCache[userId],
           let assetIdsSharedWith = UserIdToAssetGidSharedWithCache[userId] {
            return assetIdsSharedBy.count + assetIdsSharedWith.count > 0
        }
        
        let assetIdsSharedBy = try SHKGQuery.assetGlobalIdentifiers(sharedBy: [userId])
        if assetIdsSharedBy.count > 0 {
            return true
        }
        
        let assetIdsSharedWith = try SHKGQuery.assetGlobalIdentifiers(sharedWith: [userId])
        if assetIdsSharedWith.count > 0 {
            return true
        }
        
        return false
    }
    
    internal static func ingest(_ descriptors: [any SHAssetDescriptor], receiverUserId: UserIdentifier) throws {
        var errors = [Error]()
        
        // TODO: We need support for writebatch (transaction) in KGGraph. DB writes in a for loop is never a good idea
        for descriptor in descriptors {
            do {
                var allReceivers = Set(descriptor.sharingInfo.sharedWithUserIdentifiersInGroup.keys)
                allReceivers.insert(receiverUserId)
                try self.ingestShare(of: descriptor.globalIdentifier,
                                     from: descriptor.sharingInfo.sharedByUserIdentifier,
                                     to: Array(allReceivers))
            } catch {
                errors.append(error)
            }
        }
        
        if errors.isEmpty == false {
            throw errors.first!
        }
    }
    
    internal static func ingestShare(of assetIdentifier: GlobalIdentifier,
                                     from senderUserId: UserIdentifier,
                                     to receiverUserIds: [UserIdentifier],
                                     provisional: Bool = false) throws {
        var errors = [Error]()
        
        do {
            try readWriteGraphQueue.sync(flags: .barrier) {
                guard let graph = SHDBManager.graph else {
                    throw KBError.databaseNotReady
                }
                
                let allTriplesBefore = try graph.triples(matching: nil)
                log.debug("[sh-kg] graph before ingest \(allTriplesBefore)")
                
                let kgSender = graph.entity(withIdentifier: senderUserId)
                let kgAsset = graph.entity(withIdentifier: assetIdentifier)
                
                if provisional {
                    try kgSender.link(to: kgAsset, withPredicate: SHKGPredicates.attemptedShare.rawValue)
                    log.debug("[sh-kg] adding triple <user=\(kgSender.identifier), \(SHKGPredicates.attemptedShare.rawValue), asset=\(kgAsset.identifier)>")
                } else {
                    try kgSender.link(to: kgAsset, withPredicate: SHKGPredicates.shares.rawValue)
                    let tripleCondition = KBTripleCondition(subject: senderUserId, predicate: SHKGPredicates.attemptedShare.rawValue, object: kgAsset.identifier)
                    try graph.removeTriples(matching: tripleCondition)
                    log.debug("[sh-kg] adding triple <user=\(kgSender.identifier), \(SHKGPredicates.shares.rawValue), asset=\(kgAsset.identifier)>")
                    log.debug("[sh-kg] removing triples matching <user=\(senderUserId), \(SHKGPredicates.attemptedShare.rawValue), \(kgAsset.identifier)>")
                }
                
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
            guard let graph = SHDBManager.graph else {
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
            guard let graph = SHDBManager.graph else {
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
            guard let graph = SHDBManager.graph else {
                throw KBError.databaseNotReady
            }
            let _ = try graph.removeAll()
            log.debug("[sh-kg] removing all triples")
        }
    }
    
    public static func assetGlobalIdentifiers(
        sharedBy userIdentifiers: [UserIdentifier]
    ) throws -> [GlobalIdentifier: Set<UserIdentifier>] {
        guard let graph = SHDBManager.graph else {
            throw KBError.databaseNotReady
        }
        var assetsToUsers = [GlobalIdentifier: Set<UserIdentifier>]()
        var sharedByUsersCondition = KBTripleCondition(value: false)
        
        var usersIdsToSearch = Set<UserIdentifier>()
        for userId in userIdentifiers {
            if let assetIdsSharedBy = UserIdToAssetGidSharedByCache[userId] {
                assetIdsSharedBy.forEach({
                    if assetsToUsers[$0] == nil {
                        assetsToUsers[$0] = [userId]
                    } else {
                        assetsToUsers[$0]!.insert(userId)
                    }
                })
            } else {
                usersIdsToSearch.insert(userId)
            }
        }
        
        guard usersIdsToSearch.count > 0 else {
            return assetsToUsers
        }
        
        for userId in usersIdsToSearch {
            sharedByUsersCondition = sharedByUsersCondition.or(
                KBTripleCondition(
                    subject: userId,
                    predicate: SHKGPredicates.shares.rawValue,
                    object: nil
                )
            ).or(
                KBTripleCondition(
                    subject: userId,
                    predicate: SHKGPredicates.attemptedShare.rawValue,
                    object: nil
                )
            )
        }
        
        var triples = [KBTriple]()
        try readWriteGraphQueue.sync(flags: .barrier) {
            triples = try graph.triples(matching: sharedByUsersCondition)
        }
        
        if triples.count == 0 {
            for userId in usersIdsToSearch {
                UserIdToAssetGidSharedByCache[userId] = []
            }
        }
        
        for triple in triples {
            let assetId = triple.object
            if let _ = assetsToUsers[assetId] {
                assetsToUsers[assetId]!.insert(triple.subject)
            } else {
                assetsToUsers[assetId] = [triple.subject]
            }
            
            if let _ = UserIdToAssetGidSharedByCache[triple.subject] {
                UserIdToAssetGidSharedByCache[triple.subject]!.insert(triple.object)
            } else {
                UserIdToAssetGidSharedByCache[triple.subject] = [triple.object]
            }
        }
        
        return assetsToUsers
    }
    
    public static func assetGlobalIdentifiers(
        sharedWith userIdentifiers: [UserIdentifier]
    ) throws -> [GlobalIdentifier: Set<UserIdentifier>] {
        guard let graph = SHDBManager.graph else {
            throw KBError.databaseNotReady
        }
        var assetsToUsers = [GlobalIdentifier: Set<UserIdentifier>]()
        var sharedWithUsersCondition = KBTripleCondition(value: false)
        
        var usersIdsToSearch = Set<UserIdentifier>()
        for userId in userIdentifiers {
            if let assetIdsSharedWith = UserIdToAssetGidSharedWithCache[userId] {
                assetIdsSharedWith.forEach({
                    if assetsToUsers[$0] == nil {
                        assetsToUsers[$0] = [userId]
                    } else {
                        assetsToUsers[$0]!.insert(userId)
                    }
                })
            } else {
                usersIdsToSearch.insert(userId)
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
        
        if triples.count == 0 {
            for userId in usersIdsToSearch {
                UserIdToAssetGidSharedWithCache[userId] = []
            }
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
    
    public static func usersConnectedTo(assets globalIdentifiers: [GlobalIdentifier]) throws -> [UserIdentifier] {
        guard let graph = SHDBManager.graph else {
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
    
    internal static func removeTriples(matching condition: KBTripleCondition) throws {
        try readWriteGraphQueue.sync(flags: .barrier) {
            guard let graph = SHDBManager.graph else {
                throw KBError.databaseNotReady
            }
            try graph.removeTriples(matching: condition)
        }
    }
}
