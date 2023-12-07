import KnowledgeBase
import Foundation

public enum SHKGPredicates: String {
    case attemptedShare = "attemptedShare"
    case shares = "shares"
    case sharedWith = "sharedWith"
}

var UserIdToAssetGidSharedByCache = [UserIdentifier: Set<GlobalIdentifier>]()
var UserIdToAssetGidSharedWithCache = [UserIdentifier: Set<GlobalIdentifier>]()

public enum SHKGQuery {
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
        let graph = try SHDBManager.sharedInstance.graph()
        let kgSender = graph.entity(withIdentifier: senderUserId)
        var errors = [Error]()
        
        do {
            let kgAsset = graph.entity(withIdentifier: assetIdentifier)
            if provisional {
                try kgSender.link(to: kgAsset, withPredicate: SHKGPredicates.attemptedShare.rawValue)
            } else {
                try kgSender.link(to: kgAsset, withPredicate: SHKGPredicates.shares.rawValue)
                try graph.removeTriples(matching: KBTripleCondition(subject: senderUserId, predicate: SHKGPredicates.attemptedShare.rawValue, object: nil))
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
                
                if let _ = UserIdToAssetGidSharedWithCache[userId] {
                    UserIdToAssetGidSharedWithCache[userId]!.insert(assetIdentifier)
                } else {
                    UserIdToAssetGidSharedWithCache[userId] = [assetIdentifier]
                }
            }
        } catch {
            log.critical("[KG] failed to ingest descriptor for assetGid=\(assetIdentifier) into the graph")
            errors.append(error)
        }
        
        if errors.isEmpty == false {
            throw errors.first!
        }
    }
    
    public static func removeAssets(with globalIdentifiers: [GlobalIdentifier]) throws {
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
        let graph = try SHDBManager.sharedInstance.graph()
        try globalIdentifiers.forEach({
            let assetEntity = graph.entity(withIdentifier: $0)
            try assetEntity.remove()
        })
    }
    
    internal static func removeUsers(with userIdentifiers: [UserIdentifier]) throws {
        /// Invalidate cache
        userIdentifiers.forEach({ uid in
            UserIdToAssetGidSharedByCache.removeValue(forKey: uid)
            UserIdToAssetGidSharedWithCache.removeValue(forKey: uid)
        })
        
        // TODO: Support writebatch in KnowledgeGraph
        let graph = try SHDBManager.sharedInstance.graph()
        for userId in userIdentifiers {
            try graph.removeEntity(userId)
        }
    }
    
    public static func deepClean() throws {
        /// Invalidate cache
        UserIdToAssetGidSharedByCache.removeAll()
        UserIdToAssetGidSharedWithCache.removeAll()
        
        let graph = try SHDBManager.sharedInstance.graph()
        let _ = try graph.removeAll()
    }
    
    public static func assetGlobalIdentifiers(
        sharedBy userIdentifiers: [UserIdentifier]
    ) throws -> [GlobalIdentifier: Set<UserIdentifier>] {
        let graph = try SHDBManager.sharedInstance.graph()
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
        
        let triples = try graph.triples(matching: sharedByUsersCondition)
        
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
        let graph = try SHDBManager.sharedInstance.graph()
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
        
        let triples = try graph.triples(matching: sharedWithUsersCondition)
        
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
}
