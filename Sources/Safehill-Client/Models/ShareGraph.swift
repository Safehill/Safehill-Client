import KnowledgeBase
import Foundation

public enum SHKGPredicates: String {
    case shares = "shares"
    case sharedWith = "sharedWith"
}

public enum SHKGQuery {
    public static func isKnownUser(withIdentifier userId: String) throws -> Bool {
        let graph = try SHDBManager.sharedInstance.graph()
        let userEntity = graph.entity(withIdentifier: userId)
        let sharesCount = try userEntity.linkedEntities(withPredicate: SHKGPredicates.shares.rawValue).count
        if sharesCount > 0 {
            return true
        }
        let sharedWithCount = try userEntity.linkingEntities(withPredicate: SHKGPredicates.sharedWith.rawValue).count
        if sharedWithCount > 0 {
            return true
        }
        return false
    }
    
    internal static func ingest(_ descriptors: [any SHAssetDescriptor], receiverUserId: String) throws {
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
                                     from senderUserId: String,
                                     to receiverUserIds: [String]) throws {
        let graph = try SHDBManager.sharedInstance.graph()
        let kgSender = graph.entity(withIdentifier: senderUserId)
        var errors = [Error]()
        
        do {
            let kgAsset = graph.entity(withIdentifier: assetIdentifier)
            try kgSender.link(to: kgAsset, withPredicate: SHKGPredicates.shares.rawValue)
            for userId in receiverUserIds {
                if userId == senderUserId {
                    continue
                }
                let kgOtherUser = graph.entity(withIdentifier: userId)
                try kgAsset.link(to: kgOtherUser, withPredicate: SHKGPredicates.sharedWith.rawValue)
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
        // TODO: Support writebatch in KnowledgeGraph
        let graph = try SHDBManager.sharedInstance.graph()
        try globalIdentifiers.forEach({
            let assetEntity = graph.entity(withIdentifier: $0)
            try assetEntity.remove()
        })
    }
    
    public static func deepClean() throws {
        let graph = try SHDBManager.sharedInstance.graph()
        let _ = try graph.removeAll()
    }
    
    public static func assetGlobalIdentifiers(
        sharedBy userIdentifiers: [String]
    ) throws -> [GlobalIdentifier: Set<String>] {
        let graph = try SHDBManager.sharedInstance.graph()
        var assetsToUsers = [GlobalIdentifier: Set<String>]()
        var sharedByUsersCondition = KBTripleCondition(value: false)
        
        for userId in userIdentifiers {
            sharedByUsersCondition = sharedByUsersCondition.or(
                KBTripleCondition(
                    subject: userId,
                    predicate: SHKGPredicates.shares.rawValue,
                    object: nil
                )
            )
        }
        
        for triple in try graph.triples(matching: sharedByUsersCondition) {
            let assetId = triple.object
            if let _ = assetsToUsers[assetId] {
                assetsToUsers[assetId]!.insert(triple.subject)
            } else {
                assetsToUsers[assetId] = [triple.subject]
            }
        }
        
        return assetsToUsers
    }
    
    public static func assetGlobalIdentifiers(
        sharedWith userIdentifiers: [String]
    ) throws -> [GlobalIdentifier: Set<String>] {
        let graph = try SHDBManager.sharedInstance.graph()
        var assetsToUsers = [GlobalIdentifier: Set<String>]()
        var sharedWithUsersCondition = KBTripleCondition(value: false)
        
        for userId in userIdentifiers {
            sharedWithUsersCondition = sharedWithUsersCondition.or(
                KBTripleCondition(
                    subject: nil,
                    predicate: SHKGPredicates.sharedWith.rawValue,
                    object: userId
                )
            )
        }
        
        for triple in try graph.triples(matching: sharedWithUsersCondition) {
            let assetId = triple.subject
            if let _ = assetsToUsers[assetId] {
                assetsToUsers[assetId]!.insert(triple.object)
            } else {
                assetsToUsers[assetId] = [triple.object]
            }
        }
        
        return assetsToUsers
    }
}
