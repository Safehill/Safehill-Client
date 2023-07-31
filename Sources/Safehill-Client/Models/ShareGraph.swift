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
    
    internal static func ingest(_ descriptors: [any SHAssetDescriptor], receiverUserId: String) {
        let graph: KBKnowledgeStore
        do {
            graph = try SHDBManager.sharedInstance.graph()
        } catch {
            log.critical("[KG] Failed to initialize connection to Graph DB. Download event is not being recorded in the Graph.")
            return
        }
        
        let kgMyUser = graph.entity(withIdentifier: receiverUserId)
        
        for descriptor in descriptors {
            let kgSender = graph.entity(withIdentifier: descriptor.sharingInfo.sharedByUserIdentifier)
            
            do {
                let kgAsset = graph.entity(withIdentifier: descriptor.globalIdentifier)
                try kgSender.link(to: kgAsset, withPredicate: SHKGPredicates.shares.rawValue)
                try kgAsset.link(to: kgMyUser, withPredicate: SHKGPredicates.sharedWith.rawValue)
                for (userId, _) in descriptor.sharingInfo.sharedWithUserIdentifiersInGroup {
                    guard userId != receiverUserId else {
                        continue
                    }
                    let kgOtherUser = graph.entity(withIdentifier: userId)
                    try kgAsset.link(to: kgOtherUser, withPredicate: SHKGPredicates.sharedWith.rawValue)
                }
            } catch {
                log.critical("[KG] failed to ingest descriptor for assetGid=\(descriptor.globalIdentifier) into the graph")
            }
        }
    }
}
