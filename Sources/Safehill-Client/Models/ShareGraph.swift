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
}
