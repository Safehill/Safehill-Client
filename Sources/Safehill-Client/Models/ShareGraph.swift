import KnowledgeBase
import Foundation

public enum SHKGPredicates: String {
    case shares = "shares"
    case sharedWith = "sharedWith"
}

public enum SHKGQuery {
    static func isKnownUser(withIdentifier userId: String) throws -> Bool {
        let graph = try SHDBManager.sharedInstance.graph()
        let userEntity = graph.entity(withIdentifier: userId)
        let linkingCount = try userEntity.linkingEntities().count
        if linkingCount > 0 {
            return true
        }
        let linkedCount = try userEntity.linkedEntities().count
        if  linkedCount > 0 {
            return true
        }
        return false
    }
}
