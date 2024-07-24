import Foundation
import KnowledgeBase

extension SHAssetsSyncOperation {
    
    /// Removes any evidence of the users from the local server.
    ///
    /// Returns the queueItemIdentifiers replaced and the ones removed
    /// - Parameter userIdsToRemoveFromGroup: maps groupId -> list of user ids to remove
    /// - Returns: the list of keys changed and removed in the `SHShareHistoryQueue`
    ///
    func removeUsersFromShareHistoryQueueItems(
        _ userIdsToRemoveFromGroup: [String: Set<UserIdentifier>]
    ) -> (changed: [String], removed: [String]) {
        
        // TODO: Implement this
        
        return (changed: [], removed: [])
    }
}
