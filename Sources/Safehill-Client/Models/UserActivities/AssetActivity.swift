import Foundation

public protocol AssetActivity: Hashable, Equatable, Identifiable {
    var assetIds: [GlobalIdentifier] { get }
    var groupId: String { get }
    var groupTitle: String? { get }
    var eventOriginator: any SHServerUser { get }
    var shareInfo: [(with: any SHServerUser, at: Date)] { get }
    var invitationsInfo: [(with: String, at: Date)] { get }
    
    /// Returns an copy of the activity with the assets removed.
    /// Use this method to keep assets in the activity immutable.
    /// - Parameter ids: the asset ids to remove
    /// - Returns: `nil` if no assets remain, the new activity otherwise
    func withAssetsRemoved(ids: [GlobalIdentifier]) -> (any AssetActivity)?
    
    /// Returns an copy of the activity with the invited phone numbers removed.
    /// Use this method to keep assets in the activity immutable.
    /// - Parameter phoneNumbers: the asset ids to remove
    /// - Returns: `nil` if no assets remain, the new activity otherwise
    func withPhoneNumbersRemoved(_ phoneNumbers: [String]) -> any AssetActivity
}


public extension AssetActivity {
    static func == (lhs: Self, rhs: Self) -> Bool {
        Set(lhs.assetIds) == Set(rhs.assetIds)
        && lhs.groupId == rhs.groupId
        && Set(lhs.shareInfo.map({ $0.with.identifier })) == Set(rhs.shareInfo.map({ $0.with.identifier }))
        && Set(lhs.invitationsInfo.map({ $0.with })) == Set(rhs.invitationsInfo.map({ $0.with }))
    }
    
    func hash(into hasher: inout Hasher) {
        hasher.combine(assetIds)
        hasher.combine(groupId)
        hasher.combine(shareInfo.map { $0.with.identifier })
        hasher.combine(invitationsInfo.map { $0.with })
    }
    
    var id: String {
        return self.groupId
    }
}
