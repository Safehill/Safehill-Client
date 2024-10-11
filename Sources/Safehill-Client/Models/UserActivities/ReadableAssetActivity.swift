import Foundation

public protocol ReadableAssetActivity: Hashable, Equatable, Identifiable {
    var assetIds: [AssetReference] { get }
    var groupId: String { get }
    var eventOriginator: any SHServerUser { get }
    var shareInfo: [(with: any SHServerUser, at: Date)] { get }
    var invitationsInfo: [(with: String, at: Date)] { get }
}


public extension ReadableAssetActivity {
    static func == (lhs: Self, rhs: Self) -> Bool {
        Set(lhs.assetIds.map({ $0.hashValue })) == Set(rhs.assetIds.map({ $0.hashValue }))
        && lhs.groupId == rhs.groupId
        && Set(lhs.shareInfo.map({ $0.with.identifier })) == Set(rhs.shareInfo.map({ $0.with.identifier }))
        && Set(lhs.invitationsInfo.map({ $0.with })) == Set(rhs.invitationsInfo.map({ $0.with }))
    }
    
    func hash(into hasher: inout Hasher) {
        hasher.combine(assetIds.map({ $0.hashValue }))
        hasher.combine(groupId)
        hasher.combine(shareInfo.map { $0.with.identifier })
        hasher.combine(invitationsInfo.map { $0.with })
    }
    
    var id: String {
        return self.groupId
    }
}
