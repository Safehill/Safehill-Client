import Foundation

public protocol ReadableAssetActivity: Hashable, Equatable, Identifiable {
    var assetIds: [String] { get }
    var groupId: String { get }
    var eventOriginator: any SHServerUser { get }
    var shareInfo: [(with: any SHServerUser, at: Date)] { get }
}


public extension ReadableAssetActivity {
    static func == (lhs: Self, rhs: Self) -> Bool {
        Set(lhs.assetIds) == Set(rhs.assetIds)
        && lhs.groupId == rhs.groupId
        && Set(lhs.shareInfo.map({ $0.with.identifier })) == Set(rhs.shareInfo.map({ $0.with.identifier }))
    }
    
    func hash(into hasher: inout Hasher) {
        hasher.combine(assetIds)
        hasher.combine(groupId)
        hasher.combine(shareInfo.map { $0.with.identifier })
    }
    
    var id: String {
        return self.groupId
    }
}
