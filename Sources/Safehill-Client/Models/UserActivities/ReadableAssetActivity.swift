import Foundation

public protocol ReadableAssetActivity: Hashable, Equatable, Identifiable {
    var assets: [Asset] { get }
    var groupId: String { get }
    var eventOriginator: any SHServerUser { get }
    var shareInfo: [(with: any SHServerUser, at: Date)] { get }
}


public extension ReadableAssetActivity {
    static func == (lhs: Self, rhs: Self) -> Bool {
        lhs.assets.map({ $0.identifier }).sorted() == rhs.assets.map({ $0.identifier }).sorted()
        && lhs.groupId == rhs.groupId
        && lhs.shareInfo.map { $0.with.identifier }.sorted() == rhs.shareInfo.map({ $0.with.identifier }).sorted()
    }
    
    func hash(into hasher: inout Hasher) {
        hasher.combine(assets.map({ $0.identifier }))
        hasher.combine(groupId)
        hasher.combine(shareInfo.map { $0.with.identifier })
    }
    
    var id: String {
        return self.groupId
    }
    
    var sortedAssets: [Asset] {
        return assets.sorted(by: { a, b in
            return (a.creationDate ?? .distantPast).compare(b.creationDate ?? .distantPast) == .orderedDescending
        })
    }
}
