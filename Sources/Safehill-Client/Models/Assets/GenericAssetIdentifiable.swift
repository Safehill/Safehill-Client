import Foundation

public protocol GenericAssetIdentifiable: Hashable {
    var localIdentifier: LocalIdentifier? { get }
    var globalIdentifier: GlobalIdentifier? { get }
}

public extension GenericAssetIdentifiable {
    static func == (lhs: Self, rhs: Self) -> Bool {
        lhs.localIdentifier == rhs.localIdentifier
        && lhs.globalIdentifier == rhs.globalIdentifier
    }
    
    func hash(into hasher: inout Hasher) {
        hasher.combine(localIdentifier)
        hasher.combine(globalIdentifier)
    }
}
