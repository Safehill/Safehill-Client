import Foundation

public protocol GenericAssetIdentifiable: Hashable {
    var localIdentifier: String? { get }
    var globalIdentifier: String? { get }
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
