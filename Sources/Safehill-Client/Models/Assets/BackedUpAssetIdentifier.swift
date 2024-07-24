import Foundation

public protocol SHBackedUpAssetIdentifiable: Hashable {
    var globalIdentifier: GlobalIdentifier { get }
    var localIdentifier: LocalIdentifier? { get }
}

public extension SHBackedUpAssetIdentifiable {
    static func == (lhs: Self, rhs: Self) -> Bool {
        lhs.localIdentifier == rhs.localIdentifier
        && lhs.globalIdentifier == rhs.globalIdentifier
    }
    
    func hash(into hasher: inout Hasher) {
        hasher.combine(localIdentifier)
        hasher.combine(globalIdentifier)
    }
}

public struct SHBackedUpAssetIdentifier: SHBackedUpAssetIdentifiable {
    public let globalIdentifier: String
    public let localIdentifier: String?
 
    public init(globalIdentifier: String, localIdentifier: String?) {
        self.globalIdentifier = globalIdentifier
        self.localIdentifier = localIdentifier
    }
}
