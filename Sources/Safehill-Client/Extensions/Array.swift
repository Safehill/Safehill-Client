import Foundation

extension Array where Element: Hashable {
    func subtract(_ other: [Element]) -> [Element] {
        var thisSet = Set(self)
        let otherSet = Set(other)
        thisSet.subtract(otherSet)
        return Array(thisSet)
    }
}
