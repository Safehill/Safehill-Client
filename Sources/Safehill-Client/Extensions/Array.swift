import Foundation

extension Array where Element: Hashable {
    func subtract(_ other: [Element]) -> [Element] {
        var thisSet = Set(self)
        let otherSet = Set(other)
        thisSet.subtract(otherSet)
        return Array(thisSet)
    }
}

public extension Array {
    mutating func append(missingContentsFrom other: [Element],
                         compareUsing isEqual: (Element, Element) -> Bool) {
        for b in other {
            if self.contains(where: { a in isEqual(a, b) }) == false {
                self.append(b)
            }
        }
    }
}
