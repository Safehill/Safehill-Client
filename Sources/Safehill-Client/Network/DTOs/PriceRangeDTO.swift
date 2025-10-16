import Foundation

public struct PriceRangeDTO: Codable {
    public let min: Double?
    public let max: Double?

    public init(min: Double? = nil, max: Double? = nil) {
        self.min = min
        self.max = max
    }
}
