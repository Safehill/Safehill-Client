import Foundation

public struct CollectionSearchDTO: Codable {
    public let query: String?
    public let searchScope: String // "owned" for user's owned and accessed, "all" for all discoverable collections
    public let visibility: String? // Optional filter by visibility
    public let priceRange: PriceRangeDTO?

    public init(
        query: String? = nil,
        searchScope: String,
        visibility: String? = nil,
        priceRange: PriceRangeDTO? = nil
    ) {
        self.query = query
        self.searchScope = searchScope
        self.visibility = visibility
        self.priceRange = priceRange
    }
}
