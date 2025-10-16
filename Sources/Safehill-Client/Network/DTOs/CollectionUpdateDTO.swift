import Foundation

public struct CollectionUpdateDTO: Codable {
    public let name: String?
    public let description: String?
    public let pricing: Double?

    public init(name: String? = nil, description: String? = nil, pricing: Double? = nil) {
        self.name = name
        self.description = description
        self.pricing = pricing
    }
}
