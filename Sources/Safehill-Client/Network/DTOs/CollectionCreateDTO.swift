import Foundation

public struct CollectionCreateDTO: Codable {
    public let name: String
    public let description: String

    public init(name: String, description: String) {
        self.name = name
        self.description = description
    }
}
