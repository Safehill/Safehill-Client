import Foundation

// TODO: Consider reusing CacheBox for all Codable Swift structs that need to be serialized via NSObject/NSSecureCoding

/**
 * Boxes any Codable value so it can be stored in NSCache
 */
final class CacheBox<T: Codable>: NSObject {
    let value: T

    init(_ value: T) {
        self.value = value
    }
}
