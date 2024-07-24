import Foundation

public protocol SHAssetGroupInfo {
    /// The name of the asset group (optional)
    var name: String? { get }
    /// ISO8601 formatted datetime, representing the time the asset group was created
    var createdAt: Date? { get }
}
