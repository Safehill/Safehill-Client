import Foundation

public protocol SHAssetGroupInfo {
    /// The name of the asset group (optional)
    var name: String? { get }
    /// ISO8601 formatted datetime, representing the time the asset group was created
    var createdAt: Date? { get }
    /// Whether or not the share group was created from a thread (namely is a photo message)
    var createdFromThreadId: String? { get }
    /// The list of phone number that have been invited to this group
    var invitedUsersPhoneNumbers: [String: String]? { get }
}
