import Foundation

public protocol SHAssetGroupInfo {
    /// The name of the asset group (optional)
    var encryptedTitle: String? { get }
    /// The identifier of the user that created the group (introduced in Nov 2024)
    var createdBy: UserIdentifier? { get }
    /// ISO8601 formatted datetime, representing the time the asset group was created
    var createdAt: Date? { get }
    /// Whether or not the share group was created from a thread (namely is a photo message)
    var createdFromThreadId: String? { get }
    /// The list of phone number that have been invited to this group
    var invitedUsersPhoneNumbers: [String: String]? { get }
}
