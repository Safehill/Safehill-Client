import Foundation

public struct SHGenericAssetGroupInfo : SHAssetGroupInfo, Codable {
    public let encryptedTitle: String?
    public let createdBy: UserIdentifier?
    public let createdAt: Date?
    public let createdFromThreadId: String?
    public let invitedUsersPhoneNumbers: [String: String]?
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        encryptedTitle = try? container.decodeIfPresent(String.self, forKey: .encryptedTitle)
        createdBy = try? container.decodeIfPresent(UserIdentifier.self, forKey: .createdBy)
        let dateString = try? container.decodeIfPresent(String.self, forKey: .createdAt)
        createdAt = dateString?.iso8601withFractionalSeconds
        createdFromThreadId = try? container.decodeIfPresent(String.self, forKey: .createdFromThreadId)
        invitedUsersPhoneNumbers = try? container.decodeIfPresent([String: String].self, forKey: .invitedUsersPhoneNumbers)
    }
    
    init(encryptedTitle: String?,
         createdBy: UserIdentifier?,
         createdAt: Date?,
         createdFromThreadId: String?,
         invitedUsersPhoneNumbers: [String: String]?) {
        self.encryptedTitle = encryptedTitle
        self.createdBy = createdBy
        self.createdAt = createdAt
        self.createdFromThreadId = createdFromThreadId
        self.invitedUsersPhoneNumbers = invitedUsersPhoneNumbers
    }
}
