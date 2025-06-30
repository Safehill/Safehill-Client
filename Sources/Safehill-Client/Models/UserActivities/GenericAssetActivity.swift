import Foundation

public class GenericAssetActivity: AssetActivity {
    
    /// The list of asset identifiers in this request
    public let assetIds: [GlobalIdentifier]
    
    public let groupId: String
    public let groupTitle: String?
    public let groupPermissions: GroupPermission
    public let eventOriginator: SHServerUser
    public let shareInfo: [(with: SHServerUser, at: Date)]
    public let invitationsInfo: [(with: FormattedPhoneNumber, at: Date)]
    
    public let asPhotoMessageInThreadId: String?
    
    init(
        assetIds: [GlobalIdentifier],
        groupId: String,
        groupTitle: String? = nil,
        groupPermissions: GroupPermission,
        eventOriginator: SHServerUser,
        shareInfo: [(with: SHServerUser, at: Date)],
        invitationsInfo: [(with: FormattedPhoneNumber, at: Date)],
        asPhotoMessageInThreadId: String?,
    ) {
        self.assetIds = assetIds
        self.groupId = groupId
        self.groupTitle = groupTitle
        self.groupPermissions = groupPermissions
        self.eventOriginator = eventOriginator
        self.shareInfo = shareInfo
        self.invitationsInfo = invitationsInfo
        self.asPhotoMessageInThreadId = asPhotoMessageInThreadId
    }
}
