import Foundation

open class GenericAssetActivity: ReadableAssetActivity {
    
    /// The list of local asset identifiers in this request
    public let assets: [Asset]
    
    public var groupId: String
    public var eventOriginator: SHServerUser
    public var shareInfo: [(with: SHServerUser, at: Date)]
    
    init(
        assets: [Asset],
        groupId: String,
        eventOriginator: any SHServerUser,
        shareInfo: [(with: any SHServerUser, at: Date)]
    ) {
        self.assets = assets
        self.groupId = groupId
        self.eventOriginator = eventOriginator
        self.shareInfo = shareInfo
    }
}

