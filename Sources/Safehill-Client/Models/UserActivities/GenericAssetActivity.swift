import Foundation

class GenericAssetActivity: ReadableAssetActivity {
    
    /// The list of local asset identifiers in this request
    let assets: [Asset]
    
    var groupId: String
    var eventOriginator: SHServerUser
    var shareInfo: [(with: SHServerUser, at: Date)]
    
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

