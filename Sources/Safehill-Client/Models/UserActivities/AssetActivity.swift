import Foundation

public protocol AssetActivity: Hashable, Equatable, Identifiable {
    var assetIds: [GlobalIdentifier] { get }
    var groupId: String { get }
    var groupTitle: String? { get }
    var groupPermissions: GroupPermission { get }
    var eventOriginator: any SHServerUser { get }
    var asPhotoMessageInThreadId: String? { get }
    var shareInfo: [(with: any SHServerUser, at: Date)] { get }
    var invitationsInfo: [(with: FormattedPhoneNumber, at: Date)] { get }
}


public extension AssetActivity {
    static func == (lhs: Self, rhs: Self) -> Bool {
        Set(lhs.assetIds) == Set(rhs.assetIds)
        && lhs.groupId == rhs.groupId
        && lhs.asPhotoMessageInThreadId == rhs.asPhotoMessageInThreadId
        && Set(lhs.shareInfo.map({ $0.with.identifier })) == Set(rhs.shareInfo.map({ $0.with.identifier }))
        && Set(lhs.invitationsInfo.map({ $0.with })) == Set(rhs.invitationsInfo.map({ $0.with }))
    }
    
    func hash(into hasher: inout Hasher) {
        hasher.combine(assetIds)
        hasher.combine(groupId)
        hasher.combine(asPhotoMessageInThreadId)
        hasher.combine(shareInfo.map { $0.with.identifier })
        hasher.combine(invitationsInfo.map { $0.with })
    }
    
    var id: String {
        return self.groupId
    }
}
