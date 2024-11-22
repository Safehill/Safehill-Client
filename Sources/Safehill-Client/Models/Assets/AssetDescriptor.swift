import Foundation

///
/// Safehill Server descriptor: metadata associated with an asset, such as creation date, sender and list of receivers
///
public protocol SHAssetDescriptor: SHBackedUpAssetIdentifiable {
    var localIdentifier: String? { get set }
    var perceptualHash: String? { get }
    var creationDate: Date? { get }
    var uploadState: SHAssetDescriptorUploadState { get }
    var sharingInfo: SHDescriptorSharingInfo { get }
    
    func serialized() -> SHGenericAssetDescriptorClass
}

extension Array<SHAssetDescriptor> {
    func allReferencedUserIds() -> Set<UserIdentifier> {
        var userIdsDescriptorsSet = Set<UserIdentifier>()
        for descriptor in self {
            userIdsDescriptorsSet.insert(descriptor.sharingInfo.sharedByUserIdentifier)
            descriptor.sharingInfo.groupIdsByRecipientUserIdentifier.keys.forEach({ userIdsDescriptorsSet.insert($0) })
        }
        return userIdsDescriptorsSet
    }
}
