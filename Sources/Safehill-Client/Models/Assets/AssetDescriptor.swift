import Foundation

///
/// Safehill Server descriptor: metadata associated with an asset, such as creation date, sender and list of receivers
///
public protocol SHAssetDescriptor: SHBackedUpAssetIdentifiable {
    var globalIdentifier: GlobalIdentifier { get }
    var localIdentifier: LocalIdentifier? { get set }
    var creationDate: Date? { get }
    var uploadState: SHAssetUploadState { get }
    var sharingInfo: SHDescriptorSharingInfo { get }

    func serialized() -> SHGenericAssetDescriptorClass
}

public extension SHAssetDescriptor {
    /// Determines if this asset can be saved or shared by a specific user.
    ///
    /// Returns `true` if any of the following conditions are met:
    /// - The user is the owner of the asset
    /// - The user belongs to a group with shareable permissions (permissions == 1)
    /// - The asset is in a public collection (visibility == "public")
    ///
    /// The most permissive permission across all access paths wins.
    func canBeSavedOrShared(by userId: UserIdentifier) -> Bool {
        // Owner can always save/share
        if sharingInfo.sharedByUserIdentifier == userId {
            return true
        }

        // Check groups the user is in for shareable permission (permissions == 1 means shareable)
        if let groupIds = sharingInfo.groupIdsByRecipientUserIdentifier[userId] {
            for groupId in groupIds {
                if sharingInfo.groupInfoById[groupId]?.permissions == 1 {
                    return true
                }
            }
        }

        // Check collections for public visibility
        for (_, collectionInfo) in sharingInfo.collectionInfoById {
            if collectionInfo.visibility == "public" {
                return true
            }
        }

        // Default: not shareable (implicit dropbox = confidential)
        return false
    }
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
