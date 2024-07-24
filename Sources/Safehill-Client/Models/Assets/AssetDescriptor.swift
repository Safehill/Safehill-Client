import Foundation

///
/// Safehill Server descriptor: metadata associated with an asset, such as creation date, sender and list of receivers
///
public protocol SHAssetDescriptor: SHBackedUpAssetIdentifiable {
    var localIdentifier: String? { get set }
    var creationDate: Date? { get }
    var uploadState: SHAssetDescriptorUploadState { get }
    var sharingInfo: SHDescriptorSharingInfo { get }
}
