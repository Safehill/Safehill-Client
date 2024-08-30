import Foundation

public class SHDownloadRequestQueueItem: NSObject, NSSecureCoding, SHSerializableQueueItem, SHShareableGroupableQueueItem {
    
    enum CodingKeys: String, CodingKey {
        case assetDescriptor
        case userIdentifier
    }
    
    public var localIdentifier: String {
        self.assetDescriptor.globalIdentifier
    }
    
    public let eventOriginator: any SHServerUser
    public let sharedWith: [any SHServerUser] = []
    public let invitedUsers: [String]
    
    public let groupId: String
    
    private let receiverUserIdentifier: String
    
    public static var supportsSecureCoding: Bool = true
    
    public let assetDescriptor: any SHAssetDescriptor
    
    public func encode(with coder: NSCoder) {
        // Convert to SHGenericAssetDescriptorClass
        let assetDescriptor = SHGenericAssetDescriptorClass(
            globalIdentifier: assetDescriptor.globalIdentifier,
            localIdentifier: assetDescriptor.localIdentifier,
            creationDate: assetDescriptor.creationDate,
            uploadState: assetDescriptor.uploadState,
            sharingInfo: assetDescriptor.sharingInfo as! SHGenericDescriptorSharingInfo
        )
        coder.encode(assetDescriptor, forKey: CodingKeys.assetDescriptor.rawValue)
        coder.encode(receiverUserIdentifier, forKey: CodingKeys.userIdentifier.rawValue)
    }
    
    public init(assetDescriptor: any SHAssetDescriptor,
                receiverUserIdentifier: String) {
        self.assetDescriptor = assetDescriptor
        self.eventOriginator = SHRemotePhantomUser(identifier: assetDescriptor.sharingInfo.sharedByUserIdentifier)
        self.receiverUserIdentifier = receiverUserIdentifier
        
        self.groupId = self.assetDescriptor.sharingInfo.sharedWithUserIdentifiersInGroup[receiverUserIdentifier] ?? ""
        self.invitedUsers = self.assetDescriptor.sharingInfo.groupInfoById[self.groupId]?.invitedUsersPhoneNumbers ?? []
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let descriptor = decoder.decodeObject(of: SHGenericAssetDescriptorClass.self, forKey: CodingKeys.assetDescriptor.rawValue)
        let receiverUserIdentifier = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.userIdentifier.rawValue)
        
        guard let descriptor = descriptor else {
            log.error("unexpected value for assetDescriptor when decoding SHDownloadRequestQueueItem object")
            return nil
        }
        guard let receiverUserIdentifier = receiverUserIdentifier as? String else {
            log.error("unexpected value for receiverUserIdentifier when decoding SHDownloadRequestQueueItem object")
            return nil
        }
        
        // Convert to SHGenericAssetDescriptors
        let assetDescriptor = SHGenericAssetDescriptor(
            globalIdentifier: descriptor.globalIdentifier,
            localIdentifier: descriptor.localIdentifier,
            creationDate: descriptor.creationDate,
            uploadState: descriptor.uploadState,
            sharingInfo: descriptor.sharingInfo
        )
        
        self.init(assetDescriptor: assetDescriptor,
                  receiverUserIdentifier: receiverUserIdentifier)
    }
}
