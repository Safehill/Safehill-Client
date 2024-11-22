import Foundation

public class SHDownloadRequestQueueItem: NSObject, NSSecureCoding, SHSerializableQueueItem, SHShareableGroupableQueueItem {
    
    enum CodingKeys: String, CodingKey {
        case assetDescriptor
        case userIdentifier
        case groupId
    }
    
    public let eventOriginator: any SHServerUser
    public let sharedWith: [any SHServerUser] = []
    public let invitedUsers: [String]
    
    public let groupId: String
    
    private let receiverUserIdentifier: UserIdentifier
    
    public static var supportsSecureCoding: Bool = true
    
    public let assetDescriptor: any SHAssetDescriptor
    
    public func encode(with coder: NSCoder) {
        // Convert to SHGenericAssetDescriptorClass
        let assetDescriptor = SHGenericAssetDescriptorClass(
            globalIdentifier: assetDescriptor.globalIdentifier,
            localIdentifier: assetDescriptor.localIdentifier,
            perceptualHash: assetDescriptor.perceptualHash,
            creationDate: assetDescriptor.creationDate,
            uploadState: assetDescriptor.uploadState,
            sharingInfo: assetDescriptor.sharingInfo as! SHGenericDescriptorSharingInfo
        )
        coder.encode(assetDescriptor, forKey: CodingKeys.assetDescriptor.rawValue)
        coder.encode(receiverUserIdentifier, forKey: CodingKeys.userIdentifier.rawValue)
    }
    
    public init(assetDescriptor: any SHAssetDescriptor,
                receiverUserIdentifier: String,
                groupId: String) {
        self.assetDescriptor = assetDescriptor
        self.eventOriginator = SHRemotePhantomUser(identifier: assetDescriptor.sharingInfo.sharedByUserIdentifier)
        self.receiverUserIdentifier = receiverUserIdentifier
        
        self.groupId = groupId
        if let invitedUsersPhoneNumbers = self.assetDescriptor.sharingInfo.groupInfoById[self.groupId]?.invitedUsersPhoneNumbers {
            self.invitedUsers = Array(invitedUsersPhoneNumbers.keys)
        } else {
            self.invitedUsers = []
        }
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let descriptor = decoder.decodeObject(of: SHGenericAssetDescriptorClass.self, forKey: CodingKeys.assetDescriptor.rawValue)
        let receiverUserIdentifier = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.userIdentifier.rawValue)
        let groupId = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.groupId.rawValue)
        
        guard let descriptor = descriptor else {
            log.error("unexpected value for assetDescriptor when decoding SHDownloadRequestQueueItem object")
            return nil
        }
        guard let receiverUserIdentifier = receiverUserIdentifier as? String else {
            log.error("unexpected value for receiverUserIdentifier when decoding SHDownloadRequestQueueItem object")
            return nil
        }
        guard let groupId = groupId as? String else {
            log.error("unexpected value for groupId when decoding SHDownloadRequestQueueItem object")
            return nil
        }
        
        // Convert to SHGenericAssetDescriptors
        let assetDescriptor = SHGenericAssetDescriptor(
            globalIdentifier: descriptor.globalIdentifier,
            localIdentifier: descriptor.localIdentifier,
            perceptualHash: descriptor.perceptualHash,
            creationDate: descriptor.creationDate,
            uploadState: descriptor.uploadState,
            sharingInfo: descriptor.sharingInfo
        )
        
        self.init(assetDescriptor: assetDescriptor,
                  receiverUserIdentifier: receiverUserIdentifier,
                  groupId: groupId)
    }
}


extension SHDownloadRequestQueueItem {
    public var isSharingWithOtherSafehillUsers: Bool {
        return invitedUsers.count > 0
    }
    
    public var isSharingWithOrInvitingOtherUsers: Bool {
        return sharedWith.count + invitedUsers.count > 0
    }
    
    public var isOnlyInvitingUsers: Bool {
        return sharedWith.count == 0 && invitedUsers.count > 0
    }
}
