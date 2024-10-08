import Foundation

public class SHGenericShareableGroupableQueueItem: NSObject, SHShareableGroupableQueueItem, NSSecureCoding {
    
    public static var supportsSecureCoding: Bool = true
    
    enum CodingKeys: String, CodingKey {
        case asset
        case versions
        case groupId
        case eventOriginator
        case sharedWith
        case invitedUsers
        case asPhotoMessageInThreadId
        case isBackground
    }
    
    public var identifier: String {
        return SHUploadPipeline.queueItemIdentifier(
            groupId: self.groupId,
            assetLocalIdentifier: self.asset.localIdentifier,
            versions: self.versions,
            users: self.sharedWith
        )
    }
    
    public let asset: SHUploadableAsset
    public let versions: [SHAssetQuality]
    public let groupId: String
    public let eventOriginator: any SHServerUser
    public let sharedWith: [any SHServerUser] // Empty if it's just a backup request
    public let invitedUsers: [String]
    public let asPhotoMessageInThreadId: String?
    ///
    /// If set to true avoids side-effects, such as:
    /// -  calling the delegates
    /// - enqueueing in FAILED queues (as it assumes these will be tried at a later time)
    ///
    public let isBackground: Bool
    
    public init(asset: SHUploadableAsset,
                versions: [SHAssetQuality],
                groupId: String,
                eventOriginator: any SHServerUser,
                sharedWith users: [any SHServerUser],
                invitedUsers: [String],
                asPhotoMessageInThreadId: String?,
                isBackground: Bool) {
        self.asset = asset
        self.versions = versions
        self.groupId = groupId
        self.eventOriginator = eventOriginator
        self.sharedWith = users
        self.invitedUsers = invitedUsers
        self.asPhotoMessageInThreadId = asPhotoMessageInThreadId
        self.isBackground = isBackground
    }
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.asset, forKey: CodingKeys.asset.rawValue)
        coder.encode(self.versions.map({ $0.rawValue }), forKey: CodingKeys.versions.rawValue)
        coder.encode(self.groupId, forKey: CodingKeys.groupId.rawValue)
        // Convert to SHRemoteUserClass
        let remoteSender = SHRemoteUserClass(
            identifier: self.eventOriginator.identifier,
            name: self.eventOriginator.name,
            publicKeyData: self.eventOriginator.publicKeyData,
            publicSignatureData: self.eventOriginator.publicSignatureData
        )
        coder.encode(remoteSender, forKey: CodingKeys.eventOriginator.rawValue)
        let remoteReceivers = self.sharedWith.map {
            SHRemoteUserClass(
                identifier: $0.identifier,
                name: $0.name,
                publicKeyData: $0.publicKeyData,
                publicSignatureData: $0.publicSignatureData
            )
        }
        coder.encode(remoteReceivers, forKey: CodingKeys.sharedWith.rawValue)
        coder.encode(self.invitedUsers, forKey: CodingKeys.invitedUsers.rawValue)
        coder.encode(self.asPhotoMessageInThreadId, forKey: CodingKeys.asPhotoMessageInThreadId.rawValue)
        coder.encode(NSNumber(booleanLiteral: self.isBackground), forKey: CodingKeys.isBackground.rawValue)
        
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let asset = decoder.decodeObject(of: SHUploadableAsset.self, forKey: CodingKeys.asset.rawValue)
        let versions = decoder.decodeObject(of: [NSArray.self, NSString.self], forKey: CodingKeys.versions.rawValue)
        let groupId = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.groupId.rawValue)
        let sender = decoder.decodeObject(of: SHRemoteUserClass.self, forKey: CodingKeys.eventOriginator.rawValue)
        let receivers = decoder.decodeObject(of: [NSArray.self, SHRemoteUserClass.self], forKey: CodingKeys.sharedWith.rawValue)
        let invitedUsers = decoder.decodeObject(of: [NSArray.self, NSString.self], forKey: CodingKeys.invitedUsers.rawValue)
        let bg = decoder.decodeObject(of: NSNumber.self, forKey: CodingKeys.isBackground.rawValue)
        let asPhotoMessageInThreadId = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.asPhotoMessageInThreadId.rawValue)
        
        guard let asset = asset else {
            log.error("unexpected value for asset when decoding SHEncryptionRequestQueueItem object")
            return nil
        }
        
        guard let versions = versions as? [String] else {
            log.error("unexpected value for versions when decoding \(Self.Type.self) object")
            return nil
        }
        let parsedVersions: [SHAssetQuality]
        do {
            parsedVersions = try versions.map({
                let quality = SHAssetQuality(rawValue: $0)
                if quality == nil {
                    throw SHBackgroundOperationError.fatalError("unexpected asset version \($0)")
                }
                return quality!
            })
        } catch {
            log.error("unexpected value for versions when decoding \(Self.Type.self) object. \(error)")
            return nil
        }
        
        guard let groupId = groupId as? String else {
            log.error("unexpected value for groupId when decoding \(Self.Type.self) object")
            return nil
        }
        
        guard let sender = sender else {
            log.error("unexpected value for eventOriginator when decoding \(Self.Type.self) object")
            return nil
        }
        
        guard let receivers = receivers as? [SHRemoteUserClass] else {
            log.error("unexpected value for sharedWith when decoding \(Self.Type.self) object")
            return nil
        }
        
        guard let invitedUsers = invitedUsers as? [String] else {
            log.error("unexpected value for invitedUsers when decoding \(Self.Type.self) object")
            return nil
        }
        
        guard let asPhotoMessageInThreadId = asPhotoMessageInThreadId as String? else {
            log.error("unexpected value for asPhotoMessageInThreadId when decoding \(Self.Type.self) object")
            return nil
        }
        
        // Convert to SHRemoteUser
        let remoteSender = SHRemoteUser(identifier: sender.identifier,
                                        name: sender.name,
                                        publicKeyData: sender.publicKeyData,
                                        publicSignatureData: sender.publicSignatureData)
        let remoteReceivers = receivers.map {
            SHRemoteUser(identifier: $0.identifier,
                         name: $0.name,
                         publicKeyData: $0.publicKeyData,
                         publicSignatureData: $0.publicSignatureData
            )
        }
        
        guard let isBg = bg else {
            log.error("unexpected value for isBackground when decoding \(Self.Type.self) object")
            return nil
        }
        
        self.init(asset: asset,
                  versions: parsedVersions,
                  groupId: groupId,
                  eventOriginator: remoteSender,
                  sharedWith: remoteReceivers,
                  invitedUsers: invitedUsers,
                  asPhotoMessageInThreadId: asPhotoMessageInThreadId,
                  isBackground: isBg.boolValue)
    }
}

extension SHGenericShareableGroupableQueueItem {
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
