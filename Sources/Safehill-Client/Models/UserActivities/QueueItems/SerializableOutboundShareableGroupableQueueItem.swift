import Foundation
import KnowledgeBase

public class SHAbstractOutboundShareableGroupableQueueItem: NSObject, SHOutboundShareableGroupableQueueItem {
    
    enum CodingKeys: String, CodingKey {
        case identifier = "assetId"
        case isBackground
        case versions
        case groupId
        case eventOriginator
        case sharedWith
        case invitedUsers
        case asPhotoMessageInThreadId
    }
    
    public var identifier: String {
        return SHUploadPipeline.queueItemIdentifier(
            groupId: self.groupId,
            assetLocalIdentifier: self.localIdentifier,
            versions: self.versions,
            users: self.sharedWith
        )
    }
    
    public let localIdentifier: String
    
    ///
    /// The `versions` to fetch/encrypt/upload/share as part of this request
    ///
    public let versions: [SHAssetQuality]
    public let groupId: String
    public let eventOriginator: any SHServerUser
    public let sharedWith: [any SHServerUser] // Empty if it's just a backup request
    public let invitedUsers: [String]
    
    public let asPhotoMessageInThreadId: String?
    
    ///
    /// The recommended versions based on the type of request.
    /// If the requested is constructed with no versions specified, these versions will be used for this request.
    /// To be precise, this method controls the logic around which versions to upload based on whether the purpose is to share the asset or just upload it.
    /// If shared (because we want the recipient to see it asap) we upload a surrogate for the `.hiResolution` first, namely the `.midResolution`.
    /// - Parameter forSharingWith: the list of users to share it with
    /// - Returns: the list of asset versions
    ///
    public static func recommendedVersions(forSharingWith users: [SHServerUser]) -> [SHAssetQuality] {
        if users.count > 0 {
            return [.lowResolution, .midResolution]
        } else {
            return [.lowResolution, .hiResolution]
        }
    }
    
    ///
    /// If set to true avoids side-effects, such as:
    /// -  calling the delegates
    /// - enqueueing in FAILED queues (as it assumes these will be tried at a later time)
    ///
    public let isBackground: Bool
    
    public init(localIdentifier: String,
                groupId: String,
                eventOriginator: any SHServerUser,
                sharedWith users: [any SHServerUser],
                invitedUsers: [String],
                asPhotoMessageInThreadId: String?,
                isBackground: Bool = false) {
        self.localIdentifier = localIdentifier
        self.versions = SHAbstractOutboundShareableGroupableQueueItem.recommendedVersions(forSharingWith: users)
        self.groupId = groupId
        self.eventOriginator = eventOriginator
        self.sharedWith = users
        self.invitedUsers = invitedUsers
        self.isBackground = isBackground
        self.asPhotoMessageInThreadId = asPhotoMessageInThreadId
    }
    
    public init(localIdentifier: String,
                versions: [SHAssetQuality],
                groupId: String,
                eventOriginator: any SHServerUser,
                sharedWith users: [any SHServerUser],
                invitedUsers: [String],
                asPhotoMessageInThreadId: String?,
                isBackground: Bool = false) {
        self.localIdentifier = localIdentifier
        self.versions = versions
        self.groupId = groupId
        self.eventOriginator = eventOriginator
        self.sharedWith = users
        self.invitedUsers = invitedUsers
        self.isBackground = isBackground
        self.asPhotoMessageInThreadId = asPhotoMessageInThreadId
    }
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.localIdentifier, forKey: CodingKeys.identifier.rawValue)
        coder.encode(NSNumber(booleanLiteral: self.isBackground), forKey: CodingKeys.isBackground.rawValue)
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
        coder.encode(invitedUsers, forKey: CodingKeys.invitedUsers.rawValue)
        coder.encode(self.asPhotoMessageInThreadId, forKey: CodingKeys.asPhotoMessageInThreadId.rawValue)
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let assetId = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.identifier.rawValue)
        let versions = decoder.decodeObject(of: [NSArray.self, NSString.self], forKey: CodingKeys.versions.rawValue)
        let groupId = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.groupId.rawValue)
        let sender = decoder.decodeObject(of: SHRemoteUserClass.self, forKey: CodingKeys.eventOriginator.rawValue)
        let receivers = decoder.decodeObject(of: [NSArray.self, SHRemoteUserClass.self], forKey: CodingKeys.sharedWith.rawValue)
        let invitedUsers = decoder.decodeObject(of: [NSArray.self, NSString.self], forKey: CodingKeys.invitedUsers.rawValue)
        let bg = decoder.decodeObject(of: NSNumber.self, forKey: CodingKeys.isBackground.rawValue)
        let asPhotoMessageInThreadId = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.asPhotoMessageInThreadId.rawValue)
        
        guard let assetId = assetId as? String else {
            log.error("unexpected value for assetId when decoding \(Self.Type.self) object")
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
        
        self.init(localIdentifier: assetId,
                  versions: parsedVersions,
                  groupId: groupId,
                  eventOriginator: remoteSender,
                  sharedWith: remoteReceivers,
                  invitedUsers: invitedUsers,
                  asPhotoMessageInThreadId: asPhotoMessageInThreadId as String?,
                  isBackground: isBg.boolValue)
    }
    
    public func enqueue(in queue: KBQueueStore) throws {
        let data = try? NSKeyedArchiver.archivedData(withRootObject: self, requiringSecureCoding: true)
        if let data = data {
            try queue.enqueue(data, withIdentifier: self.identifier)
        } else {
            throw SHBackgroundOperationError.fatalError("failed to enqueue item with id \(identifier)")
        }
    }
    
    public func insert(in queue: KBQueueStore, at timestamp: Date) throws {
        let data = try? NSKeyedArchiver.archivedData(withRootObject: self, requiringSecureCoding: true)
        if let data = data {
            try queue.insert(data, withIdentifier: self.identifier, timestamp: timestamp)
        } else {
            throw SHBackgroundOperationError.fatalError("failed to enqueue item with id \(identifier)")
        }
    }
}
