import Foundation
import KnowledgeBase

private let AssetKey = "asset"
private let AssetIdKey = "assetId"
private let VersionsKey = "versions"
private let IsBackgroundKey = "isBackground"
private let GlobalAssetIdKey = "globalAssetId"
private let GroupIdKey = "groupId"
private let EventOriginatorKey = "eventOriginator"
private let SharedWithKey = "sharedWith"
private let UserNameKey = "userName"
private let UserIdentifierKey = "userIdentifier"
private let PublicKeyKey = "publicKey"
private let PublicSignatureKey = "publicSignature"
private let AssetDescriptorKey = "assetDescriptor"
private let AssetShouldUploadKey = "shouldUpload"
private let IsPhotoMessageInThread = "isPhotoMessage"


/// A class (not a swift struct, such as SHRemoteUser) for SHServer objects
/// to conform to NSSecureCoding, and safely store sharing information in the KBStore.
/// This serialization method is  relevant when storing SHSerializableQueueItem
/// in the queue, and hold user sharing information.
public class SHRemoteUserClass: NSObject, NSSecureCoding {
    
    public static var supportsSecureCoding: Bool = true
    
    public let identifier: String
    public let name: String
    public let publicKeyData: Data
    public let publicSignatureData: Data
    
    enum CodingKeys: String, CodingKey {
        case identifier
        case name
        case publicKeyData = "publicKey"
        case publicSignatureData = "publicSignature"
    }
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.identifier, forKey: UserIdentifierKey)
        coder.encode(self.name, forKey: UserNameKey)
        coder.encode(self.publicKeyData.base64EncodedString(), forKey: PublicKeyKey)
        coder.encode(self.publicSignatureData.base64EncodedString(), forKey: PublicSignatureKey)
    }
    
    public init(identifier: String, name: String, publicKeyData: Data, publicSignatureData: Data) {
        self.identifier = identifier
        self.name = name
        self.publicKeyData = publicKeyData
        self.publicSignatureData = publicSignatureData
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let identifier = decoder.decodeObject(of: NSString.self, forKey: UserIdentifierKey)
        let name = decoder.decodeObject(of: NSString.self, forKey: UserNameKey)
        let publicKeyDataBase64 = decoder.decodeObject(of: NSString.self, forKey: PublicKeyKey)
        let publicSignatureDataBase64 = decoder.decodeObject(of: NSString.self, forKey: PublicSignatureKey)
        
        guard let identifier = identifier as? String else {
            log.error("unexpected value for identifier when decoding SHRemoteUserClass object")
            return nil
        }
        guard let name = name as? String else {
            log.error("unexpected value for name when decoding SHRemoteUserClass object")
            return nil
        }
        guard let publicKeyDataBase64 = publicKeyDataBase64 as? String,
              let publicKeyData = Data(base64Encoded: publicKeyDataBase64) else {
            log.error("unexpected value for publicKey when decoding SHRemoteUserClass object")
            return nil
        }
        guard let publicSignatureDataBase64 = publicSignatureDataBase64 as? String,
              let publicSignatureData = Data(base64Encoded: publicSignatureDataBase64) else {
            log.error("unexpected value for publicSignature when decoding SHRemoteUserClass object")
            return nil
        }
        
        self.init(identifier: identifier,
                  name: name,
                  publicKeyData: publicKeyData,
                  publicSignatureData: publicSignatureData)
    }
}

/// A class (not a swift struct, such as SHRemoteUser) for SHServer objects
/// to conform to NSSecureCoding, and safely store sharing information in the KBStore.
/// This serialization method is  relevant when storing SHGroupableUploadQueueItem
/// in the queue, and hold user sharing information.
public class SHGenericAssetDescriptorClass: NSObject, NSSecureCoding {
    
    public static var supportsSecureCoding: Bool = true
    
    public let globalIdentifier: String
    public let localIdentifier: String?
    public let creationDate: Date?
    public let uploadState: SHAssetDescriptorUploadState
    public let sharingInfo: SHDescriptorSharingInfo
    
    enum CodingKeys: String, CodingKey {
        case globalIdentifier
        case localIdentifier
        case creationDate
        case uploadState
        case sharingInfo
    }
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.globalIdentifier, forKey: CodingKeys.globalIdentifier.rawValue)
        coder.encode(self.localIdentifier, forKey: CodingKeys.localIdentifier.rawValue)
        coder.encode(self.creationDate, forKey: CodingKeys.creationDate.rawValue)
        coder.encode(self.uploadState.rawValue, forKey: CodingKeys.uploadState.rawValue)
        let encodableSharingInfo = try? JSONEncoder().encode(self.sharingInfo as! SHGenericDescriptorSharingInfo)
        coder.encode(encodableSharingInfo, forKey: CodingKeys.sharingInfo.rawValue)
    }
    
    public init(globalIdentifier: String,
                localIdentifier: String?,
                creationDate: Date?,
                uploadState: SHAssetDescriptorUploadState,
                sharingInfo: SHGenericDescriptorSharingInfo) {
        self.globalIdentifier = globalIdentifier
        self.localIdentifier = localIdentifier
        self.creationDate = creationDate
        self.uploadState = uploadState
        self.sharingInfo = sharingInfo
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let globalIdentifier = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.globalIdentifier.rawValue)
        let localIdentifier = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.localIdentifier.rawValue)
        let creationDate = decoder.decodeObject(of: NSDate.self, forKey: CodingKeys.creationDate.rawValue)
        let uploadStateStr = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.uploadState.rawValue)
        let sharingInfoData = decoder.decodeObject(of: NSData.self, forKey: CodingKeys.sharingInfo.rawValue)
                
        guard let globalIdentifier = globalIdentifier as? String else {
            log.error("unexpected value for globalIdentifier when decoding SHGenericAssetDescriptorClass object")
            return nil
        }
        guard let uploadStateStr = uploadStateStr as? String,
              let uploadState = SHAssetDescriptorUploadState(rawValue: uploadStateStr)
        else {
            log.error("unexpected value for uploadState when decoding SHGenericAssetDescriptorClass object")
            return nil
        }
        guard let sharingInfoData = sharingInfoData as? Data,
              let sharingInfo = try? JSONDecoder().decode(SHGenericDescriptorSharingInfo.self, from: sharingInfoData) else {
            log.error("unexpected value for sharingInfo when decoding SHGenericAssetDescriptorClass object")
            return nil
        }
        
        self.init(
            globalIdentifier: globalIdentifier,
            localIdentifier: localIdentifier as? String,
            creationDate: creationDate as? Date,
            uploadState: uploadState,
            sharingInfo: sharingInfo
        )
    }
}

public protocol SHSerializableQueueItem: NSCoding {
    func enqueue(in queue: KBQueueStore, with identifier: String) throws
}

extension SHSerializableQueueItem {
    public func enqueue(in queue: KBQueueStore, with identifier: String) throws {
        let data = try? NSKeyedArchiver.archivedData(withRootObject: self, requiringSecureCoding: true)
        if let data = data {
            try queue.enqueue(data, withIdentifier: identifier)
        } else {
            throw SHBackgroundOperationError.fatalError("failed to enqueue item with id \(identifier)")
        }
    }
    
    internal func insert(in queue: KBQueueStore, with identifier: String, at date: Date) throws {
        let data = try? NSKeyedArchiver.archivedData(withRootObject: self, requiringSecureCoding: true)
        if let data = data {
            try queue.insert(data, withIdentifier: identifier, timestamp: date)
        } else {
            throw SHBackgroundOperationError.fatalError("failed to insert item with id \(identifier)")
        }
    }
}

public protocol SHGroupableQueueItem: SHSerializableQueueItem {
    var groupId: String { get }
}

public protocol SHShareableGroupableQueueItem: SHGroupableQueueItem {
    var localIdentifier: String { get }
    var eventOriginator: SHServerUser { get }
    var sharedWith: [SHServerUser] { get }
    
    /// Helper to determine if the sender (eventOriginator)
    /// is sharing it with recipients other than self
    var isSharingWithOtherUsers: Bool { get }
}

public extension SHShareableGroupableQueueItem {
    var isSharingWithOtherUsers: Bool {
        return sharedWith.count > 0
    }
}

public protocol SHOutboundShareableGroupableQueueItem: SHShareableGroupableQueueItem {
    var isPhotoMessage: Bool { get }
}

public class SHAbstractOutboundShareableGroupableQueueItem: NSObject, SHOutboundShareableGroupableQueueItem {
    
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
    public let eventOriginator: SHServerUser
    public let sharedWith: [SHServerUser] // Empty if it's just a backup request
    
    public let isPhotoMessage: Bool
    
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
                eventOriginator: SHServerUser,
                sharedWith users: [SHServerUser],
                isPhotoMessage: Bool,
                isBackground: Bool = false) {
        self.localIdentifier = localIdentifier
        self.versions = SHAbstractOutboundShareableGroupableQueueItem.recommendedVersions(forSharingWith: users)
        self.groupId = groupId
        self.eventOriginator = eventOriginator
        self.sharedWith = users
        self.isBackground = isBackground
        self.isPhotoMessage = isPhotoMessage
    }
    
    public init(localIdentifier: String,
                versions: [SHAssetQuality],
                groupId: String,
                eventOriginator: SHServerUser,
                sharedWith users: [SHServerUser],
                isPhotoMessage: Bool,
                isBackground: Bool = false) {
        self.localIdentifier = localIdentifier
        self.versions = versions
        self.groupId = groupId
        self.eventOriginator = eventOriginator
        self.sharedWith = users
        self.isBackground = isBackground
        self.isPhotoMessage = isPhotoMessage
    }
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.localIdentifier, forKey: AssetIdKey)
        coder.encode(NSNumber(booleanLiteral: self.isBackground), forKey: IsBackgroundKey)
        coder.encode(self.versions.map({ $0.rawValue }), forKey: VersionsKey)
        coder.encode(self.groupId, forKey: GroupIdKey)
        // Convert to SHRemoteUserClass
        let remoteSender = SHRemoteUserClass(identifier: self.eventOriginator.identifier,
                                             name: self.eventOriginator.name,
                                             publicKeyData: self.eventOriginator.publicKeyData,
                                             publicSignatureData: self.eventOriginator.publicSignatureData)
        coder.encode(remoteSender, forKey: EventOriginatorKey)
        let remoteReceivers = self.sharedWith.map {
            SHRemoteUserClass(identifier: $0.identifier, name: $0.name, publicKeyData: $0.publicKeyData, publicSignatureData: $0.publicSignatureData)
        }
        coder.encode(remoteReceivers, forKey: SharedWithKey)
        coder.encode(NSNumber(booleanLiteral: self.isPhotoMessage), forKey: IsPhotoMessageInThread)
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let assetId = decoder.decodeObject(of: NSString.self, forKey: AssetIdKey)
        let versions = decoder.decodeObject(of: [NSArray.self, NSString.self], forKey: VersionsKey)
        let groupId = decoder.decodeObject(of: NSString.self, forKey: GroupIdKey)
        let sender = decoder.decodeObject(of: SHRemoteUserClass.self, forKey: EventOriginatorKey)
        let receivers = decoder.decodeObject(of: [NSArray.self, SHRemoteUserClass.self], forKey: SharedWithKey)
        let bg = decoder.decodeObject(of: NSNumber.self, forKey: IsBackgroundKey)
        let isPhotoMessage = decoder.decodeObject(of: NSNumber.self, forKey: IsPhotoMessageInThread)
        
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
                  isPhotoMessage: isPhotoMessage?.boolValue ?? false,
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

public class SHLocalFetchRequestQueueItem: SHAbstractOutboundShareableGroupableQueueItem, NSSecureCoding {
    
    public static var supportsSecureCoding: Bool = true
    
    public let globalIdentifier: String?
    
    public let shouldUpload: Bool
    
    public init(localIdentifier: String,
                globalIdentifier: String? = nil,
                groupId: String,
                eventOriginator: SHServerUser,
                sharedWith users: [SHServerUser],
                shouldUpload: Bool,
                isPhotoMessage: Bool,
                isBackground: Bool = false) {
        self.globalIdentifier = globalIdentifier
        self.shouldUpload = shouldUpload
        super.init(localIdentifier: localIdentifier,
                   groupId: groupId,
                   eventOriginator: eventOriginator,
                   sharedWith: users,
                   isPhotoMessage: isPhotoMessage,
                   isBackground: isBackground)
    }
    
    public init(localIdentifier: String,
                globalIdentifier: String? = nil,
                versions: [SHAssetQuality],
                groupId: String,
                eventOriginator: SHServerUser,
                sharedWith users: [SHServerUser],
                shouldUpload: Bool,
                isPhotoMessage: Bool,
                isBackground: Bool = false) {
        self.globalIdentifier = globalIdentifier
        self.shouldUpload = shouldUpload
        super.init(localIdentifier: localIdentifier,
                   versions: versions,
                   groupId: groupId,
                   eventOriginator: eventOriginator,
                   sharedWith: users,
                   isPhotoMessage: isPhotoMessage,
                   isBackground: isBackground)
    }
    
    public override func encode(with coder: NSCoder) {
        super.encode(with: coder)
        coder.encode(self.globalIdentifier, forKey: GlobalAssetIdKey)
        coder.encode(NSNumber(booleanLiteral: self.shouldUpload), forKey: AssetShouldUploadKey)
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        if let superSelf = SHAbstractOutboundShareableGroupableQueueItem(coder: decoder) {
            let globalAssetId = decoder.decodeObject(of: NSString.self, forKey: GlobalAssetIdKey)
            let shouldUpload = decoder.decodeObject(of: NSNumber.self, forKey: AssetShouldUploadKey)
            
            guard let su = shouldUpload else {
                log.error("unexpected value for shouldUpload when decoding SHLocalFetchRequestQueueItem object")
                return nil
            }
            
            self.init(localIdentifier: superSelf.localIdentifier,
                      globalIdentifier: globalAssetId as? String,
                      versions: superSelf.versions,
                      groupId: superSelf.groupId,
                      eventOriginator: superSelf.eventOriginator,
                      sharedWith: superSelf.sharedWith,
                      shouldUpload: su.boolValue,
                      isPhotoMessage: superSelf.isPhotoMessage,
                      isBackground: superSelf.isBackground)
            return
        }
       
        return nil
    }
}

/// On-disk representation of an upload queue item.
/// References a group id, which is the unique identifier of the request.
public class SHConcreteEncryptionRequestQueueItem: SHAbstractOutboundShareableGroupableQueueItem, NSSecureCoding {
    
    public static var supportsSecureCoding: Bool = true
    
    public let asset: SHApplePhotoAsset
    
    public init(asset: SHApplePhotoAsset,
                versions: [SHAssetQuality],
                groupId: String,
                eventOriginator: SHServerUser,
                sharedWith users: [SHServerUser] = [],
                isPhotoMessage: Bool,
                isBackground: Bool = false) {
        self.asset = asset
        super.init(localIdentifier: asset.phAsset.localIdentifier,
                   versions: versions,
                   groupId: groupId,
                   eventOriginator: eventOriginator,
                   sharedWith: users,
                   isPhotoMessage: isPhotoMessage,
                   isBackground: isBackground)
    }
    
    public override func encode(with coder: NSCoder) {
        super.encode(with: coder)
        coder.encode(self.asset, forKey: AssetKey)
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        if let superSelf = SHAbstractOutboundShareableGroupableQueueItem(coder: decoder) {
            let asset = decoder.decodeObject(of: SHApplePhotoAsset.self, forKey: AssetKey)
            
            guard let asset = asset else {
                log.error("unexpected value for asset when decoding SHEncryptionRequestQueueItem object")
                return nil
            }
            
            self.init(asset: asset,
                      versions: superSelf.versions,
                      groupId: superSelf.groupId,
                      eventOriginator: superSelf.eventOriginator,
                      sharedWith: superSelf.sharedWith,
                      isPhotoMessage: superSelf.isPhotoMessage,
                      isBackground: superSelf.isBackground)
            return
        }
       
        return nil
    }
}

public class SHConcreteShareableGroupableQueueItem: SHAbstractOutboundShareableGroupableQueueItem, NSSecureCoding {
    
    public static var supportsSecureCoding: Bool = true
    
    public let globalAssetId: String
    
    public init(localAssetId: String,
                globalAssetId: String,
                versions: [SHAssetQuality],
                groupId: String,
                eventOriginator: SHServerUser,
                sharedWith users: [SHServerUser] = [],
                isPhotoMessage: Bool,
                isBackground: Bool = false) {
        self.globalAssetId = globalAssetId
        super.init(localIdentifier: localAssetId,
                   versions: versions,
                   groupId: groupId,
                   eventOriginator: eventOriginator,
                   sharedWith: users,
                   isPhotoMessage: isPhotoMessage,
                   isBackground: isBackground)
    }
    
    public override func encode(with coder: NSCoder) {
        super.encode(with: coder)
        coder.encode(self.globalAssetId, forKey: GlobalAssetIdKey)
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        if let superSelf = SHAbstractOutboundShareableGroupableQueueItem(coder: decoder) {
            let globalAssetId = decoder.decodeObject(of: NSString.self, forKey: GlobalAssetIdKey)
            
            guard let globalAssetId = globalAssetId as? String else {
                log.error("unexpected value for globalAssetId when decoding SHConcreteShareableGroupableQueueItem object")
                return nil
            }
            
            self.init(
                localAssetId: superSelf.localIdentifier,
                globalAssetId: globalAssetId,
                versions: superSelf.versions,
                groupId: superSelf.groupId,
                eventOriginator: superSelf.eventOriginator,
                sharedWith: superSelf.sharedWith,
                isPhotoMessage: superSelf.isPhotoMessage,
                isBackground: superSelf.isBackground
            )
            return
        }
        
        return nil
    }
}

public class SHFailedQueueItem: SHAbstractOutboundShareableGroupableQueueItem, NSSecureCoding {
    public static var supportsSecureCoding: Bool = true
    
    public required convenience init?(coder decoder: NSCoder) {
        if let superSelf = SHAbstractOutboundShareableGroupableQueueItem(coder: decoder) {
            self.init(localIdentifier: superSelf.localIdentifier,
                      versions: superSelf.versions,
                      groupId: superSelf.groupId,
                      eventOriginator: superSelf.eventOriginator,
                      sharedWith: superSelf.sharedWith,
                      isPhotoMessage: superSelf.isPhotoMessage,
                      isBackground: superSelf.isBackground)
            return
        }
       
        return nil
    }
}

public typealias SHEncryptionRequestQueueItem = SHConcreteEncryptionRequestQueueItem
public typealias SHEncryptionForSharingRequestQueueItem = SHConcreteEncryptionRequestQueueItem
public typealias SHUploadRequestQueueItem = SHConcreteShareableGroupableQueueItem
public typealias SHUploadHistoryItem = SHConcreteShareableGroupableQueueItem
public typealias SHShareHistoryItem = SHConcreteShareableGroupableQueueItem
public typealias SHFailedShareRequestQueueItem = SHFailedQueueItem
public typealias SHFailedUploadRequestQueueItem = SHFailedQueueItem


/// A placeholder for a SHServerUser when only its identifier is available
public struct SHRemotePhantomUser : SHServerUser {
    public let identifier: String
    public let name: String = ""
    public let publicKeyData: Data = "".data(using: .utf8)!
    public let publicSignatureData: Data = "".data(using: .utf8)!
    
    public init(identifier: String) {
        self.identifier = identifier
    }
}

public class SHDownloadRequestQueueItem: NSObject, NSSecureCoding, SHSerializableQueueItem, SHShareableGroupableQueueItem {
    public var localIdentifier: String {
        self.assetDescriptor.globalIdentifier
    }
    
    public let eventOriginator: SHServerUser
    public let sharedWith: [SHServerUser] = []
    
    public var groupId: String
    
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
        coder.encode(assetDescriptor, forKey: AssetDescriptorKey)
        coder.encode(receiverUserIdentifier, forKey: UserIdentifierKey)
    }
    
    public init(assetDescriptor: any SHAssetDescriptor,
                receiverUserIdentifier: String) {
        self.assetDescriptor = assetDescriptor
        self.eventOriginator = SHRemotePhantomUser(identifier: assetDescriptor.sharingInfo.sharedByUserIdentifier)
        self.receiverUserIdentifier = receiverUserIdentifier
        
        /// The relevant group id is the group id used to share this asset with the receiver
        for (userId, groupId) in self.assetDescriptor.sharingInfo.sharedWithUserIdentifiersInGroup {
            if userId == receiverUserIdentifier {
                self.groupId = groupId
                return
            }
        }
        self.groupId = ""
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let descriptor = decoder.decodeObject(of: SHGenericAssetDescriptorClass.self, forKey: AssetDescriptorKey)
        let receiverUserIdentifier = decoder.decodeObject(of: NSString.self, forKey: UserIdentifierKey)
        
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