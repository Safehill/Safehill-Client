//
//  UploadQueueItem.swift
//  Enkey
//
//  Created by Gennaro on 23/02/22.
//

import Foundation
import KnowledgeBase

private let AssetKey = "asset"
private let AssetIdKey = "assetId"
private let LocalAssetIdKey = "localAssetId"
private let GlobalAssetIdKey = "globalAssetId"
private let GroupIdKey = "groupId"
private let SharedWithKey = "sharedWith"
private let ShouldUploadAssetKey = "shouldUploadAsset"
private let UserNameKey = "userName"
private let UserIdentifierKey = "userIdentifier"
private let PublicKeyKey = "publicKey"
private let PublicSignatureKey = "publicSignature"
private let AssetDescriptorKey = "assetDescriptor"
private let AssetShouldUploadKey = "shouldUpload"


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
        
        guard let identifier = identifier as String? else {
            log.error("unexpected value for identifier when decoding SHRemoteUserClass object")
            return nil
        }
        guard let name = name as String? else {
            log.error("unexpected value for name when decoding SHRemoteUserClass object")
            return nil
        }
        guard let publicKeyDataBase64 = publicKeyDataBase64 as String?,
              let publicKeyData = Data(base64Encoded: publicKeyDataBase64) else {
            log.error("unexpected value for publicKey when decoding SHRemoteUserClass object")
            return nil
        }
        guard let publicSignatureDataBase64 = publicSignatureDataBase64 as String?,
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
    public let sharingInfo: SHDescriptorSharingInfo
    
    enum CodingKeys: String, CodingKey {
        case globalIdentifier
        case localIdentifier
        case creationDate
        case sharingInfo
    }
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.globalIdentifier, forKey: CodingKeys.globalIdentifier.rawValue)
        coder.encode(self.localIdentifier, forKey: CodingKeys.localIdentifier.rawValue)
        coder.encode(self.creationDate, forKey: CodingKeys.creationDate.rawValue)
        let encodableSharingInfo = try? JSONEncoder().encode(self.sharingInfo as! SHGenericDescriptorSharingInfo)
        coder.encode(encodableSharingInfo, forKey: CodingKeys.sharingInfo.rawValue)
    }
    
    public init(globalIdentifier: String,
                localIdentifier: String?,
                creationDate: Date?,
                sharingInfo: SHGenericDescriptorSharingInfo) {
        self.globalIdentifier = globalIdentifier
        self.localIdentifier = localIdentifier
        self.creationDate = creationDate
        self.sharingInfo = sharingInfo
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let globalIdentifier = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.globalIdentifier.rawValue)
        let localIdentifier = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.localIdentifier.rawValue)
        let creationDate = decoder.decodeObject(of: NSDate.self, forKey: CodingKeys.creationDate.rawValue)
        let sharingInfoData = decoder.decodeObject(of: [NSData.self], forKey: CodingKeys.sharingInfo.rawValue) as! Data
                
        guard let globalIdentifier = globalIdentifier as String? else {
            log.error("unexpected value for globalIdentifier when decoding SHGenericAssetDescriptorClass object")
            return nil
        }
        guard let localIdentifier = localIdentifier as String? else {
            log.error("unexpected value for localIdentifier when decoding SHGenericAssetDescriptorClass object")
            return nil
        }
        guard let creationDate = creationDate as Date? else {
            log.error("unexpected value for creationDate when decoding SHGenericAssetDescriptorClass object")
            return nil
        }
        guard let sharingInfo = try? JSONDecoder().decode(SHGenericDescriptorSharingInfo.self, from: sharingInfoData) else {
            log.error("unexpected value for sharingInfo when decoding SHGenericAssetDescriptorClass object")
            return nil
        }
        
        self.init(
            globalIdentifier: globalIdentifier,
            localIdentifier: localIdentifier,
            creationDate: creationDate,
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
            throw KBError.fatalError("failed to enqueue asset with id \(identifier)")
        }
    }
}

public protocol SHGroupableQueueItem: SHSerializableQueueItem {
    var groupId: String { get }
}

public protocol SHShareableGroupableQueueItem: SHGroupableQueueItem {
    var assetId: String { get }
    var sharedWith: [SHServerUser] { get }
}

public class SHAbstractShareableGroupableQueueItem: NSObject, SHShareableGroupableQueueItem {
    
    public let assetId: String
    public let groupId: String
    public let sharedWith: [SHServerUser] // Empty if it's just a backup request
    
    public init(localIdentifier: String, groupId: String, sharedWith users: [SHServerUser]) {
        self.assetId = localIdentifier
        self.groupId = groupId
        self.sharedWith = users
    }
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.assetId, forKey: AssetIdKey)
        coder.encode(self.groupId, forKey: GroupIdKey)
        // Convert to SHRemoteUserClass
        let remoteUsers = self.sharedWith.map {
            SHRemoteUserClass(identifier: $0.identifier, name: $0.name, publicKeyData: $0.publicKeyData, publicSignatureData: $0.publicSignatureData)
        }
        coder.encode(remoteUsers, forKey: SharedWithKey)
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let assetId = decoder.decodeObject(of: NSString.self, forKey: AssetIdKey)
        let groupId = decoder.decodeObject(of: NSString.self, forKey: GroupIdKey)
        let users = decoder.decodeObject(of: [NSArray.self, SHRemoteUserClass.self], forKey: SharedWithKey)
        
        guard let assetId = assetId as String? else {
            log.error("unexpected value for assetId when decoding \(Self.Type.self) object")
            return nil
        }
        
        guard let groupId = groupId as String? else {
            log.error("unexpected value for groupId when decoding \(Self.Type.self) object")
            return nil
        }
        
        guard let users = users as? [SHRemoteUserClass] else {
            log.error("unexpected value for sharedWith when decoding \(Self.Type.self) object")
            return nil
        }
        // Convert to SHRemoteUser
        let remoteUsers = users.map {
            SHRemoteUser(identifier: $0.identifier,
                         name: $0.name,
                         email: nil,
                         publicKeyData: $0.publicKeyData,
                         publicSignatureData: $0.publicSignatureData
            )
        }
        
        self.init(localIdentifier: assetId, groupId: groupId, sharedWith: remoteUsers)
    }
}

public class SHLocalFetchRequestQueueItem: SHAbstractShareableGroupableQueueItem, NSSecureCoding {
    
    public static var supportsSecureCoding: Bool = true
    
    public let shouldUpload: Bool
    
    public init(localIdentifier: String, groupId: String, sharedWith users: [SHServerUser], shouldUpload: Bool) {
        self.shouldUpload = shouldUpload
        super.init(localIdentifier: localIdentifier,
                   groupId: groupId,
                   sharedWith: users)
    }
    
    public override func encode(with coder: NSCoder) {
        super.encode(with: coder)
        coder.encode(NSNumber(booleanLiteral: self.shouldUpload), forKey: AssetShouldUploadKey)
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        if let superSelf = SHAbstractShareableGroupableQueueItem(coder: decoder) {
            let shouldUpload = decoder.decodeObject(of: NSNumber.self, forKey: AssetShouldUploadKey)
            
            guard let shouldUpload = shouldUpload?.boolValue else {
                log.error("unexpected value for shouldUpload when decoding SHLocalFetchRequestQueueItem object")
                return nil
            }
            
            self.init(localIdentifier: superSelf.assetId,
                      groupId: superSelf.groupId,
                      sharedWith: superSelf.sharedWith,
                      shouldUpload: shouldUpload)
            return
        }
       
        return nil
    }
}

/// On-disk representation of an upload queue item.
/// References a group id, which is the unique identifier of the request.
public class SHConcreteEncryptionRequestQueueItem: SHAbstractShareableGroupableQueueItem, NSSecureCoding {
    
    public static var supportsSecureCoding: Bool = true
    
    public let asset: KBPhotoAsset
    
    public init(asset: KBPhotoAsset,
                groupId: String, sharedWith users: [SHServerUser] = []) {
        self.asset = asset
        super.init(localIdentifier: asset.phAsset.localIdentifier,
                   groupId: groupId,
                   sharedWith: users)
    }
    
    public override func encode(with coder: NSCoder) {
        super.encode(with: coder)
        coder.encode(self.asset, forKey: AssetKey)
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        if let superSelf = SHAbstractShareableGroupableQueueItem(coder: decoder) {
            let asset = decoder.decodeObject(of: KBPhotoAsset.self, forKey: AssetKey)
            
            guard let asset = asset as KBPhotoAsset? else {
                log.error("unexpected value for asset when decoding SHEncryptionRequestQueueItem object")
                return nil
            }
            
            self.init(asset: asset, groupId: superSelf.groupId, sharedWith: superSelf.sharedWith)
            return
        }
       
        return nil
    }
}

public class SHUploadRequestQueueItem: SHAbstractShareableGroupableQueueItem, NSSecureCoding {
    
    public static var supportsSecureCoding: Bool = true
    
    public let globalAssetId: String
    
    public init(localAssetId: String, globalAssetId: String, groupId: String, sharedWith users: [SHServerUser] = []) {
        self.globalAssetId = globalAssetId
        super.init(localIdentifier: localAssetId, groupId: groupId, sharedWith: users)
    }
    
    public override func encode(with coder: NSCoder) {
        super.encode(with: coder)
        coder.encode(self.globalAssetId, forKey: GlobalAssetIdKey)
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        if let superSelf = SHAbstractShareableGroupableQueueItem(coder: decoder) {
            let globalAssetId = decoder.decodeObject(of: NSString.self, forKey: GlobalAssetIdKey)
            
            guard let globalAssetId = globalAssetId as String? else {
                log.error("unexpected value for globalAssetId when decoding SHUploadRequestQueueItem object")
                return nil
            }
            
            self.init(
                localAssetId: superSelf.assetId,
                globalAssetId: globalAssetId,
                groupId: superSelf.groupId,
                sharedWith: superSelf.sharedWith
            )
            return
        }
        
        return nil
    }
}

public class SHConcreteShareableGroupableQueueItem: SHAbstractShareableGroupableQueueItem, NSSecureCoding {
    public static var supportsSecureCoding: Bool = true
    
    public required convenience init?(coder decoder: NSCoder) {
        if let superSelf = SHAbstractShareableGroupableQueueItem(coder: decoder) {
            self.init(localIdentifier: superSelf.assetId,
                      groupId: superSelf.groupId,
                      sharedWith: superSelf.sharedWith)
            return
        }
       
        return nil
    }
}

public typealias SHEncryptionRequestQueueItem = SHConcreteEncryptionRequestQueueItem
public typealias SHEncryptionForSharingRequestQueueItem = SHConcreteEncryptionRequestQueueItem
public typealias SHUploadHistoryItem = SHConcreteShareableGroupableQueueItem
public typealias SHShareHistoryItem = SHConcreteShareableGroupableQueueItem
public typealias SHFailedShareRequestQueueItem = SHConcreteShareableGroupableQueueItem
public typealias SHFailedUploadRequestQueueItem = SHConcreteShareableGroupableQueueItem

public class SHDownloadRequestQueueItem: NSObject, NSSecureCoding, SHSerializableQueueItem, SHShareableGroupableQueueItem {
    public var assetId: String {
        self.assetDescriptor.globalIdentifier
    }
    
    public let sharedWith: [SHServerUser] = []
    
    public var groupId: String
    
    private let selfUserPublicId: String
    
    public static var supportsSecureCoding: Bool = true
    
    public let assetDescriptor: SHAssetDescriptor
    
    public func encode(with coder: NSCoder) {
        // Convert to SHGenericAssetDescriptorClass
        let assetDescriptor = SHGenericAssetDescriptorClass(
            globalIdentifier: assetDescriptor.globalIdentifier,
            localIdentifier: assetDescriptor.localIdentifier,
            creationDate: assetDescriptor.creationDate,
            sharingInfo: assetDescriptor.sharingInfo as! SHGenericDescriptorSharingInfo
        )
        coder.encode(assetDescriptor, forKey: AssetDescriptorKey)
        coder.encode(selfUserPublicId, forKey: UserIdentifierKey)
    }
    
    public init(assetDescriptor: SHAssetDescriptor, selfUserPublicId: String) {
        self.assetDescriptor = assetDescriptor
        self.selfUserPublicId = selfUserPublicId
        for (userId, groupId) in self.assetDescriptor.sharingInfo.sharedWithUserIdentifiersInGroup {
            if userId == selfUserPublicId {
                self.groupId = groupId
                return
            }
        }
        self.groupId = ""
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let descriptor = decoder.decodeObject(of: SHGenericAssetDescriptorClass.self, forKey: AssetDescriptorKey)
        let selfUserPublicId = decoder.decodeObject(of: NSString.self, forKey: UserIdentifierKey)
        
        guard let descriptor = descriptor else {
            log.error("unexpected value for assetDescriptor when decoding SHDownloadRequestQueueItem object")
            return nil
        }
        guard let selfUserPublicId = selfUserPublicId as? String else {
            log.error("unexpected value for selfUserPublicId when decoding SHDownloadRequestQueueItem object")
            return nil
        }
        
        // Convert to SHGenericAssetDescriptors
        let assetDescriptor = SHGenericAssetDescriptor(
            globalIdentifier: descriptor.globalIdentifier,
            localIdentifier: descriptor.localIdentifier,
            creationDate: descriptor.creationDate,
            sharingInfo: descriptor.sharingInfo
        )
        
        self.init(assetDescriptor: assetDescriptor, selfUserPublicId: selfUserPublicId)
    }
}
