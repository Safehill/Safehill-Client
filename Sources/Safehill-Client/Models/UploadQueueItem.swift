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
private let AssetDescriptorsKey = "assetDescriptors"


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
    public let sharedByUserIdentifier: String
    public let sharedWithUserIdentifiers: [String]
    
    enum CodingKeys: String, CodingKey {
        case globalIdentifier
        case localIdentifier
        case creationDate
        case sharedByUserIdentifier
        case sharedWithUserIdentifiers
    }
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.globalIdentifier, forKey: CodingKeys.globalIdentifier.rawValue)
        coder.encode(self.localIdentifier, forKey: CodingKeys.localIdentifier.rawValue)
        coder.encode(self.creationDate, forKey: CodingKeys.creationDate.rawValue)
        coder.encode(self.sharedByUserIdentifier, forKey: CodingKeys.sharedByUserIdentifier.rawValue)
        coder.encode(self.sharedWithUserIdentifiers, forKey: CodingKeys.sharedWithUserIdentifiers.rawValue)
    }
    
    public init(globalIdentifier: String, localIdentifier: String?, creationDate: Date?, sharedByUserIdentifier: String, sharedWithUserIdentifiers: [String]) {
        self.globalIdentifier = globalIdentifier
        self.localIdentifier = localIdentifier
        self.creationDate = creationDate
        self.sharedByUserIdentifier = sharedByUserIdentifier
        self.sharedWithUserIdentifiers = sharedWithUserIdentifiers
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let globalIdentifier = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.globalIdentifier.rawValue)
        let localIdentifier = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.localIdentifier.rawValue)
        let creationDate = decoder.decodeObject(of: NSDate.self, forKey: CodingKeys.creationDate.rawValue)
        let sharedByUserIdentifier = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.sharedByUserIdentifier.rawValue)
        let sharedWithUserIdentifiers = decoder.decodeObject(of: [NSArray.self, NSString.self], forKey: CodingKeys.sharedWithUserIdentifiers.rawValue)
                
        guard let globalIdentifier = globalIdentifier as String? else {
            log.error("unexpected value for globalIdentifier when decoding SHFGenericAssetDescriptorClass object")
            return nil
        }
        guard let localIdentifier = localIdentifier as String? else {
            log.error("unexpected value for localIdentifier when decoding SHFGenericAssetDescriptorClass object")
            return nil
        }
        guard let creationDate = creationDate as Date? else {
            log.error("unexpected value for creationDate when decoding SHFGenericAssetDescriptorClass object")
            return nil
        }
        guard let sharedByUserIdentifier = sharedByUserIdentifier as String? else {
            log.error("unexpected value for sharedByUserIdentifier when decoding SHFGenericAssetDescriptorClass object")
            return nil
        }
        guard let sharedWithUserIdentifiers = sharedWithUserIdentifiers as? [String] else {
            log.error("unexpected value for sharedWithUserIdentifiers when decoding SHFGenericAssetDescriptorClass object")
            return nil
        }
        
        self.init(
            globalIdentifier: globalIdentifier,
            localIdentifier: localIdentifier,
            creationDate: creationDate,
            sharedByUserIdentifier: sharedByUserIdentifier,
            sharedWithUserIdentifiers: sharedWithUserIdentifiers
        )
    }
}

public protocol SHSerializableQueueItem: NSSecureCoding {
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

/// On-disk representation of an upload queue item.
/// References a group id, which is the unique identifier of the request.
public class SHEncryptionRequestQueueItem: NSObject, NSSecureCoding, SHShareableGroupableQueueItem {
    
    public static var supportsSecureCoding: Bool = true
    
    public let asset: KBPhotoAsset
    public let groupId: String
    public let sharedWith: [SHServerUser] // Empty if it's just a backup request
    
    public var assetId: String {
        asset.phAsset.localIdentifier
    }
    
    public init(asset: KBPhotoAsset, groupId: String, sharedWith users: [SHServerUser] = []) {
        self.asset = asset
        self.groupId = groupId
        self.sharedWith = users
    }
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.asset, forKey: AssetKey)
        coder.encode(self.groupId, forKey: GroupIdKey)
        // Convert to SHRemoteUserClass
        let remoteUsers = self.sharedWith.map {
            SHRemoteUserClass(identifier: $0.identifier, name: $0.name, publicKeyData: $0.publicKeyData, publicSignatureData: $0.publicSignatureData)
        }
        coder.encode(remoteUsers, forKey: SharedWithKey)
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let asset = decoder.decodeObject(of: KBPhotoAsset.self, forKey: AssetKey)
        let groupId = decoder.decodeObject(of: NSString.self, forKey: GroupIdKey)
        let users = decoder.decodeObject(of: [NSArray.self, SHRemoteUserClass.self], forKey: SharedWithKey)
        
        guard let asset = asset as KBPhotoAsset? else {
            log.error("unexpected value for asset when decoding SHEncryptionRequestQueueItem object")
            return nil
        }
        
        guard let groupId = groupId as String? else {
            log.error("unexpected value for groupId when decoding SHEncryptionRequestQueueItem object")
            return nil
        }
        
        guard let users = users as? [SHRemoteUserClass] else {
            log.error("unexpected value for sharedWith when decoding SHEncryptionRequestQueueItem object")
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
        
        self.init(asset: asset, groupId: groupId, sharedWith: remoteUsers)
    }
}

public class SHUploadRequestQueueItem: NSObject, NSSecureCoding, SHShareableGroupableQueueItem {
    
    public static var supportsSecureCoding: Bool = true
    
    public let localAssetId: String
    public let globalAssetId: String
    public let groupId: String
    public let sharedWith: [SHServerUser] // Empty if it's just a backup request
    
    public var assetId: String {
        localAssetId
    }
    
    public init(localAssetId: String, globalAssetId: String, groupId: String, sharedWith users: [SHServerUser] = []) {
        self.localAssetId = localAssetId
        self.globalAssetId = globalAssetId
        self.groupId = groupId
        self.sharedWith = users
    }
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.localAssetId, forKey: LocalAssetIdKey)
        coder.encode(self.globalAssetId, forKey: GlobalAssetIdKey)
        coder.encode(self.groupId, forKey: GroupIdKey)
        // Convert to SHRemoteUserClass
        let remoteUsers = self.sharedWith.map {
            SHRemoteUserClass(identifier: $0.identifier,
                              name: $0.name,
                              publicKeyData: $0.publicKeyData,
                              publicSignatureData: $0.publicSignatureData
            )
        }
        coder.encode(remoteUsers, forKey: SharedWithKey)
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let localAssetId = decoder.decodeObject(of: NSString.self, forKey: LocalAssetIdKey)
        let globalAssetId = decoder.decodeObject(of: NSString.self, forKey: GlobalAssetIdKey)
        let groupId = decoder.decodeObject(of: NSString.self, forKey: GroupIdKey)
        let users = decoder.decodeObject(of: [NSArray.self, SHRemoteUserClass.self], forKey: SharedWithKey)
        
        guard let localAssetId = localAssetId as String? else {
            log.error("unexpected value for localAssetId when decoding SHUploadRequestQueueItem object")
            return nil
        }
        
        guard let globalAssetId = globalAssetId as String? else {
            log.error("unexpected value for globalAssetId when decoding SHUploadRequestQueueItem object")
            return nil
        }
        
        guard let groupId = groupId as String? else {
            log.error("unexpected value for groupId when decoding SHUploadRequestQueueItem object")
            return nil
        }
        
        guard let users = users as? [SHRemoteUserClass] else {
            log.error("unexpected value for sharedWith when decoding SHUploadRequestQueueItem object")
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
        
        self.init(localAssetId: localAssetId, globalAssetId: globalAssetId, groupId: groupId, sharedWith: remoteUsers)
    }
}

public class SHEncryptionForSharingRequestQueueItem: SHEncryptionRequestQueueItem {}

//public class SHShareRequestQueueItem: SHUploadRequestQueueItem {}

public class SHUploadHistoryItem: NSObject, NSSecureCoding, SHShareableGroupableQueueItem {
    
    public static var supportsSecureCoding: Bool = true
    
    public let assetId: String
    public let groupId: String
    public let sharedWith: [SHServerUser] // Empty if it's just a backup request
    
    public init(assetId: String, groupId: String, sharedWith users: [SHServerUser]) {
        self.assetId = assetId
        self.groupId = groupId
        self.sharedWith = users
    }
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.assetId, forKey: AssetIdKey)
        coder.encode(self.groupId, forKey: GroupIdKey)
        // Convert to SHRemoteUserClass
        let remoteUsers = self.sharedWith.map {
            SHRemoteUserClass(identifier: $0.identifier,
                              name: $0.name,
                              publicKeyData: $0.publicKeyData,
                              publicSignatureData: $0.publicSignatureData
            )
        }
        coder.encode(remoteUsers, forKey: SharedWithKey)
    }

    public required convenience init?(coder decoder: NSCoder) {
        let assetId = decoder.decodeObject(of: NSString.self, forKey: AssetIdKey)
        let groupId = decoder.decodeObject(of: NSString.self, forKey: GroupIdKey)
        let users = decoder.decodeObject(of: [NSArray.self, SHRemoteUserClass.self], forKey: SharedWithKey)

        guard let assetId = assetId as String? else {
            log.error("unexpected value for assetId when decoding SHUploadHistoryItem object")
            return nil
        }

        guard let groupId = groupId as String? else {
            log.error("unexpected value for groupId when decoding SHUploadHistoryItem object")
            return nil
        }

        guard let users = users as? [SHRemoteUserClass] else {
            log.error("unexpected value for sharedWith when decoding SHEncryptionRequestQueueItem object")
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

        self.init(assetId: assetId, groupId: groupId, sharedWith: remoteUsers)
    }
}

public class SHShareHistoryItem: SHUploadHistoryItem {}

public class SHFailedUploadRequestQueueItem: SHUploadHistoryItem {}

public class SHFailedShareRequestQueueItem: SHUploadHistoryItem {}

public class SHDownloadRequestQueueItem: NSObject, NSSecureCoding, SHSerializableQueueItem {
    
    public static var supportsSecureCoding: Bool = true
    
    public let assetDescriptor: SHAssetDescriptor
    
    public func encode(with coder: NSCoder) {
        // Convert to SHGenericAssetDescriptorClass
        let assetDescriptor = SHGenericAssetDescriptorClass(
            globalIdentifier: assetDescriptor.globalIdentifier,
            localIdentifier: assetDescriptor.localIdentifier,
            creationDate: assetDescriptor.creationDate,
            sharedByUserIdentifier: assetDescriptor.sharedByUserIdentifier,
            sharedWithUserIdentifiers: assetDescriptor.sharedWithUserIdentifiers
        )
        coder.encode(assetDescriptor, forKey: AssetDescriptorsKey)
    }
    
    public init(assetDescriptor: SHAssetDescriptor) {
        self.assetDescriptor = assetDescriptor
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let descriptors = decoder.decodeObject(of: SHGenericAssetDescriptorClass.self, forKey: AssetDescriptorsKey)
        guard let descriptor = descriptors else {
            log.error("unexpected value for assetDescriptor when decoding SHDownloadRequestQueueItem object")
            return nil
        }
        
        // Convert to SHGenericAssetDescriptors
        let assetDescriptor = SHGenericAssetDescriptor(
            globalIdentifier: descriptor.globalIdentifier,
            localIdentifier: descriptor.localIdentifier,
            creationDate: descriptor.creationDate,
            sharedByUserIdentifier: descriptor.sharedByUserIdentifier,
            sharedWithUserIdentifiers: descriptor.sharedWithUserIdentifiers
        )
        
        self.init(assetDescriptor: assetDescriptor)
    }
}
