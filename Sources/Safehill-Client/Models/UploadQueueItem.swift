//
//  UploadQueueItem.swift
//  Enkey
//
//  Created by Gennaro on 23/02/22.
//

import Foundation
import KnowledgeBase

public protocol SHGroupableUploadQueueItem: NSSecureCoding {
    var groupId: String { get }
    var assetId: String { get }
    
    func enqueue(in queue: KBQueueStore, with identifier: String) throws
}

extension SHGroupableUploadQueueItem {
    public func enqueue(in queue: KBQueueStore, with identifier: String) throws {
        let data = try? NSKeyedArchiver.archivedData(withRootObject: self, requiringSecureCoding: true)
        if let data = data {
            try queue.enqueue(data, withIdentifier: identifier)
        } else {
            throw KBError.fatalError("failed to enqueue asset with id \(identifier)")
        }
    }
}


private let AssetKey = "asset"
private let AssetIdKey = "AssetId"
private let LocalAssetIdKey = "localAssetId"
private let GlobalAssetIdKey = "globalAssetId"
private let GroupIdKey = "groupId"

/// On-disk representation of an upload queue item.
/// References a group id, which is the unique identifier of the request.
public class SHEncryptionRequestQueueItem: NSObject, SHGroupableUploadQueueItem {
    
    public static var supportsSecureCoding: Bool = true
    
    public let asset: KBPhotoAsset
    public let groupId: String
    
    public var assetId: String {
        asset.phAsset.localIdentifier
    }
    
    public init(asset: KBPhotoAsset, groupId: String) {
        self.asset = asset
        self.groupId = groupId
    }
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.asset, forKey: AssetKey)
        coder.encode(self.groupId, forKey: GroupIdKey)
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let asset = decoder.decodeObject(of: KBPhotoAsset.self, forKey: AssetKey)
        let groupId = decoder.decodeObject(of: NSString.self, forKey: GroupIdKey)
        
        guard let asset = asset as KBPhotoAsset? else {
            log.error("unexpected value for asset when decoding SHEncryptionRequestQueueItem object")
            return nil
        }
        
        guard let groupId = groupId as String? else {
            log.error("unexpected value for groupId when decoding SHEncryptionRequestQueueItem object")
            return nil
        }
        
        self.init(asset: asset, groupId: groupId)
    }
}

public class SHUploadRequestQueueItem: NSObject, NSSecureCoding, SHGroupableUploadQueueItem {
    
    public static var supportsSecureCoding: Bool = true
    
    public let localAssetId: String
    public let globalAssetId: String
    public let groupId: String
    
    public var assetId: String {
        localAssetId
    }
    
    public init(localAssetId: String, globalAssetId: String, groupId: String) {
        self.localAssetId = localAssetId
        self.globalAssetId = globalAssetId
        self.groupId = groupId
    }
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.localAssetId, forKey: LocalAssetIdKey)
        coder.encode(self.globalAssetId, forKey: GlobalAssetIdKey)
        coder.encode(self.groupId, forKey: GroupIdKey)
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let localAssetId = decoder.decodeObject(of: NSString.self, forKey: LocalAssetIdKey)
        let globalAssetId = decoder.decodeObject(of: NSString.self, forKey: GlobalAssetIdKey)
        let groupId = decoder.decodeObject(of: NSString.self, forKey: GroupIdKey)
        
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
        
        self.init(localAssetId: localAssetId, globalAssetId: globalAssetId, groupId: groupId)
    }
}

public class SHUploadHistoryItem: NSObject, NSSecureCoding, SHGroupableUploadQueueItem {
    
    public static var supportsSecureCoding: Bool = true
    
    public let assetId: String
    public let groupId: String
    
    public init(assetId: String, groupId: String) {
        self.assetId = assetId
        self.groupId = groupId
    }
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.assetId, forKey: AssetIdKey)
        coder.encode(self.groupId, forKey: GroupIdKey)
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        let assetId = decoder.decodeObject(of: NSString.self, forKey: AssetIdKey)
        let groupId = decoder.decodeObject(of: NSString.self, forKey: GroupIdKey)
        
        guard let assetId = assetId as String? else {
            log.error("unexpected value for assetId when decoding SHUploadHistoryItem object")
            return nil
        }
        
        guard let groupId = groupId as String? else {
            log.error("unexpected value for groupId when decoding SHUploadHistoryItem object")
            return nil
        }
        
        self.init(assetId: assetId, groupId: groupId)
    }
}
