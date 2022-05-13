//
//  AssetUploader.swift
//  Safehill-Client
//
//  Created by Gennaro Frazzingaro on 8/26/21.
//

import Foundation
import os

public protocol SHAssetUploaderDelegate {
    func didStartUploading(itemWithLocalIdentifier: String, globalIdentifier: String, groupId: String)
    func didUpload(itemWithLocalIdentifier: String, globalIdentifier: String, groupId: String)
    func didFailUpload(itemWithLocalIdentifier: String, globalIdentifier: String, groupId: String)
}

open class SHUploadOperation: SHAbstractBackgroundOperation, SHBackgroundOperationProtocol {
    
    public let log = Logger(subsystem: "com.safehill", category: "BG-UPLOAD")
    
    public let limit: Int?
    public let user: SHLocalUser
    public let delegate: SHAssetUploaderDelegate?
    
    public init(user: SHLocalUser, delegate: SHAssetUploaderDelegate? = nil, limitPerRun limit: Int? = nil) {
        self.user = user
        self.limit = limit
        self.delegate = delegate
    }
    
    public var serverProxy: SHServerProxy {
        SHServerProxy(user: self.user)
    }
    
    public func clone() -> SHBackgroundOperationProtocol {
        SHUploadOperation(user: self.user, limitPerRun: self.limit)
    }
}

public class SHAssetsUploadQueueProcessor : SHOperationQueueProcessor<SHUploadOperation> {
    /// Singleton (with private initializer)
    public static var shared = SHAssetsUploadQueueProcessor(
        delayedStartInSeconds: 2,
        dispatchIntervalInSeconds: 2
    )
    
    private override init(delayedStartInSeconds: Int = 0,
                          dispatchIntervalInSeconds: Int? = nil) {
        super.init(dispatchIntervalInSeconds: dispatchIntervalInSeconds)
    }
}
