//
//  AssetUploader.swift
//  Safehill-Client
//
//  Created by Gennaro Frazzingaro on 8/26/21.
//

import Foundation

open class SHUploadOperation: SHAbstractBackgroundOperation, SHBackgroundOperationProtocol {
    
    public let limit: Int?
    public let user: SHLocalUser
    
    public init(user: SHLocalUser, limitPerRun limit: Int? = nil) {
        self.user = user
        self.limit = limit
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
        delayedStartInSeconds: 5,
        dispatchIntervalInSeconds: 10
    )
    
    private override init(delayedStartInSeconds: Int = 0,
                          dispatchIntervalInSeconds: Int? = nil) {
        super.init(dispatchIntervalInSeconds: dispatchIntervalInSeconds)
    }
}
