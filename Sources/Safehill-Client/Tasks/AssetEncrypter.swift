//
//  AssetEncrypter.swift
//  
//
//  Created by Gennaro on 06/11/21.
//

import Foundation

public protocol SHAssetEncrypterDelegate {
    func didStartEncryption(itemWithLocalIdentifier: String)
    func didEncrypt(itemWithLocalIdentifier: String, globalIdentifier: String)
    func didFailEncryption(itemWithLocalIdentifier: String)
}

open class SHEncryptionOperation: SHAbstractBackgroundOperation, SHBackgroundOperationProtocol {
    
    public let limit: Int?
    public let user: SHLocalUser
    public let delegate: SHAssetEncrypterDelegate?
    
    public init(user: SHLocalUser, delegate: SHAssetEncrypterDelegate? = nil, limitPerRun limit: Int? = nil) {
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

public class SHAssetsEncrypterQueueProcessor : SHOperationQueueProcessor<SHEncryptionOperation> {
    /// Singleton (with private initializer)
    public static var shared = SHAssetsEncrypterQueueProcessor(
        delayedStartInSeconds: 5,
        dispatchIntervalInSeconds: 10
    )
    
    private override init(delayedStartInSeconds: Int = 0,
                          dispatchIntervalInSeconds: Int? = nil) {
        super.init(dispatchIntervalInSeconds: dispatchIntervalInSeconds)
    }
}
