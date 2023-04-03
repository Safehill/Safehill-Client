//
//  EncryptedData.swift
//  
//
//  Created by Gennaro Frazzingaro on 9/22/21.
//

import Foundation
#if os(Linux)
@_exported import Crypto
#else
import CryptoKit
#endif

public struct SHEncryptedData {
    /// privateSecret should never be shared
    public let privateSecret: SymmetricKey
    public let encryptedData: Data
    
    public init(privateSecret: SymmetricKey, clearData: Data) throws {
        self.privateSecret = privateSecret
        self.encryptedData = try SHCypher.encrypt(clearData, using: privateSecret)
    }
    
    public init(clearData: Data) throws {
        let secret = SymmetricKey(size: .bits256)
        try self.init(privateSecret: secret, clearData: clearData)
    }
}

