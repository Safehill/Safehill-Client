//
//  Context.swift
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

public struct SHShareablePayload {
    public let ephemeralPublicKeyData: Data
    public let cyphertext: Data
    public let signature: Data
    let recipient: SHCryptoUser?
    
    public init(ephemeralPublicKeyData: Data,
         cyphertext: Data,
         signature: Data,
         recipient: SHCryptoUser? = nil) {
        self.ephemeralPublicKeyData = ephemeralPublicKeyData
        self.cyphertext = cyphertext
        self.signature = signature
        self.recipient = recipient
    }
}
