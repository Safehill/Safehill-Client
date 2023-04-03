//
//  Signing.swift
//  
//
//  Created by Gennaro Frazzingaro on 9/1/21.
//

import Foundation
#if os(Linux)
@_exported import Crypto
#else
import CryptoKit
import LocalAuthentication
#endif

public struct SHHash {
    
    public static func stringDigest(for data: Data) -> String {
        return SHA512.hash(data: data).compactMap { String(format: "%02hhx", $0) }.joined()
    }
    
    public static func dataDigest(for data: Data) -> Data {
        return SHA512.hash(data: data).withUnsafeBytes {
            return Data(Array($0))
        }
    }
    
    private static func digest(forFileAtPath path: String) -> SHA512.Digest {
        var hasher = SHA512()
        let stream = InputStream(fileAtPath: path)!
        stream.open()
        let bufferSize = 512
        let buffer = UnsafeMutablePointer<UInt8>.allocate(capacity: bufferSize)
        while stream.hasBytesAvailable {
            let read = stream.read(buffer, maxLength: bufferSize)
            let bufferPointer = UnsafeRawBufferPointer(start: buffer,
                                                       count: read)
            hasher.update(bufferPointer: bufferPointer)
        }
        return hasher.finalize()
    }
    
    public static func stringDigest(forFileAtPath path: String) -> String {
        return self.digest(forFileAtPath: path).compactMap { String(format: "%02hhx", $0) }.joined()
    }
    
    public static func dataDigest(forFileAtPath path: String) -> Data {
        return self.digest(forFileAtPath: path).withUnsafeBytes {
            return Data(Array($0))
        }
    }
}

public struct SHSignature {
    let account: String?
    
    public init(saveToKeychainAccount account: String? = nil) {
        self.account = account
    }
    
    private func temporarySignature(for transactionData: Data) throws -> P256.Signing.ECDSASignature {
        let privateKey = P256.Signing.PrivateKey()
#if !os(Linux)
        if let account = account {
            try SHKeychain.storeKey(privateKey, label: account)
        }
#endif
        return try privateKey.signature(for: transactionData)
    }
    
    public func temporarySignature(for transactionData: Data,
                                   description: String,
                                   durationInSeconds: TimeInterval? = nil) throws -> P256.Signing.ECDSASignature {
#if os(Linux)
        return try self.temporarySignature(for: transactionData)
#else
        
        if !SecureEnclave.isAvailable {
            return try self.temporarySignature(for: transactionData)
        } else {
            // Request for biometric authentication
            let accessControl = SecAccessControlCreateWithFlags(nil,
                                                                kSecAttrAccessibleWhenUnlockedThisDeviceOnly,
                                                                [.privateKeyUsage, .userPresence],
                                                                nil)!
            // Keep user authenticated for durationInSeconds (if present)
            var authContext: LAContext? = nil
            if let durationInSeconds = durationInSeconds {
                authContext = LAContext()
                authContext!.touchIDAuthenticationAllowableReuseDuration = durationInSeconds
                authContext!.localizedReason = description
            }
            
            let privateKey = try SecureEnclave.P256.Signing.PrivateKey(accessControl: accessControl,
                                                                       authenticationContext: authContext)
            if let account = account {
                try SHKeychain.storeKey(privateKey, account: account)
            }
            let digest512 = SHA512.hash(data: transactionData)
            return try! privateKey.signature(for: Data(digest512))
        }
#endif
    }

    public static func validateSignature(for data: Data,
                                         digest: Data,
                                         signatureForData: P256.Signing.ECDSASignature,
                                         signatureForDigest: P256.Signing.ECDSASignature,
                                         receivedFrom user: SHRemoteCryptoUser) -> Bool {
        guard user.signature.isValidSignature(signatureForData, for: data) else {
            return false
        }
        log.info("the expected user sent this data.")
        
        guard user.signature.isValidSignature(signatureForDigest, for: digest) else {
          return false
        }
        
        log.info("data received == data sent.")
        return true
    }

}

