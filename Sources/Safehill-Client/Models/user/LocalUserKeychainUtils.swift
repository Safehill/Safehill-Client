import Safehill_Crypto
import CryptoKit

extension SHLocalUser {
    
    ///
    /// If the user key and signature are missing from the non-synchronized keychain,
    /// but they are present in the iCloud-synchronizable keychain,
    /// copy the items from the latter to the former.
    ///
    public static func syncKeychainItemsIfMissing(keychainPrefix: String) throws {
        
        let keysKeychainLabel = SHLocalUser.keysKeychainLabel(keychainPrefix: keychainPrefix)
        
        let (privateKey, privateSignature) = try SHLocalCryptoUser.keysInKeychain(
            label: keysKeychainLabel,
            synchronizable: false
        )
        
        if privateKey != nil, privateSignature != nil {
            return
        }
        
        try self.copySyncKeychainToLocal(keychainPrefix: keychainPrefix)
    }
    
    ///
    /// Force copy the items from the non-synchronized keychain to the iCloud-synchronizable keychain
    ///
    public static func copyLocalKeychainToSync(keychainPrefix: String) throws {
        
        let keysKeychainLabel = SHLocalUser.keysKeychainLabel(keychainPrefix: keychainPrefix)
        
        let (localPrivateKey, localPrivateSignature) = try SHLocalCryptoUser.keysInKeychain(
            label: keysKeychainLabel,
            synchronizable: false
        )
        
        guard let localPrivateKey, let localPrivateSignature else {
            return
        }
        
        try SHLocalCryptoUser.storeKeyInKeychain(
            localPrivateKey,
            label: keysKeychainLabel,
            synchronizable: true,
            force: true
        )
        try SHLocalCryptoUser.storeSignatureInKeychain(
            localPrivateSignature,
            label: keysKeychainLabel,
            synchronizable: true,
            force: true
        )
    }
    
    ///
    /// Force copy the items from the iCloud-synchronizable keychain to the the non-synchronized one
    ///
    public static func copySyncKeychainToLocal(keychainPrefix: String) throws {
        
        let keysKeychainLabel = SHLocalUser.keysKeychainLabel(keychainPrefix: keychainPrefix)
        
        let (syncPrivateKey, syncPrivateSignature) = try SHLocalCryptoUser.keysInKeychain(
            label: keysKeychainLabel,
            synchronizable: true
        )
        
        guard let syncPrivateKey, let syncPrivateSignature else {
            return
        }
        
        try SHLocalCryptoUser.storeKeyInKeychain(
            syncPrivateKey,
            label: keysKeychainLabel,
            synchronizable: false,
            force: true
        )
        try SHLocalCryptoUser.storeSignatureInKeychain(
            syncPrivateSignature,
            label: keysKeychainLabel,
            synchronizable: false,
            force: true
        )
    }
}

