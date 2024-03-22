import Safehill_Crypto
import CryptoKit

extension SHLocalUser {
    
    ///
    /// If the user key and signature are missing from the iCloud-synchronizable keychain,
    /// but they are present in the non-syncronizable keychain,
    /// copy the items from the latter to the former, and delete such keys from the latter.
    ///
    public static func upgradeKeychain(keychainPrefix: String) throws {
        
        let (privateKey, privateSignature) = try SHLocalCryptoUser.keysInKeychain(
            label: keychainPrefix,
            synchronizable: true
        )
        
        if privateKey != nil, privateSignature != nil {
            return
        }
        
        let (oldPrivateKey, oldPrivateSignature) = try SHLocalCryptoUser.keysInKeychain(
            label: keychainPrefix,
            synchronizable: false
        )
        
        if privateKey == nil, let oldPrivateKey {
            try SHLocalCryptoUser.storeKeyInKeychain(
                oldPrivateKey,
                label: keychainPrefix,
                synchronizable: true,
                force: true
            )
        }
        
        if privateSignature == nil, let oldPrivateSignature {
            try SHLocalCryptoUser.storeSignatureInKeychain(
                oldPrivateSignature,
                label: keychainPrefix,
                synchronizable: true,
                force: true
            )
        }
        
        try SHLocalCryptoUser.deleteKeysInKeychain(
            withLabel: keychainPrefix,
            synchronizable: false
        )
    }
}

