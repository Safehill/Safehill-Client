import Foundation
import Safehill_Crypto


// MARK: Store auth token in the keychaing

extension SHKeychain {
    static func retrieveValue(from account: String) throws -> String? {
        // Seek a generic password with the given account.
        let query = [kSecClass: kSecClassGenericPassword,
                     kSecAttrAccount: account,
                     kSecUseDataProtectionKeychain: true,
                     kSecReturnData: true] as [String: Any]

        // Find and cast the result as data.
        var item: CFTypeRef?
        switch SecItemCopyMatching(query as CFDictionary, &item) {
        case errSecSuccess:
            guard let data = item as? Data else { return nil }
            return String(data: data, encoding: .utf8)
        case errSecItemNotFound: return nil
        case let status: throw SHKeychain.Error.unexpectedStatus(status)
        }
    }
    
    static func storeValue(_ token: String, account: String) throws {
        guard let tokenData = token.data(using: .utf8) else {
            throw SHKeychain.Error.generic("Unable to convert string to data.")
        }
        
        // Treat the key data as a generic password.
        let query = [kSecClass: kSecClassGenericPassword,
                     kSecAttrAccount: account,
                     kSecAttrAccessible: kSecAttrAccessibleWhenUnlocked,
                     kSecUseDataProtectionKeychain: true,
                     kSecValueData: tokenData] as [String: Any]

        // Add the key data.
        let status = SecItemAdd(query as CFDictionary, nil)
        guard status == errSecSuccess else {
            throw SHKeychain.Error.unexpectedStatus(status)
        }
        
        log.debug("Successfully saved value \(token, privacy: .private) in account \(account)")
    }
    
    static func deleteValue(account: String) throws {
        let query = [kSecClass: kSecClassGenericPassword,
                     kSecAttrAccount: account] as [String: Any]
        
        let status = SecItemDelete(query as CFDictionary)
        guard status == errSecSuccess || status == errSecItemNotFound else { throw SHKeychain.Error.unexpectedStatus(status) }
        
#if DEBUG
        log.info("Successfully deleted account \(account)")
#endif
    }
}
