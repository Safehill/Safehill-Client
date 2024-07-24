import Foundation

/// 
/// A placeholder for a SHServerUser when only its identifier is available
/// 
public struct SHRemotePhantomUser : SHServerUser {
    public let identifier: String
    public let name: String = ""
    public let publicKeyData: Data = "".data(using: .utf8)!
    public let publicSignatureData: Data = "".data(using: .utf8)!
    
    public init(identifier: String) {
        self.identifier = identifier
    }
}

