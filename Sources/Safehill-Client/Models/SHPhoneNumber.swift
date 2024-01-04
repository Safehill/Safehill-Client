import Safehill_Crypto

public struct SHPhoneNumber: Hashable {
    public let number: String
    public let label: String?
    
    init(_ phoneNumber: String, label: String? = nil) {
        self.number = phoneNumber
        self.label = label
    }
    
    public var hashedPhoneNumber: String {
        SHHash.stringDigest(for: number.data(using: .utf8)!)
    }
}
