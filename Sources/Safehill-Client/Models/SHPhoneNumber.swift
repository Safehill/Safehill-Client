import Foundation
import Safehill_Crypto

public class SHPhoneNumberClass: NSObject, NSSecureCoding {
    
    public static var supportsSecureCoding: Bool = true
    
    public let e164FormattedNumber: String
    public let stringValue: String
    public let label: String?
    
    enum CodingKeys: String, CodingKey {
        case e164FormattedNumber
        case stringValue
        case label
    }
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.e164FormattedNumber, forKey: CodingKeys.e164FormattedNumber.rawValue)
        coder.encode(self.stringValue, forKey: CodingKeys.stringValue.rawValue)
        coder.encode(self.label, forKey: CodingKeys.label.rawValue)
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        guard let e164FormattedNumber = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.e164FormattedNumber.rawValue) as? String,
              let stringValue = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.stringValue.rawValue) as? String else {
            return nil
        }
        
        let label = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.label.rawValue) as? String
                
        self.init(e164FormattedNumber: e164FormattedNumber,
                  stringValue: stringValue,
                  label: label)
    }
    
    init(e164FormattedNumber: String, stringValue: String, label: String?) {
        self.e164FormattedNumber = e164FormattedNumber
        self.stringValue = stringValue
        self.label = label
    }
}

public struct SHPhoneNumber: Hashable {
    public let e164FormattedNumber: String
    public let stringValue: String
    public let label: String?
    
    public func hash(into hasher: inout Hasher) {
        hasher.combine(e164FormattedNumber)
    }
    
    internal init(e164FormattedNumber: String, stringValue: String, label: String?) {
        self.e164FormattedNumber = e164FormattedNumber
        self.stringValue = stringValue
        self.label = label
    }
    
    public var hashedPhoneNumber: String {
        SHHash.stringDigest(for: e164FormattedNumber.data(using: .utf8)!)
    }
}
