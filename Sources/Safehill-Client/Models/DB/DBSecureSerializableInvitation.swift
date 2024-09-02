import Foundation

class DBSecureSerializableInvitation: NSObject, NSSecureCoding {
    
    public static var supportsSecureCoding = true
    
    enum CodingKeys: String, CodingKey {
        case phoneNumber
        case invitedAt
    }
    
    let phoneNumber: String
    let invitedAt: String // ISO8601 formatted datetime
    
    init(phoneNumber: String,
         invitedAt: String) {
        self.phoneNumber = phoneNumber
        self.invitedAt = invitedAt
    }
    
    required convenience init?(coder decoder: NSCoder) {
        let phoneNumber = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.phoneNumber.rawValue)
        let invitedAt = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.invitedAt.rawValue)
        
        guard let phoneNumber = phoneNumber as? String else {
            log.error("unexpected value for phoneNumber when decoding DBSecureSerializableInvitation object")
            return nil
        }
        guard let invitedAt = invitedAt as? String else {
            log.error("unexpected value for invitedAt when decoding DBSecureSerializableInvitation object")
            return nil
        }
        
        self.init(
            phoneNumber: phoneNumber,
            invitedAt: invitedAt
        )
    }
    
    func encode(with coder: NSCoder) {
        coder.encode(phoneNumber, forKey: CodingKeys.phoneNumber.rawValue)
        coder.encode(invitedAt, forKey: CodingKeys.invitedAt.rawValue)
    }
}
