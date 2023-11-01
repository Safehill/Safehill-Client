import Foundation


public struct SendCodeToUserRequestDTO {
    
    public enum Medium: String {
        case phone = "phone", sms = "sms"
    }
    
    let countryCode: Int
    let phoneNumber: Int
    let code: String
    let medium: Medium
}
