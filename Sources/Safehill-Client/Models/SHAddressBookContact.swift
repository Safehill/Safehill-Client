import Foundation
import Contacts
import PhoneNumberKit

public final class SHAddressBookContact: NSObject, NSSecureCoding {
    
    public static let supportsSecureCoding = true
    
    enum CodingKeys: String, CodingKey {
        case identifier
        case givenName
        case lastName
        case systemContact
    }
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.id, forKey: CodingKeys.identifier.rawValue)
        coder.encode(self.givenName, forKey: CodingKeys.givenName.rawValue)
        coder.encode(self.lastName, forKey: CodingKeys.lastName.rawValue)
        coder.encode(self.systemContact, forKey: CodingKeys.systemContact.rawValue)
    }
    
    public required convenience init?(coder decoder: NSCoder) {
        guard let id = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.identifier.rawValue) as? String,
              let systemContact = decoder.decodeObject(of: CNContact.self, forKey: CodingKeys.systemContact.rawValue)
        else {
            return nil
        }
        
        let givenName = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.givenName.rawValue) as? String
        let lastName = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.lastName.rawValue) as? String
        
        self.init(
            id: id,
            givenName: givenName ?? "",
            lastName: lastName ?? "",
            systemContact: systemContact
        )
    }
    
    public let id: String
    private let givenName: String
    private let lastName: String

    public let systemContact: CNContact
    
    public var verifiedPhoneNumbers: [SHPhoneNumber] {
        return SHPhoneNumberParser.sharedInstance.parse(systemContact.phoneNumbers).compactMap({ $0 })
    }

    internal required init(id: String, 
                           givenName: String,
                           lastName: String,
                           systemContact: CNContact) {
        self.id = id
        self.givenName = givenName
        self.lastName = lastName
        self.systemContact = systemContact
    }

    public static func fromCNContact(contact: CNContact) -> SHAddressBookContact {
        return self.init(id: contact.identifier,
                         givenName: contact.givenName,
                         lastName: contact.familyName,
                         systemContact: contact)
    }

    public func fullName() -> String {
        return "\(self.givenName) \(self.lastName)"
    }
    
    public func labeledPhoneNumbers() -> [(label: String, stringValue: String)] {
        return systemContact.phoneNumbers.compactMap({
            (value: CNLabeledValue<CNPhoneNumber>) in

            let localizedLabel = CNLabeledValue<NSString>.localizedString(forLabel: value.label ?? "")
            return (label: localizedLabel, stringValue: value.value.stringValue)
        })
    }
}

