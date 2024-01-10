import Foundation
import Contacts

public class SHAddressBookContact: NSObject, NSSecureCoding {
    public static var supportsSecureCoding = true
    
    enum CodingKeys: String, CodingKey {
        case identifier
        case givenName
        case lastName
        case numbers
        case systemContact
    }
    
    public func encode(with coder: NSCoder) {
        coder.encode(self.id, forKey: CodingKeys.identifier.rawValue)
        coder.encode(self.givenName, forKey: CodingKeys.givenName.rawValue)
        coder.encode(self.lastName, forKey: CodingKeys.lastName.rawValue)
        coder.encode(
            self.numbers.map({
                SHPhoneNumberClass(
                    e164FormattedNumber: $0.e164FormattedNumber,
                    stringValue: $0.stringValue,
                    label: $0.label
                )
            }),
            forKey: CodingKeys.numbers.rawValue
        )
        coder.encode(self.systemContact, forKey: CodingKeys.systemContact.rawValue)
    }
    
    public required init?(coder decoder: NSCoder) {
        guard let id = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.identifier.rawValue) as? String,
              let givenName = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.givenName.rawValue) as? String,
              let lastName = decoder.decodeObject(of: NSString.self, forKey: CodingKeys.lastName.rawValue) as? String,
              let phoneNumberClassObjs = decoder.decodeArrayOfObjects(ofClass: SHPhoneNumberClass.self, forKey: CodingKeys.numbers.rawValue),
              let systemContact = decoder.decodeObject(of: CNContact.self, forKey: CodingKeys.systemContact.rawValue)
        else {
            return nil
        }
        
        self.id = id
        self.givenName = givenName
        self.lastName = lastName
        self.numbers = phoneNumberClassObjs.map({
            SHPhoneNumber(
                e164FormattedNumber: $0.e164FormattedNumber,
                label: $0.label
            )
        })
        self.systemContact = systemContact
    }
    
    public var id: String
    private var givenName: String
    private var lastName: String

    public var numbers: [SHPhoneNumber]

    public var systemContact: CNContact? // Keep a reference, although it's not necessary

    internal required init(id: String, 
                           givenName: String,
                           lastName: String,
                           numbers: [SHPhoneNumber],
                           systemContact: CNContact) {
        self.id = id
        self.givenName = givenName
        self.lastName = lastName
        self.numbers = numbers
        self.systemContact = systemContact
    }

    public static func fromCNContact(contact: CNContact) -> SHAddressBookContact {
        let numbers = contact.phoneNumbers.compactMap({
            (value: CNLabeledValue<CNPhoneNumber>) -> SHPhoneNumber? in

            let localizedLabel = CNLabeledValue<NSString>.localizedString(forLabel: value.label ?? "")
            return SHPhoneNumber.init(value.value.stringValue, label: localizedLabel)
        })

        return self.init(id: contact.identifier,
                         givenName: contact.givenName,
                         lastName: contact.familyName,
                         numbers: numbers,
                         systemContact: contact)
    }

    public func fullName() -> String {
        return "\(self.givenName) \(self.lastName)"
    }
}

