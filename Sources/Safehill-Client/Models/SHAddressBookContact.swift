import Foundation
import Contacts

public struct SHAddressBookContact {
    
    public var id = UUID()
    private var givenName: String
    private var lastName: String

    public var numbers: [SHPhoneNumber]

    public var systemContact: CNContact? // Keep a reference, although it's not necessary

    init(givenName: String, lastName: String, numbers: [SHPhoneNumber], systemContact: CNContact) {
        self.givenName = givenName
        self.lastName = lastName
        self.numbers = numbers
        self.systemContact = systemContact
    }

    init(givenName: String, lastName: String, numbers: [SHPhoneNumber]) {
        self.givenName = givenName
        self.lastName = lastName
        self.numbers = numbers
    }

    static func fromCNContact(contact: CNContact) -> SHAddressBookContact {
        let numbers = contact.phoneNumbers.compactMap({
            (value: CNLabeledValue<CNPhoneNumber>) -> SHPhoneNumber? in

            let localizedLabel = CNLabeledValue<NSString>.localizedString(forLabel: value.label ?? "")
            return SHPhoneNumber.init(value.value.stringValue, label: localizedLabel)
        })

        return self.init(givenName: contact.givenName, lastName: contact.familyName, numbers: numbers, systemContact: contact)
    }

    public func fullName() -> String {
        return "\(self.givenName) \(self.lastName)"
    }
}


extension SHAddressBookContact : Equatable {
    public static func == (lhs: SHAddressBookContact, rhs: SHAddressBookContact) -> Bool {
        lhs.id == rhs.id
    }
}

