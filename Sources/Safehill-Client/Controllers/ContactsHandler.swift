import Foundation
import Contacts

public enum SHAddressBookError : Error, LocalizedError {
    case unauthorized
}

public class SHAddressBookContactHandler {
    
    var contactStore: CNContactStore?
    
    public init(contactStore: CNContactStore? = nil) {
        self.contactStore = contactStore
    }
    
    private func fetchOrRequestPermission(completionHandler: @escaping (Result<Bool, Error>) -> Void) {
        self.contactStore = CNContactStore.init()
        self.contactStore!.requestAccess(for: .contacts) { authorized, error in
            guard error == nil else {
                completionHandler(.failure(error!))
                return
            }
            completionHandler(.success(authorized))
        }
    }
    
    public func fetchSystemContacts(
        withThumbnail: Bool = false,
        withFullImage: Bool = false,
        completionHandler: @escaping (Result<[SHAddressBookContact], Error>) -> Void
    ) {
        self.fetchOrRequestPermission() { result in
            switch result {
            case .success(let authorized):
                guard authorized else {
                    completionHandler(.failure(SHAddressBookError.unauthorized))
                    return
                }
                do {
                    var keysToFetch = [
                        CNContactGivenNameKey,
                        CNContactFamilyNameKey,
                        CNContactPhoneNumbersKey
                    ] as [CNKeyDescriptor]
                    
                    if withThumbnail {
                        keysToFetch.append(CNContactThumbnailImageDataKey as CNKeyDescriptor)
                    }
                    if withFullImage {
                        keysToFetch.append(contentsOf: [
                            CNContactImageDataAvailableKey,
                            CNContactImageDataKey
                        ] as [CNKeyDescriptor])
                    }

                    var contacts = [CNContact]()
                    let request = CNContactFetchRequest(keysToFetch: keysToFetch)

                    try self.contactStore!.enumerateContacts(with: request) {
                        (contact, stop) in
                        contacts.append(contact)
                    }

                    let formatted = contacts.compactMap({
                        // filter out all contacts with no phone number or empty names
                        if ($0.phoneNumbers.count > 0 && ($0.givenName.count > 0 || $0.familyName.count > 0)) {
                            return SHAddressBookContact.fromCNContact(contact: $0)
                        }

                        return nil
                    })

                    completionHandler(.success(formatted))
                } catch {
                    log.critical("Failed to fetch contact, error: \(error)")
                    completionHandler(.failure(error))
                }
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    public func fetchSafehillUserMatches(
        requestor: SHLocalUser,
        systemContacts: [SHAddressBookContact],
        completionHandler: @escaping (Result<[SHPhoneNumber: (SHAddressBookContact, SHServerUser)], Error>) -> Void)
    {
        let contactsByPhoneNumber = systemContacts.reduce([SHPhoneNumber: SHAddressBookContact]()) { partialResult, contact in
            var result = partialResult
            for number in contact.numbers {
                result[number] = contact
            }
            return result
        }
        
        let serverProxy = SHServerProxy(user: requestor)
        serverProxy.getUsers(withPhoneNumbers: Array(contactsByPhoneNumber.keys)) { result in
            switch result {
            case .failure(let err):
                completionHandler(.failure(err))
            case .success(let usersByHashedNumber):
                var usersByPhoneNumber = [SHPhoneNumber: (SHAddressBookContact, SHServerUser)]()
                for (phoneNumber, contact) in contactsByPhoneNumber {
                    if let shUserMatch = usersByHashedNumber[phoneNumber.hashedPhoneNumber] {
                        usersByPhoneNumber[phoneNumber] = (contact, shUserMatch)
                    }
                }
                completionHandler(.success(usersByPhoneNumber))
            }
        }
    }
}
