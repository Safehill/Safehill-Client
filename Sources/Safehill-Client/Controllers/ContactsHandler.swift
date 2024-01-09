import Foundation
import Contacts
import SwiftUI

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
    
    public func fetchThumbnail(
        for contact: CNContact,
        completionHandler: @escaping (Result<NSUIImage?, Error>) -> Void
    ) {
        self.fetchOrRequestPermission() { result in
            switch result {
            case .success(let authorized):
                guard authorized else {
                    completionHandler(.failure(SHAddressBookError.unauthorized))
                    return
                }
                do {
                    let keysToFetch = [
                        CNContactThumbnailImageDataKey
                    ] as [CNKeyDescriptor]
                    
                    let contactWithThumbnail = try self.contactStore!.unifiedContact(withIdentifier: contact.identifier,
                                                                                     keysToFetch: keysToFetch)
                    
#if os(macOS)
                    if let data = contactWithThumbnail.thumbnailImageData,
                       let image = NSImage(data: data) {
                        completionHandler(.success(NSUIImage.appKit(image)))
                        return
                    }
#else
                    if let data = contactWithThumbnail.thumbnailImageData,
                       let image = UIImage(data: data) {
                        completionHandler(.success(NSUIImage.uiKit(image)))
                        return
                    }
#endif
                    
                    return completionHandler(.success(nil))
                } catch {
                    completionHandler(.failure(error))
                }
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    public func fetchSystemContacts(
        completionHandler: @escaping (Result<[CNContact], Error>) -> Void
    ) {
        self.fetchOrRequestPermission() { result in
            switch result {
            case .success(let authorized):
                guard authorized else {
                    completionHandler(.failure(SHAddressBookError.unauthorized))
                    return
                }
                do {
                    let keysToFetch = [
                        CNContactGivenNameKey,
                        CNContactFamilyNameKey,
                        CNContactPhoneNumbersKey
                    ] as [CNKeyDescriptor]

                    var contacts = [CNContact]()
                    let request = CNContactFetchRequest(keysToFetch: keysToFetch)
                    request.sortOrder = .userDefault

                    try self.contactStore!.enumerateContacts(with: request) {
                        (contact, stop) in
                        // filter out all contacts with no phone number or empty names
                        if (contact.phoneNumbers.count > 0 && (contact.givenName.count + contact.familyName.count > 0)) {
                            contacts.append(contact)
                        }
                    }

                    completionHandler(.success(contacts))
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
