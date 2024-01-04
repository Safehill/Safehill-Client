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
    
    public func fetchSystemContacts(completionHandler: @escaping (Result<[SHAddressBookContact], Error>) -> Void) {
        self.fetchOrRequestPermission() { result in
            switch result {
            case .success(let authorized):
                guard authorized else {
                    completionHandler(.failure(SHAddressBookError.unauthorized))
                    return
                }
                do {
                    let keysToFetch = [CNContactGivenNameKey, CNContactFamilyNameKey, CNContactPhoneNumbersKey] as [CNKeyDescriptor]

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
    
    func fetchSafehillUserMatches(requestor: SHLocalUser) throws -> [SHPhoneNumber: (SHAddressBookContact, SHServerUser)] {
        var usersByPhoneNumber = [SHPhoneNumber: (SHAddressBookContact, SHServerUser)]()
        
        let group = DispatchGroup()
        var contacts = [SHAddressBookContact]()
        var error: Error? = nil
        
        group.enter()
        self.fetchSystemContacts { result in
            switch result {
            case .success(let cs):
                contacts = cs
            case .failure(let err):
                error = err
            }
            group.leave()
        }
        
        var dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds))
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
        guard error != nil else {
            throw error!
        }
        
        let contactsByPhoneNumber = contacts.reduce([SHPhoneNumber: SHAddressBookContact]()) { partialResult, contact in
            var result = partialResult
            for number in contact.numbers {
                result[number] = contact
            }
            return result
        }
        
        group.enter()
        let serverProxy = SHServerProxy(user: requestor)
        serverProxy.getUsers(withPhoneNumbers: Array(contactsByPhoneNumber.keys)) { result in
            switch result {
            case .failure(let err):
                error = err
            case .success(let usersByHashedNumber):
                for (phoneNumber, contact) in contactsByPhoneNumber {
                    if let shUserMatch = usersByHashedNumber[phoneNumber.hashedPhoneNumber] {
                        usersByPhoneNumber[phoneNumber] = (contact, shUserMatch)
                    }
                }
            }
            group.leave()
        }
        
        dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultDBTimeoutInMilliseconds))
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
        guard error != nil else {
            throw error!
        }
        
        return usersByPhoneNumber
    }
}
