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
    
    public func fetchImage(
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
                        CNContactImageDataKey
                    ] as [CNKeyDescriptor]
                    
                    let contactWithThumbnail = try self.contactStore!.unifiedContact(withIdentifier: contact.identifier,
                                                                                     keysToFetch: keysToFetch)
                    
#if os(macOS)
                    if let data = contactWithThumbnail.imageData,
                       let image = NSImage(data: data) {
                        completionHandler(.success(NSUIImage.appKit(image)))
                        return
                    }
#else
                    if let data = contactWithThumbnail.imageData,
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
        withIdentifiers: [String]? = nil,
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
                    if let identifiers = withIdentifiers {
                        request.predicate = CNContact.predicateForContacts(withIdentifiers: identifiers)
                    }
                    
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
        given systemContacts: [SHAddressBookContact],
        completionHandler: @escaping (Result<[SHPhoneNumber: (SHAddressBookContact, SHServerUser)], Error>) -> Void)
    {
        /// The result object
        var usersByPhoneNumber = [SHPhoneNumber: (SHAddressBookContact, SHServerUser)]()
        var error: Error? = nil
        let group = DispatchGroup()
        
        for allSystemContactChunk in systemContacts.chunked(into: 500) {
            
            /// Create a new array where all elements have phone numbers parsed
            /// Phone numbers need to be parsed, then hashed in order to be looked up on the server.
            /// This ensures resiliency to the different phone number formatting
            let allSystemContactChunkWithParsedNumbers = allSystemContactChunk.map { contact in
                return contact.withParsedPhoneNumbers()
            }
            /// Calculate the hashed phone numbers once and key phone numbers by hash
            let allParsedNumbersByHash = allSystemContactChunkWithParsedNumbers
                .flatMap { $0.parsedPhoneNumbers! }
                .reduce([String: SHPhoneNumber]()) {
                    (partialResult: [String: SHPhoneNumber], phoneNumber: SHPhoneNumber) in
                    var result = partialResult
                    result[phoneNumber.hashedPhoneNumber] = phoneNumber
                    return result
                }
            
            group.enter()
            
            let serverProxy = SHServerProxy(user: requestor)
            serverProxy.getUsers(withHashedPhoneNumbers: Array(allParsedNumbersByHash.keys)) { result in
                switch result {
                    
                case .failure(let err):
                    error = err
                    
                case .success(let usersByHashedNumber):
                    let contactsByPhoneNumber = allSystemContactChunkWithParsedNumbers
                        .reduce([SHPhoneNumber: SHAddressBookContact]()) {
                            (partialResult: [SHPhoneNumber: SHAddressBookContact], contact: SHAddressBookContact) in
                            var result = partialResult
                            for parsedPhoneNumber in contact.parsedPhoneNumbers! {
                                result[parsedPhoneNumber] = contact
                            }
                            return result
                        }
                    
                    for (hashedPhoneNumber, safehillUser) in usersByHashedNumber {
                        if let phoneNumber = allParsedNumbersByHash[hashedPhoneNumber],
                           let contact = contactsByPhoneNumber[phoneNumber] {
                            
                            usersByPhoneNumber[phoneNumber] = (contact, safehillUser)
                            
                            DispatchQueue.global(qos: .background).async {
                                ///
                                /// Cache the phone number and the system contact identifier in the user store
                                ///
                                serverProxy.updateLocalUser(
                                    safehillUser as! SHRemoteUser,
                                    phoneNumber: phoneNumber,
                                    linkedSystemContact: contact.systemContact
                                ) {
                                    result in
                                    if case .failure(let failure) = result {
                                        log.error("failed to add link to contact to user cache: \(failure.localizedDescription)")
                                    }
                                }
                            }
                        }
                    }
                }
                
                group.leave()
            }
            
            let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultNetworkTimeoutInMilliseconds))
            guard dispatchResult == .success else {
                completionHandler(.failure(SHBackgroundOperationError.timedOut))
                return
            }
            guard error == nil else {
                completionHandler(.failure(error!))
                return
            }
        }
        
        completionHandler(.success(usersByPhoneNumber))
    }
    
    public func syncContactsAndLocalServerUsers(
        requestor: SHLocalUser,
        given systemContacts: [SHAddressBookContact]
    ) {
        DispatchQueue.global(qos: .userInitiated).async { /// Do it fast so that `systemContacts` (which is big in memory) can be released
            ///
            /// Remove items from the cache for users that are no longer in the Contacts
            /// When the systemContactId is linked to a SHRemoteUser in the cache, and the contact is removed, that link needs to be removed, too
            ///
            let serverProxy = SHServerProxy(user: requestor)
            serverProxy.getAllLocalUsers { result in
                switch result {
                case .success(let serverUsers):
                    var usersWithLinksToRemove = [SHRemoteUserLinkedToContact]()
                    let allAddressBookParsedPhoneNumbers = systemContacts.flatMap { contact in
                        contact.withParsedPhoneNumbers().parsedPhoneNumbers!
                    }
                    for serverUser in serverUsers {
                        if let linkedToSystemContactUser = serverUser as? SHRemoteUserLinkedToContact {
                            let phoneNumber = SHPhoneNumber(
                                e164FormattedNumber: linkedToSystemContactUser.phoneNumber,
                                label: nil
                            )
                            if allAddressBookParsedPhoneNumbers.contains(phoneNumber) {
                                usersWithLinksToRemove.append(linkedToSystemContactUser)
                            }
                        }
                    }
                    
                    if usersWithLinksToRemove.isEmpty == false {
                        DispatchQueue.global(qos: .background).async { /// This can be done in the background
                            serverProxy.removeLinkedSystemContact(from: usersWithLinksToRemove) {
                                result in
                                if case .failure(let failure) = result {
                                    log.error("failed to remove links to contact from user cache: \(failure.localizedDescription)")
                                }
                            }
                        }
                    }
                case .failure(_):
                    break
                }
            }
        }
    }
}