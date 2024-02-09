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
    
    /// Matches the contacts in the address book with safehill users on the server based on their parsed phone numbers (hashed)
    /// - Parameters:
    ///   - requestor: the local user making the HTTP request to fetch safehill users
    ///   - systemContacts: The contacts in the address book to match. If phone numbers are not parsed in this list they are not taken into account, so **ensure that you call `.withParsedPhoneNumbers()` on each member of this list before calling this method**
    ///   - completionHandler: the callback method
    public func fetchSafehillUserMatches(
        requestor: SHAuthenticatedLocalUser,
        given systemContacts: [SHAddressBookContact],
        completionHandler: @escaping (Result<[SHPhoneNumber: (SHAddressBookContact, SHServerUser)], Error>) -> Void)
    {
        /// The result object
        var usersByPhoneNumber = [SHPhoneNumber: (SHAddressBookContact, SHServerUser)]()
        var error: Error? = nil
        let group = DispatchGroup()
        
        for allSystemContactChunk in systemContacts.chunked(into: 500) {
            /// Calculate the hashed phone numbers once and key phone numbers by hash
            let allParsedNumbersByHash = allSystemContactChunk
                .flatMap { $0.formattedPhoneNumbers }
                .reduce([String: SHPhoneNumber]()) {
                    (partialResult: [String: SHPhoneNumber], phoneNumber: SHPhoneNumber) in
                    var result = partialResult
                    result[phoneNumber.hashedPhoneNumber] = phoneNumber
                    return result
                }
            
            group.enter()
            
            requestor.serverProxy.getUsers(withHashedPhoneNumbers: Array(allParsedNumbersByHash.keys)) { result in
                switch result {
                    
                case .failure(let err):
                    error = err
                    
                case .success(let usersByHashedNumber):
                    let contactsByPhoneNumber = allSystemContactChunk
                        .reduce([SHPhoneNumber: SHAddressBookContact]()) {
                            (partialResult: [SHPhoneNumber: SHAddressBookContact], contact: SHAddressBookContact) in
                            var result = partialResult
                            for parsedPhoneNumber in contact.formattedPhoneNumbers {
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
                                requestor.serverProxy.updateLocalUser(
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
        requestor: SHAuthenticatedLocalUser,
        given systemContacts: [SHAddressBookContact]
    ) {
        DispatchQueue.global(qos: .userInitiated).async { /// Do it fast so that `systemContacts` (which is big in memory) can be released
            ///
            /// Remove items from the cache for users that are no longer in the Contacts
            /// When the systemContactId is linked to a SHRemoteUser in the cache, and the contact is removed, that link needs to be removed, too
            ///
            requestor.serverProxy.getAllLocalUsers { result in
                switch result {
                case .success(let serverUsers):
                    var usersWithLinksToRemove = [SHRemoteUserLinkedToContact]()
                    for serverUser in serverUsers {
                        if let linkedToSystemContactUser = serverUser as? SHRemoteUserLinkedToContact {
                            if systemContacts.contains(where: { $0.id == linkedToSystemContactUser.linkedSystemContactId }) == false {
                                usersWithLinksToRemove.append(linkedToSystemContactUser)
                            }
                        }
                    }
                    
                    if usersWithLinksToRemove.isEmpty == false {
                        DispatchQueue.global(qos: .background).async { /// This can be done in the background
                            requestor.serverProxy.removeLinkedSystemContact(from: usersWithLinksToRemove) {
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
