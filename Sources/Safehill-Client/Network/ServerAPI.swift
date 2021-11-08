//
//  ServerAPI.swift
//
//
//  Created by Gennaro Frazzingaro on 9/22/21.
//

import Foundation

public protocol SHServerAPI {
    
    var requestor: SHLocalUser { get }
    
    // MARK: User Management
    
    /// Creates a User entry on the server given name, phone number, public key and public signature
    /// - Parameters:
    ///   - user: the User object
    ///   - completionHandler: the callback method
    func createUser(completionHandler: @escaping (Swift.Result<SHServerUser?, Error>) -> ())
    
    /// Sends a validation code to the user's phone number
    /// - Parameters:
    ///   - user: the User object
    ///   - completionHandler: the callback method
    func sendAuthenticationCode(completionHandler: @escaping (Swift.Result<Void, Error>) -> ())
    
    /// Validates the user's phone number via code
    /// - Parameters:
    ///   - user: the User object
    ///   - completionHandler: the callback method
    func validateAuthenticationCode(completionHandler: @escaping (Swift.Result<Void, Error>) -> ())
    
    /// Get a User's public key and public signature
    /// - Parameters:
    ///   - userIdentifier: the unique identifier for the user
    ///   - completionHandler: the callback method
    func getUsers(withIdentifiers: [String], completionHandler: @escaping (Swift.Result<[SHServerUser], Error>) -> ())
    
    // MARK: Assets Read
    
    func getAssetDescriptors(completionHandler: @escaping (Swift.Result<[SHAssetDescriptor], Error>) -> ())
    
    func getLowResAssets(withGlobalIdentifiers: [String], completionHandler: @escaping (Swift.Result<[String: SHEncryptedAsset], Error>) -> ())
    
    func getHiResAssets(withGlobalIdentifiers: [String], completionHandler: @escaping (Swift.Result<[String: SHEncryptedAsset], Error>) -> ())
    
    // MARK: Assets Write
    
    /// Store encrypted assets as data to the CDN.
    /// - Parameters:
    ///   - lowResAsset: the low resolution version of the encrypted data
    ///   - hiResAsset: the high resolution version of the encrypted data
    ///   - completionHandler: the callback method
    func storeAsset(lowResAsset: SHEncryptedAsset,
                    hiResAsset: SHEncryptedAsset,
                    completionHandler: @escaping (Swift.Result<Void, Error>) -> ())
    
    
    /// Removes assets from the CDN
    /// - Parameters:
    ///   - withGlobalIdentifiers: the global identifier
    ///   - completionHandler: the callback method. Returns the list of global identifiers removed
    func deleteAssets(withGlobalIdentifiers: [String], completionHandler: @escaping (Result<[String], Error>) -> ())
}
