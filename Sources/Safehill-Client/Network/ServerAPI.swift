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
    
    /// Creates a new user on the server given credentials, their public key and public signature
    /// - Parameters:
    ///   - email  the user email
    ///   - name  the user name
    ///   - password  the user password
    ///   - completionHandler: the callback method
    func createUser(email: String,
                    name: String,
                    password: String,
                    completionHandler: @escaping (Swift.Result<SHServerUser, Error>) -> ())
    
    /// Using AppleID credentials either signs in an existing user or creates a new user with such credentials, their public key and public signature
    /// - Parameters:
    ///   - email  the user email
    ///   - name  the user name
    ///   - authorizationCode  the data containing the auth code  to validate
    ///   - identityToken  the data containing the identity token to validate
    ///   - completionHandler: the callback method
    func signInWithApple(email: String,
                         name: String,
                         authorizationCode: Data,
                         identityToken: Data,
                         completionHandler: @escaping (Swift.Result<SHAuthResponse, Error>) -> ())
    
    /// Logs the current user, aka the requestor
    func signIn(password: String, completionHandler: @escaping (Swift.Result<SHAuthResponse, Error>) -> ())
    
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
