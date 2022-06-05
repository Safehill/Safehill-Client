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
    
    /// Creates a new user given their credentials, their public key and public signature (store in the `requestor` object)
    /// - Parameters:
    ///   - email  the user email
    ///   - name  the user name
    ///   - password  the user password
    ///   - completionHandler: the callback method
    func createUser(email: String,
                    name: String,
                    password: String,
                    completionHandler: @escaping (Swift.Result<SHServerUser, Error>) -> ())
    
    /// Updates an existing user details or credentials
    /// - Parameters:
    ///   - email  the user email
    ///   - name  the user name
    ///   - password  the user password
    ///   - completionHandler: the callback method
    func updateUser(email: String?,
                    name: String?,
                    password: String?,
                    completionHandler: @escaping (Swift.Result<SHServerUser, Error>) -> ())
    
    /// Delete the user making the request and all related assets, metadata and sharing information
    /// - Parameters:
    ///   - email: the user email
    ///   - password: the password for authorization
    ///   - completionHandler: the callback method
    func deleteAccount(email: String, password: String, completionHandler: @escaping (Swift.Result<Void, Error>) -> ())
    
    /// Delete the user making the request and all related assets, metadata and sharing information
    /// - Parameters:
    ///   - completionHandler: the callback method
    func deleteAccount(completionHandler: @escaping (Swift.Result<Void, Error>) -> ())
    
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
    func signIn(email: String?, password: String, completionHandler: @escaping (Swift.Result<SHAuthResponse, Error>) -> ())
    
    /// Get a User's public key and public signature
    /// - Parameters:
    ///   - userIdentifier: the unique identifier for the user
    ///   - completionHandler: the callback method
    func getUsers(withIdentifiers: [String], completionHandler: @escaping (Swift.Result<[SHServerUser], Error>) -> ())
    
    /// Get a User's public key and public signature
    /// - Parameters:
    ///   - query: the query string
    ///   - completionHandler: the callback method
    func searchUsers(query: String, completionHandler: @escaping (Swift.Result<[SHServerUser], Error>) -> ())
    
    // MARK: Assets Fetch
    
    func getAssetDescriptors(completionHandler: @escaping (Swift.Result<[SHAssetDescriptor], Error>) -> ())
    
    /// Retrieve assets data and metadata
    /// - Parameters:
    ///   - withGlobalIdentifiers: filtering by global identifier
    ///   - versions: filtering by version
    ///   - completionHandler: the callback method
    func getAssets(withGlobalIdentifiers: [String],
                   versions: [SHAssetQuality]?,
                   completionHandler: @escaping (Swift.Result<[String: SHEncryptedAsset], Error>) -> ())
    
    // MARK: Assets Write
    
    /// Create encrypted asset and versions (low res and hi res)
    /// - Parameters:
    ///   - assets: the encrypted data for each asset
    ///   - completionHandler: the callback method
    func create(assets: [SHEncryptedAsset],
                completionHandler: @escaping (Swift.Result<[SHServerAsset], Error>) -> ())
    
    /// Shares one or more assets with a set of users
    /// - Parameters:
    ///   - asset: the asset to share, with references to asset id, version and user id to share with
    ///   - completionHandler: the callback method
    func share(asset: SHShareableEncryptedAsset,
               completionHandler: @escaping (Swift.Result<Void, Error>) -> ())
    
    /// Upload encrypted asset versions data to the CDN.
    func upload(serverAsset: SHServerAsset,
                asset: SHEncryptedAsset,
                completionHandler: @escaping (Swift.Result<Void, Error>) -> ())
    
    
    /// Removes assets from the CDN
    /// - Parameters:
    ///   - withGlobalIdentifiers: the global identifier
    ///   - completionHandler: the callback method. Returns the list of global identifiers removed
    func deleteAssets(withGlobalIdentifiers: [String], completionHandler: @escaping (Result<[String], Error>) -> ())
}
