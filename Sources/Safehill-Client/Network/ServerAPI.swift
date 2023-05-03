import Foundation

public protocol SHServerAPI {
    
    var requestor: SHLocalUser { get }
    
    // MARK: User Management
    
    /// Creates a new user given their credentials, their public key and public signature (store in the `requestor` object)
    /// - Parameters:
    ///   - name  the user name
    ///   - completionHandler: the callback method
    func createUser(name: String,
                    completionHandler: @escaping (Swift.Result<SHServerUser, Error>) -> ())
    
    /// Updates an existing user details or credentials
    /// - Parameters:
    ///   - email  the new user email
    ///   - name  the new user name
    ///   - password  the new user password
    ///   - completionHandler: the callback method
    func updateUser(email: String?,
                    name: String?,
                    completionHandler: @escaping (Swift.Result<SHServerUser, Error>) -> ())
    
    /// Delete the user making the request and all related assets, metadata and sharing information
    /// - Parameters:
    ///   - name: the user name
    ///   - password: the password for authorization
    ///   - completionHandler: the callback method
    func deleteAccount(name: String, password: String, completionHandler: @escaping (Swift.Result<Void, Error>) -> ())
    
    /// Delete the user making the request and all related assets, metadata and sharing information
    /// - Parameters:
    ///   - completionHandler: the callback method
    func deleteAccount(completionHandler: @escaping (Swift.Result<Void, Error>) -> ())
    
    /// Using AppleID credentials either signs in an existing user or creates a new user with such credentials, their public key and public signature
    /// - Parameters:
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
    func signIn(name: String, completionHandler: @escaping (Swift.Result<SHAuthResponse, Error>) -> ())
    
    /// Get a User's public key and public signature
    /// - Parameters:
    ///   - userIdentifiers: the unique identifiers for the users. If NULL, retrieves all the connected users
    ///   - completionHandler: the callback method
    func getUsers(withIdentifiers: [String]?, completionHandler: @escaping (Swift.Result<[SHServerUser], Error>) -> ())
    
    /// Get a User's public key and public signature
    /// - Parameters:
    ///   - query: the query string
    ///   - completionHandler: the callback method
    func searchUsers(query: String, completionHandler: @escaping (Swift.Result<[SHServerUser], Error>) -> ())
    
    // MARK: Assets Fetch
    
    func getAssetDescriptors(completionHandler: @escaping (Swift.Result<[SHAssetDescriptor], Error>) -> ())
    
    func getAssetDescriptors(forAssetGlobalIdentifiers: [String],
                             completionHandler: @escaping (Swift.Result<[SHAssetDescriptor], Error>) -> ())
    
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
    ///   - groupId: the group identifier used for the first upload
    ///   - completionHandler: the callback method
    func create(assets: [any SHEncryptedAsset],
                groupId: String,
                filterVersions: [SHAssetQuality]?,
                completionHandler: @escaping (Swift.Result<[SHServerAsset], Error>) -> ())
    
    /// Shares one or more assets with a set of users
    /// - Parameters:
    ///   - asset: the asset to share, with references to asset id, version and user id to share with
    ///   - completionHandler: the callback method
    func share(asset: SHShareableEncryptedAsset,
               completionHandler: @escaping (Swift.Result<Void, Error>) -> ())
    
    /// Unshares one asset (all of its versions) with a user. If the asset or the user don't exist, or the asset is not shared with the user, it's a no-op
    /// - Parameters:
    ///   - assetId: the identifier of asset previously shared
    ///   - with: the public identifier of the user it was previously shared with
    ///   - completionHandler: the callback method
    func unshare(assetId: GlobalIdentifier,
                 with userPublicIdentifier: String,
                 completionHandler: @escaping (Swift.Result<Void, Error>) -> ())
    
    /// Upload encrypted asset versions data to the CDN.
    func upload(serverAsset: SHServerAsset,
                asset: any SHEncryptedAsset,
                filterVersions: [SHAssetQuality]?,
                completionHandler: @escaping (Swift.Result<Void, Error>) -> ())
    
    /// Mark encrypted asset versions data as uploaded to the CDN.
    /// - Parameters:
    ///   - assetGlobalIdentifier: the global identifier of the asset
    ///   - quality: the version of the asset
    ///   - as: the new state
    ///   - completionHandler: the callback method
    func markAsset(with assetGlobalIdentifier: String,
                   quality: SHAssetQuality,
                   as: SHAssetDescriptorUploadState,
                   completionHandler: @escaping (Result<Void, Error>) -> ())
    
    /// Removes assets from the CDN
    /// - Parameters:
    ///   - withGlobalIdentifiers: the global identifier
    ///   - completionHandler: the callback method. Returns the list of global identifiers removed
    func deleteAssets(withGlobalIdentifiers: [String], completionHandler: @escaping (Result<[String], Error>) -> ())
    
    /// Validates an AppStore transaction (with receipt)
    /// - Parameters:
    ///   - originalTransactionId: the unique identifier for the transaction
    ///   - receipt: the base64 encoded receipt for the purchases made by this app
    ///   - productId: the identifier of the current subscription known by the client
    ///   - completionHandler: the callback method. Returns a `SHReceiptValidationResponse` object
    func validateTransaction(
        originalTransactionId: String,
        receipt: String,
        productId: String,
        completionHandler: @escaping (Result<SHReceiptValidationResponse, Error>) -> ()
    )
}
