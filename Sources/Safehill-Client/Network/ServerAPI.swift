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
    
    /// Send a code to a user to verify identity, via either phone or SMS
    /// - Parameters:
    ///   - countryCode: the recipient's phone country code
    ///   - phoneNumber: the recipient's phone number
    ///   - code: the code to send
    ///   - medium: the medium, either SMS or email
    ///   - completionHandler: the callback method
    func sendCodeToUser(countryCode: Int, 
                        phoneNumber: Int,
                        code: String,
                        medium: SendCodeToUserRequestDTO.Medium,
                        completionHandler: @escaping (Swift.Result<Void, Error>) -> ())
    
    /// Updates an existing user details or credentials. If the value is nil, it's not updated
    /// - Parameters:
    ///   - name  the new user name
    ///   - phoneNumber  the new user phone number
    ///   - email  the new user email
    ///   - completionHandler: the callback method
    func updateUser(name: String?,
                    phoneNumber: String?,
                    email: String?,
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
    
    /// Logs the current user, aka the requestor
    func signIn(name: String, clientBuild: Int?, completionHandler: @escaping (Swift.Result<SHAuthResponse, Error>) -> ())
    
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
    
    func getAssetDescriptors(forAssetGlobalIdentifiers: [GlobalIdentifier]?,
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
    
    /// Create encrypted assets and their versions on the server, and retrieves the presigned URL for the client to upload.
    /// - Parameters:
    ///   - assets: the encrypted data for each asset
    ///   - groupId: the group identifier used for the first upload
    ///   - filterVersions: because the input `SHEncryptedAsset`, optionally specify which versions to pick up from the `assets` object
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
    
    /// Creates a group and provides the encryption details for users in the group for E2EE.
    /// This method needs to be called every time a share (group) is created so that reactions and comments can be added to it.
    /// - Parameters:
    ///   - groupId: the group identifier
    ///   - recipientsEncryptionDetails: the encryption details for each reciepient
    ///   - completionHandler: the callback method
    func setGroupEncryptionDetails(
        groupId: String,
        recipientsEncryptionDetails: [RecipientEncryptionDetailsDTO],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    )
    
    /// Retrieved the E2EE details for a group, if one exists
    /// - Parameters:
    ///   - groupId: the group identifier
    ///   - completionHandler: the callback method
    func retrieveGroupUserEncryptionDetails(
        forGroup groupId: String,
        completionHandler: @escaping (Result<[RecipientEncryptionDetailsDTO], Error>) -> ()
    )
    
    /// Adds reactions to a share (group)
    /// - Parameters:
    ///   - reactions: the reactions details
    ///   - groupId: the group identifier
    ///   - completionHandler: the callback method
    func addReactions(
        _: [ReactionInput],
        toGroupId: String,
        completionHandler: @escaping (Result<[ReactionOutputDTO], Error>) -> ()
    )
    
    /// Removes a reaction to an asset or a message
    /// - Parameters:
    ///   - reaction: the reaction type and references to remove
    ///   - fromGroupId: the group the reaction belongs to
    ///   - completionHandler: the callback method
    func removeReaction(
        _ reaction: ReactionInput,
        fromGroupId groupId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    )
    
    /// Retrieves all the messages and reactions for a group id. Results are paginated and returned in reverse cronological order.
    /// - Parameters:
    ///   - groupId: the group identifier
    ///   - per: the number of items to retrieve
    ///   - page: the page number, because results are paginated
    ///   - completionHandler: the callback method
    func retrieveInteractions(
        inGroup groupId: String,
        per: Int,
        page: Int,
        completionHandler: @escaping (Result<InteractionsGroupDTO, Error>) -> ()
    )
    
    /// Adds a messages to a share (group)
    /// - Parameters:
    ///   - messages: the message details
    ///   - groupId: the group identifier
    ///   - completionHandler: the callback method
    func addMessages(
        _ messages: [MessageInput],
        toGroupId groupId: String,
        completionHandler: @escaping (Result<[MessageOutputDTO], Error>) -> ()
    )
}
