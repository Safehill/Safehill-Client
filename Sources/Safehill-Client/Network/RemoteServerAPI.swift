import Foundation

public protocol SHRemoteServerAPI : SHServerAPI {
    
    func getAssetDescriptors(
        forAssetGlobalIdentifiers: [GlobalIdentifier],
        filteringGroupIds: [String]?,
        after: Date?,
        ignoreCached: Bool,
        completionHandler: @escaping (Result<[any SHAssetDescriptor], Error>) -> ()
    )
    
    // MARK: Asset embeddings
    
    func updateAssetFingerprint(for: GlobalIdentifier, _ fingerprint: AssetFingerprint) async throws
    
    func searchSimilarAssets(to fingerprint: AssetFingerprint) async throws
    
    // MARK: User Management
    
    /// Logs the current user, aka the requestor
    func signIn(clientBuild: String?, completionHandler: @escaping (Result<SHAuthResponse, Error>) -> ())
    
    /// Send a code to a user to verify identity, via either phone or SMS
    /// - Parameters:
    ///   - countryCode: the recipient's phone country code
    ///   - phoneNumber: the recipient's phone number
    ///   - code: the code to send
    ///   - medium: the medium, either SMS or email
    ///   - appName: the name of the app to mention in the SMS ("Snoog", "Safehill", "Nova Stream", …)
    ///   - completionHandler: the callback method
    func sendCodeToUser(countryCode: Int,
                        phoneNumber: Int,
                        code: String,
                        medium: SendCodeToUserRequestDTO.Medium,
                        appName: String,
                        completionHandler: @escaping (Result<Void, Error>) -> ())
    
    /// Get a list of verified users given a list of phone numbers.
    /// Used to determine who - from the user's address book - is a Safehill user
    /// - Parameters:
    ///   - phoneNumbers: the list of phone numbers
    ///   - completionHandler: the callback method
    func getUsers(withHashedPhoneNumbers hashedPhoneNumbers: [String], completionHandler: @escaping (Result<[String: any SHServerUser], Error>) -> ())
    
    
    /// Get a User's public key and public signature
    /// - Parameters:
    ///   - query: the query string
    ///   - completionHandler: the callback method
    func searchUsers(query: String, completionHandler: @escaping (Result<[any SHServerUser], Error>) -> ())
    
    // MARK: User Connection Management
    
    func authorizeUsers(
        with userPublicIdentifiers: [String],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    )
    
    func blockUsers(
        with userPublicIdentifiers: [String],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    )
    
    func pendingOrBlockedUsers(
        completionHandler: @escaping (Result<UserAuthorizationStatusDTO, Error>) -> ()
    )
    
    // MARK: Groups
    
    /// If a thread not be fetched, the user can request the originator of that share for access.
    /// This will trigger a push notification to the originator asking to grant access to this user.
    /// - Parameter groupId: the groupId
    func requestAccess(
        toThreadId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    )
    
    /// If a share group could not be downloaded, the user can request the originator of that share for access.
    /// This will trigger a push notification to the originator asking to grant access to this user.
    /// - Parameter groupId: the groupId
    func requestAccess(
        toGroupId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    )
    
    // MARK: Devices
    
    func registerDevice(
        _ deviceId: String,
        token: String?,
        appBundleId: String?,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    )
    
    // MARK: Web login

    func sendEncryptedKeysToWebClient(
        sessionId: String,
        requestorIp: String,
        encryptedPrivateKeyData: Data,
        encryptedPrivateKeyIvData: Data,
        encryptedPrivateSignatureData: Data,
        encryptedPrivateSignatureIvData: Data
    ) async throws -> Void
    
    // MARK: Collections

    /// Creates a new collection
    /// - Parameters:
    ///   - name: the collection name
    ///   - description: the collection description
    ///   - completionHandler: the callback method
    func createCollection(
        name: String,
        description: String,
        completionHandler: @escaping (Result<CollectionOutputDTO, Error>) -> ()
    )

    /// Retrieve all collections for the user (owned + shared + accessed public)
    /// - Parameter completionHandler: the callback method
    func retrieveCollections(
        completionHandler: @escaping (Result<[CollectionOutputDTO], Error>) -> ()
    )

    /// Retrieve a single collection by ID
    /// - Parameters:
    ///   - id: the collection identifier
    ///   - completionHandler: the callback method
    func retrieveCollection(
        id: String,
        completionHandler: @escaping (Result<CollectionOutputDTO, Error>) -> ()
    )

    /// Update an existing collection (only if owned by user)
    /// - Parameters:
    ///   - id: the collection identifier
    ///   - name: the new name (optional)
    ///   - description: the new description (optional)
    ///   - pricing: the new pricing (optional)
    ///   - completionHandler: the callback method
    func updateCollection(
        id: String,
        name: String?,
        description: String?,
        pricing: Double?,
        completionHandler: @escaping (Result<CollectionOutputDTO, Error>) -> ()
    )

    /// Track collection access (for collections not owned by user)
    /// - Parameters:
    ///   - id: the collection identifier
    ///   - completionHandler: the callback method
    func trackCollectionAccess(
        id: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    )

    /// Search collections with different criteria
    /// - Parameters:
    ///   - query: search query text (optional)
    ///   - searchScope: "owned" for user's owned and accessed, "all" for all discoverable collections
    ///   - visibility: optional filter by visibility
    ///   - priceRange: optional price range filter
    ///   - completionHandler: the callback method
    func searchCollections(
        query: String?,
        searchScope: String,
        visibility: String?,
        priceRange: PriceRangeDTO?,
        completionHandler: @escaping (Result<[CollectionOutputDTO], Error>) -> ()
    )

    /// Get top pick collections
    /// - Parameter completionHandler: the callback method
    /// - Returns: Up to 10 recommended public collections based on popularity, excluding collections already accessed or owned by the user
    func topPickCollections(
        completionHandler: @escaping (Result<[CollectionOutputDTO], Error>) -> ()
    )
    
    /// Removes a tracked collection from the user's collection list (only if not owned by user and not a system collection)
    /// - Parameters:
    ///   - id: the collection identifier
    ///   - completionHandler: the callback method
    func softRemoveCollection(
        id: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    )

    // MARK: Collections - Payments

    /// Create a Stripe Checkout Session for collection payment
    /// - Parameters:
    ///   - collectionId: the collection identifier
    ///   - completionHandler: the callback method
    func createCheckoutSession(
        collectionId: String,
        completionHandler: @escaping (Result<CheckoutSessionDTO, Error>) -> ()
    )

    /// Check if the user has access to a collection
    /// - Parameters:
    ///   - collectionId: the collection identifier
    ///   - completionHandler: the callback method
    func checkCollectionAccess(
        collectionId: String,
        completionHandler: @escaping (Result<AccessCheckResultDTO, Error>) -> ()
    )

    /// Validate Apple IAP transaction (StoreKit 2) for collection payment
    /// - Parameters:
    ///   - collectionId: the collection identifier
    ///   - jwsTransaction: the JWS transaction from StoreKit 2
    ///   - productId: the product identifier
    ///   - transactionId: the transaction identifier
    ///   - completionHandler: the callback method
    func validateIAPReceipt(
        collectionId: String,
        jwsTransaction: String,
        productId: String,
        transactionId: String,
        completionHandler: @escaping (Result<IAPReceiptValidationResponseDTO, Error>) -> ()
    )
    
    // MARK: Credential backup via Passkeys

    func registerPasskeyStart(
        userIdentifier: UserIdentifier,
        completionHandler: @escaping (Result<PasskeyCreationOptions, Error>) -> ()
    )

    func registerPasskeyComplete(
        registrationDetails: PasskeyRegistrationRequest,
        completionHandler: @escaping (Result<PasskeyRegistrationResult, Error>) -> ()
    )

    func listPasskeys(
        completionHandler: @escaping (Result<[PasskeyCredentialInfo], Error>) -> ()
    )

    func revokePasskey(
        credentialId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    )

    func startPasskeyRecovery(
        completionHandler: @escaping (Result<PasskeyAuthenticationOptions, Error>) -> ()
    )

    func completePasskeyRecovery(
        recoveryDetails: PasskeyRecoveryRequest,
        completionHandler: @escaping (Result<PasskeyRecoveryResult, Error>) -> ()
    )
}
