import Foundation

public protocol SHRemoteServerAPI : SHServerAPI {
    
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
}
