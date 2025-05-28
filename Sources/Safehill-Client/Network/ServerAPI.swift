import Foundation

public protocol SHServerAPI {
    
    var requestor: SHLocalUserProtocol { get }
    
    // MARK: User Management
    
    /// Creates a new user given their credentials, their public key and public signature (store in the `requestor` object)
    /// - Parameters:
    ///   - name  the user name
    ///   - completionHandler: the callback method
    func createOrUpdateUser(name: String,
                            completionHandler: @escaping (Result<any SHServerUser, Error>) -> ())
    
    /// Updates an existing user details or credentials. If the value is nil, it's not updated
    /// - Parameters:
    ///   - name  the new name
    ///   - phoneNumber  the new phone number
    ///   - completionHandler: the callback method
    func updateUser(name: String?,
                    phoneNumber: SHPhoneNumber?,
                    forcePhoneNumberLinking: Bool,
                    completionHandler: @escaping (Result<any SHServerUser, Error>) -> ())
    
    /// Delete the user making the request and all related assets, metadata and sharing information
    /// - Parameters:
    ///   - name: the user name
    ///   - password: the password for authorization
    ///   - completionHandler: the callback method
    func deleteAccount(name: String, password: String, completionHandler: @escaping (Result<Void, Error>) -> ())
    
    /// Delete the user making the request and all related assets, metadata and sharing information
    /// - Parameters:
    ///   - completionHandler: the callback method
    func deleteAccount(completionHandler: @escaping (Result<Void, Error>) -> ())
    
    /// Get a User's public key and public signature
    /// - Parameters:
    ///   - userIdentifiers: the unique identifiers for the users. If NULL, retrieves all the connected users
    ///   - completionHandler: the callback method
    func getUsers(withIdentifiers: [String]?, completionHandler: @escaping (Result<[any SHServerUser], Error>) -> ())
    
    // MARK: Assets Management
    
    /// Count how many assets were created by this user
    /// - Parameters:
    ///   - completionHandler: the callback method
    func countUploaded(
        completionHandler: @escaping (Swift.Result<Int, Error>) -> ()
    )
    
    /// Get descriptors for specific asset global identifiers
    /// - Parameters:
    ///   - forAssetGlobalIdentifiers: if not empty, retrieve only the provided asset gids
    ///   - after: retrieve only the ones uploaded or shared after this date
    ///   - filteringGroupIds: only returns descriptors for assets that are shared via the group ids, and return the group information only for the provided these group ids
    ///   - completionHandler: the callback method
    func getAssetDescriptors(
        forAssetGlobalIdentifiers: [GlobalIdentifier],
        filteringGroupIds: [String]?,
        after: Date?,
        completionHandler: @escaping (Result<[any SHAssetDescriptor], Error>) -> ()
    )
    
    /// Retrieve asset descriptor created or updated since the reference date
    /// - Parameters:
    ///   - after: retrieve only the ones uploaded or shared after this date
    ///   - completionHandler: the callback method
    func getAssetDescriptors(
        after: Date?,
        completionHandler: @escaping (Swift.Result<[any SHAssetDescriptor], Error>) -> ()
    )
    
    /// Retrieve assets data and metadata
    /// - Parameters:
    ///   - withGlobalIdentifiers: filtering by global identifier
    ///   - versions: filtering by version
    ///   - completionHandler: the callback method
    func getAssets(withGlobalIdentifiers: [String],
                   versions: [SHAssetQuality]?,
                   completionHandler: @escaping (Result<[GlobalIdentifier: any SHEncryptedAsset], Error>) -> ())
    
    /// Create encrypted assets and their versions on the server, and retrieves the presigned URL for the client to upload.
    /// - Parameters:
    ///   - assets: the encrypted data for each asset
    ///   - groupId: the group identifier used for the first upload
    ///   - filterVersions: because the input `SHEncryptedAsset`, optionally specify which versions to pick up from the `assets` object
    ///   - force: if set to true overrides all sharing information for the existing asset version for the requesting user, if one with the same name exists
    ///   - completionHandler: the callback method
    func create(assets: [any SHEncryptedAsset],
                groupId: String,
                filterVersions: [SHAssetQuality]?,
                force: Bool,
                completionHandler: @escaping (Result<[SHServerAsset], Error>) -> ())
    
    // MARK: Assets Sharing
    
    /// Shares one or more assets with a set of users
    /// - Parameters:
    ///   - asset: the asset to share, with references to asset id, version and user id to share with
    ///   - asPhotoMessageInThreadId: whether or not the asset is being shared in the context of a thread and if so which thread
    ///   - permissions: whether or not the asset should stay confidential, can be downloaded or reshared, or it's public
    ///   - suppressNotification: do not send a notification to the user. For instance, when the high resolution is shared in the background
    ///   - completionHandler: the callback method
    func share(asset: SHShareableEncryptedAsset,
               asPhotoMessageInThreadId: String?,
               permissions: Int?,
               suppressNotification: Bool,
               completionHandler: @escaping (Result<Void, Error>) -> ())
    
    /// Unshares one asset (all of its versions) with a user. If the asset or the user don't exist, or the asset is not shared with the user, it's a no-op
    /// - Parameters:
    ///   - assetId: the identifier of asset previously shared
    ///   - with: the public identifier of the user it was previously shared with
    ///   - completionHandler: the callback method
    func unshare(
        assetIdsWithUsers: [GlobalIdentifier: [UserIdentifier]],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    )
    
    /// Changes the sharing permissions for an existing group (from regular to confidential, for instance)
    /// - Parameters:
    ///   - groupId: the group identifier
    ///   - permission: the permission code
    ///   - completionHandler: the callback method
    func changeGroupPermission(
        groupId: String,
        permission: Int,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    )
    
    // MARK: Assets Uploading
    
    /// Upload encrypted asset versions data to the CDN.
    func uploadAsset(
        with globalIdentifier: GlobalIdentifier,
        versionsDataManifest: [SHAssetQuality: (URL, Data)],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    )
    
    /// Mark encrypted asset versions data as uploaded to the CDN.
    /// - Parameters:
    ///   - assetGlobalIdentifier: the global identifier of the asset
    ///   - quality: the version of the asset
    ///   - as: the new state
    ///   - completionHandler: the callback method
    func markAsset(with assetGlobalIdentifier: GlobalIdentifier,
                   quality: SHAssetQuality,
                   as: SHAssetDescriptorUploadState,
                   completionHandler: @escaping (Result<Void, Error>) -> ())
    
    // MARK: Assets Removal
    
    /// Removes assets from the CDN
    /// - Parameters:
    ///   - withGlobalIdentifiers: the global identifier
    ///   - completionHandler: the callback method. Returns the list of global identifiers removed
    func deleteAssets(withGlobalIdentifiers: [String], completionHandler: @escaping (Result<[String], Error>) -> ())
    
    // MARK: Subscriptions
    
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
    
    // MARK: Threads
    
    /// Creates a thread and provides the encryption details for the users in it for E2EE.
    /// This method needs to be called every time both a thread is created or a  so that reactions and comments can be added to it.
    /// - Parameters:
    ///   - name: the optional name of the thread
    ///   - recipientsEncryptionDetails: the encryption details for each reciepient. `nil` to update other properties of the thread
    ///   - invitedPhoneNumbers: the phone numbers invited to the thread. `nil` for updates to other properties of the thread
    ///   - completionHandler: the callback method, with the threadId
    func createOrUpdateThread(
        name: String?,
        recipientsEncryptionDetails: [RecipientEncryptionDetailsDTO]?,
        invitedPhoneNumbers: [String]?,
        completionHandler: @escaping (Result<ConversationThreadOutputDTO, Error>) -> ()
    )
    
    /// Updates the thread name
    /// - Parameters:
    ///   - threadId: the thread identifier
    ///   - newName: the new name. nil for clearing an existing name
    ///   - completionHandler: the callback
    func updateThread(
       _ threadId: String,
       newName: String?,
       completionHandler: @escaping (Result<Void, Error>) -> ()
   )
    
    /// Updates the thread name
    /// - Parameters:
    ///   - threadId: the thread identifier
    ///   - update: the updates to the members
    ///   - completionHandler: the callback
    func updateThreadMembers(
       for threadId: String,
       _ update: ConversationThreadMembersUpdateDTO,
       completionHandler: @escaping (Result<Void, Error>) -> ()
   )
    
    /// List all the threads visibile to the requestor
    /// - Parameter completionHandler: the callback method
    func listThreads(
        completionHandler: @escaping (Result<[ConversationThreadOutputDTO], Error>) -> ()
    )
    
    /// Retrieved the thread details, including the E2EE details, if one exists
    /// - Parameters:
    ///   - threadId: the thread identifier
    ///   - completionHandler: the callback method
    func getThread(
        withId threadId: String,
        completionHandler: @escaping (Result<ConversationThreadOutputDTO?, Error>) -> ()
    )
    
    /// Deletes a thread given its identifier
    /// - Parameters:
    ///   - threadId: the thread identifier
    ///   - completionHandler: the callback method
    func deleteThread(
        withId threadId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    )
    
    /// Retrieve the thread with the specified users, if one exists
    /// - Parameters:
    ///   - userIds: the users to match
    ///   - phoneNumbers: the phone numbers invited to the thread to match
    ///   - completionHandler: the callback method
    func getThread(
        withUserIds userIds: [UserIdentifier],
        and phoneNumbers: [String],
        completionHandler: @escaping (Result<ConversationThreadOutputDTO?, Error>) -> ()
    )
    
    /// Retrieve the assets in a thread
    /// - Parameters:
    ///   - threadId: the thread identifier
    ///   - completionHandler: the callback method
    func getAssets(
        inThread threadId: String,
        completionHandler: @escaping (Result<ConversationThreadAssetsDTO, Error>) -> ()
    )
    
    // MARK: Groups
    
    /// Creates a group and provides the encryption details for users in the group for E2EE.
    /// This method needs to be called every time a share (group) is created so that reactions and comments can be added to it.
    /// - Parameters:
    ///   - groupId: the group identifier
    ///   - encryptedTitle: the message accompanying the group, if any, encrypted with the group E2EE details
    ///   - recipientsEncryptionDetails: the encryption details for each reciepient
    ///   - completionHandler: the callback method
    func setupGroup(
        groupId: String,
        encryptedTitle: String?,
        recipientsEncryptionDetails: [RecipientEncryptionDetailsDTO],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    )
    
    /// Delete a group, related messages and reactions, given its id
    /// - Parameters:
    ///   - groupId: the group identifier
    ///   - completionHandler: the callback method
    func deleteGroup(
        groupId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    )
    
    /// Retrieves the title and the encryption details for the group
    /// - Parameters:
    ///   - groupId: the group identifier
    ///   - completionHandler: the callback method
    func retrieveGroupDetails(
        forGroup groupId: String,
        completionHandler: @escaping (Result<InteractionsGroupDetailsResponseDTO?, Error>) -> Void
    )
    
    /// Retrieved the E2EE details for a group, if one exists
    /// - Parameters:
    ///   - groupId: the group identifier
    ///   - completionHandler: the callback method
    func retrieveUserEncryptionDetails(
        forGroup groupId: String,
        completionHandler: @escaping (Result<RecipientEncryptionDetailsDTO?, Error>) -> ()
    )
    
    // MARK: Interactions
    
    /// Retrieve an overall summary of all interactions in threads and groups
    /// - Parameter completionHandler: the callback method
    func topLevelInteractionsSummary(
        completionHandler: @escaping (Result<InteractionsSummaryDTO, Error>) -> ()
    )
    
    /// Retrieve an overall summary of all interactions in all threads
    /// - Parameter completionHandler: the callback method
    func topLevelThreadsInteractionsSummary(
        completionHandler: @escaping (Result<[String: InteractionsThreadSummaryDTO], Error>) -> ()
    )
    
    /// Retrieve an overall summary of all interactions in all groups
    /// - Parameter completionHandler: the callback method
    func topLevelGroupsInteractionsSummary(
        completionHandler: @escaping (Result<[String: InteractionsGroupSummaryDTO], Error>) -> ()
    )
    
    /// Retrieve an overall summary of all interactions in a specific group
    /// - Parameters:
    ///   - groupId:  the group identifier
    ///   - completionHandler: the callback method
    func topLevelInteractionsSummary(
        inGroup groupId: String,
        completionHandler: @escaping (Result<InteractionsGroupSummaryDTO, Error>) -> ()
    )
    
    /// Adds reactions to a share (group)
    /// - Parameters:
    ///   - reactions: the reactions details
    ///   - groupId: the group identifier
    ///   - completionHandler: the callback method
    func addReactions(
        _: [ReactionInput],
        toGroup groupId: String,
        completionHandler: @escaping (Result<[ReactionOutputDTO], Error>) -> ()
    )
    
    /// Adds reactions to a message in a thread
    /// - Parameters:
    ///   - reactions: the reactions details
    ///   - messageId: the message the reaction refers to
    ///   - threadId: the thread identifier
    ///   - completionHandler: the callback method
    func addReactions(
        _: [ReactionInput],
        toThread threadId: String,
        completionHandler: @escaping (Result<[ReactionOutputDTO], Error>) -> ()
    )
    
    /// Removes reactions to an asset or a message in a share (group)
    /// - Parameters:
    ///   - reactionType: the reaction type and references to remove
    ///   - inReplyToAssetGlobalIdentifier: the referenced asset
    ///   - inReplyToInteractionId: the referenced interaction
    ///   - groupId: the container group
    ///   - completionHandler: the callback method
    func removeReaction(
        _ reactionType: ReactionType,
        senderPublicIdentifier: UserIdentifier,
        inReplyToAssetGlobalIdentifier: GlobalIdentifier?,
        inReplyToInteractionId: String?,
        fromGroup groupId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    )
    
    /// Removes a set of reactions to a message
    /// - Parameters:
    ///   - reactionType: the reaction type and references to remove
    ///   - inReplyToAssetGlobalIdentifier: the referenced asset
    ///   - inReplyToInteractionId: the referenced interaction
    ///   - threadId: the container thread
    ///   - completionHandler: the callback method
    func removeReaction(
        _ reactionType: ReactionType,
        senderPublicIdentifier: UserIdentifier,
        inReplyToAssetGlobalIdentifier: GlobalIdentifier?,
        inReplyToInteractionId: String?,
        fromThread threadId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    )
    
    /// Retrieves all the messages and reactions for a group id. Results are paginated and returned in reverse cronological order.
    /// - Parameters:
    ///   - groupId: the group identifier
    ///   - filtering: messages-only, or reactions-only, or both if nil
    ///   - refMessageId: if a nested thread, the message it's nested under
    ///   - before: (optional query modifier) retrieve only messages with creation date lower than this date. Defaults to NOW
    ///   - limit: limits the number of results returned
    ///   - completionHandler: the callback method
    func retrieveInteractions(
        inGroup groupId: String,
        ofType type: InteractionType?,
        underMessage refMessageId: String?,
        before: Date?,
        limit: Int,
        completionHandler: @escaping (Result<InteractionsGroupDTO, Error>) -> ()
    )
    
    /// Retrieves all the messages and reactions in a thread. Results are paginated and returned in reverse cronological order.
    /// Optionally specify the anchor message, if this is a reply to another message (sub-thread)
    /// - Parameters:
    ///   - groupId: the group identifier
    ///   - before: (optional query modifier) retrieve only messages with creation date lower than this date. Defaults to NOW
    ///   - limit: limits the number of results returned
    ///   - completionHandler: the callback method
    func retrieveInteractions(
        inThread threadId: String,
        ofType type: InteractionType?,
        underMessage refMessageId: String?,
        before: Date?,
        limit: Int,
        completionHandler: @escaping (Result<InteractionsGroupDTO, Error>) -> ()
    )
    
    /// Adds messages to a share (group)
    /// - Parameters:
    ///   - messages: the message details
    ///   - groupId: the group identifier
    ///   - completionHandler: the callback method
    func addMessages(
        _ messages: [MessageInput],
        toGroup groupId: String,
        completionHandler: @escaping (Result<[MessageOutputDTO], Error>) -> ()
    )
    
    
    /// Adds messages to a thread.
    /// Optionally specify the anchor message, if this is a reply to another message (sub-thread)
    /// - Parameters:
    ///   - messages: the message details
    ///   - groupId: the group identifier
    ///   - completionHandler: the callback method
    func addMessages(
        _ messages: [MessageInput],
        toThread threadId: String,
        completionHandler: @escaping (Result<[MessageOutputDTO], Error>) -> ()
    )
    
    /// Invite a list of phone numbers to a share, referenced by its group identifier
    /// - Parameters:
    ///   - phoneNumbers: the list of phone numbers to add to the invite
    ///   - groupId: the group id
    ///   - completionHandler: the callback method
    func invite(
        _ phoneNumbers: [String], 
        to groupId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    )
    
    /// Remove a list of phone numbers to a share, referenced by its group identifier, if they were previously invited
    /// - Parameters:
    ///   - phoneNumbers: the list of phone numbers to remove from the invite
    ///   - groupId: the group id
    ///   - completionHandler: the callback method
    func uninvite(
        _ phoneNumbers: [String],
        from groupId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    )

    func avatarImage(for user: any SHServerUser) async throws -> Data?
    
    /// Save the user avatar image
    /// - Parameter data: the image data
    /// - Returns: the URL the image has been saved to
    func saveAvatarImage(data: Data, for user: any SHServerUser) async throws
    
    /// Delete the user avatar image, if any exists
    func deleteAvatarImage(for user: any SHServerUser) async throws
    
    func updateAssetFingerprint(for: GlobalIdentifier, _ fingerprint: PerceptualHash) async throws
    
    func searchSimilarAssets(to fingerprint: PerceptualHash) async throws
}
