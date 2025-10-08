//
//  MockRemoteServer.swift
//  Safehill-Client
//
//  Created by Gennaro on 5/19/25.
//

@testable import Safehill_Client
import Foundation

class MockRemoteServer: SHRemoteServerAPI {
    
    var requestor: any SHLocalUserProtocol
    
    var stubbedDescriptors: [any SHAssetDescriptor] = []
    var stubbedAssets: [GlobalIdentifier: any SHEncryptedAsset] = [:]
    
    init(requestor: any SHLocalUserProtocol,
         stubbedDescriptors: [any SHAssetDescriptor],
         stubbedAssets: [GlobalIdentifier : any SHEncryptedAsset]) {
        self.requestor = requestor
        self.stubbedDescriptors = stubbedDescriptors
        self.stubbedAssets = stubbedAssets
    }
    
    func createOrUpdateUser(name: String, completionHandler: @escaping (Result<any SHServerUser, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func sendCodeToUser(countryCode: Int, phoneNumber: Int, code: String, medium: SendCodeToUserRequestDTO.Medium, appName: String, completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func updateUser(name: String?, phoneNumber: SHPhoneNumber?, forcePhoneNumberLinking: Bool, completionHandler: @escaping (Result<any SHServerUser, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func deleteAccount(name: String, password: String, completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func deleteAccount(completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func signIn(clientBuild: String?, completionHandler: @escaping (Result<SHAuthResponse, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func getUsers(withIdentifiers: [String]?, completionHandler: @escaping (Result<[any SHServerUser], any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func getUsers(withHashedPhoneNumbers hashedPhoneNumbers: [String], completionHandler: @escaping (Result<[String : any SHServerUser], any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func searchUsers(query: String, completionHandler: @escaping (Result<[any SHServerUser], any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func authorizeUsers(with userPublicIdentifiers: [String], completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func blockUsers(with userPublicIdentifiers: [String], completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func pendingOrBlockedUsers(completionHandler: @escaping (Result<UserAuthorizationStatusDTO, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func countUploaded(completionHandler: @escaping (Result<Int, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func getAssetDescriptors(after: Date?, completionHandler: @escaping (Result<[any SHAssetDescriptor], any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func getAssets(withGlobalIdentifiers: [String], versions: [SHAssetQuality]?, completionHandler: @escaping (Result<[GlobalIdentifier : any SHEncryptedAsset], any Error>) -> ()) {
        completionHandler(.success(stubbedAssets))
    }
    
    func create(assets: [any SHEncryptedAsset], fingerprintsById: [GlobalIdentifier: AssetFingerprint], groupId: String, filterVersions: [SHAssetQuality]?, force: Bool, completionHandler: @escaping (Result<[SHServerAsset], any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func share(asset: any SHShareableEncryptedAsset, asPhotoMessageInThreadId: String?, permissions: Int?, suppressNotification: Bool, completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func unshare(assetIdsWithUsers: [GlobalIdentifier : [UserIdentifier]], completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func changeGroupPermission(groupId: String, permission: Int, completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func uploadAsset(with globalIdentifier: GlobalIdentifier, versionsDataManifest: [SHAssetQuality : (URL, Data)], completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func markAsset(with assetGlobalIdentifier: GlobalIdentifier, quality: SHAssetQuality, as: SHAssetUploadState, completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func deleteAssets(withGlobalIdentifiers: [String], completionHandler: @escaping (Result<[String], any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func validateTransaction(originalTransactionId: String, receipt: String, productId: String, completionHandler: @escaping (Result<SHReceiptValidationResponse, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func createOrUpdateThread(name: String?, recipientsEncryptionDetails: [RecipientEncryptionDetailsDTO]?, invitedPhoneNumbers: [String]?, completionHandler: @escaping (Result<ConversationThreadOutputDTO, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func updateThread(_ threadId: String, newName: String?, completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func updateThreadMembers(for threadId: String, _ update: ConversationThreadMembersUpdateDTO, completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func listThreads(completionHandler: @escaping (Result<[ConversationThreadOutputDTO], any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func getThread(withId threadId: String, completionHandler: @escaping (Result<ConversationThreadOutputDTO?, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func deleteThread(withId threadId: String, completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func getThread(withUserIds userIds: [UserIdentifier], and phoneNumbers: [String], completionHandler: @escaping (Result<ConversationThreadOutputDTO?, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func getAssets(inThread threadId: String, completionHandler: @escaping (Result<SharedAssetsLibraryDTO, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func setupGroup(groupId: String, encryptedTitle: String?, recipientsEncryptionDetails: [RecipientEncryptionDetailsDTO], completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func deleteGroup(groupId: String, completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func retrieveGroupDetails(forGroup groupId: String, completionHandler: @escaping (Result<InteractionsGroupDetailsResponseDTO?, any Error>) -> Void) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func retrieveUserEncryptionDetails(forGroup groupId: String, completionHandler: @escaping (Result<RecipientEncryptionDetailsDTO?, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func topLevelInteractionsSummary(completionHandler: @escaping (Result<InteractionsSummaryDTO, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func topLevelThreadsInteractionsSummary(completionHandler: @escaping (Result<[String : InteractionsThreadSummaryDTO], any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func topLevelGroupsInteractionsSummary(completionHandler: @escaping (Result<[String : InteractionsGroupSummaryDTO], any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func topLevelInteractionsSummary(inGroup groupId: String, completionHandler: @escaping (Result<InteractionsGroupSummaryDTO, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func addReactions(_: [any ReactionInput], toGroup groupId: String, completionHandler: @escaping (Result<[ReactionOutputDTO], any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func addReactions(_: [any ReactionInput], toThread threadId: String, completionHandler: @escaping (Result<[ReactionOutputDTO], any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func removeReaction(_ reactionType: ReactionType, senderPublicIdentifier: UserIdentifier, inReplyToAssetGlobalIdentifier: GlobalIdentifier?, inReplyToInteractionId: String?, fromGroup groupId: String, completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func removeReaction(_ reactionType: ReactionType, senderPublicIdentifier: UserIdentifier, inReplyToAssetGlobalIdentifier: GlobalIdentifier?, inReplyToInteractionId: String?, fromThread threadId: String, completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func retrieveInteractions(inGroup groupId: String, ofType type: InteractionType?, underMessage refMessageId: String?, before: Date?, limit: Int, completionHandler: @escaping (Result<InteractionsGroupDTO, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func retrieveInteractions(inThread threadId: String, ofType type: InteractionType?, underMessage refMessageId: String?, before: Date?, limit: Int, completionHandler: @escaping (Result<InteractionsGroupDTO, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func addMessages(_ messages: [any MessageInput], toGroup groupId: String, completionHandler: @escaping (Result<[MessageOutputDTO], any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func addMessages(_ messages: [any MessageInput], toThread threadId: String, completionHandler: @escaping (Result<[MessageOutputDTO], any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func invite(_ phoneNumbers: [String], to groupId: String, completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func uninvite(_ phoneNumbers: [String], from groupId: String, completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func requestAccess(toThreadId: String, completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func requestAccess(toGroupId: String, completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func avatarImage(for user: any SHServerUser) async throws -> Data? {
        return nil
    }
    
    func saveAvatarImage(data: Data, for user: any SHServerUser) async throws {
        
    }
    
    func deleteAvatarImage(for user: any SHServerUser) async throws {
        
    }
    
    func updateAssetFingerprint(for: GlobalIdentifier, _ fingerprint: AssetFingerprint) async throws {
        
    }
    
    func searchSimilarAssets(to fingerprint: AssetFingerprint) async throws {
        
    }
    
    func registerDevice(_ deviceId: String, token: String?, appBundleId: String?, completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }

    func getAssetDescriptors(forAssetGlobalIdentifiers: [GlobalIdentifier], filteringGroupIds: [String]?, after: Date?, completionHandler: @escaping (Result<[any SHAssetDescriptor], Error>) -> ()) {
        completionHandler(.success(stubbedDescriptors))
    }

    func getRemoteAssets(withGlobalIdentifiers: [GlobalIdentifier], versions: [SHAssetQuality]) async throws -> [GlobalIdentifier: any SHEncryptedAsset] {
        return stubbedAssets
    }
    
    func sendEncryptedKeysToWebClient(sessionId: String, requestorIp: String, encryptedPrivateKeyData: Data, encryptedPrivateKeyIvData: Data, encryptedPrivateSignatureData: Data, encryptedPrivateSignatureIvData: Data) async throws {
    }

    func createCollection(name: String, description: String, completionHandler: @escaping (Result<CollectionOutputDTO, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }

    func retrieveCollections(completionHandler: @escaping (Result<[CollectionOutputDTO], any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }

    func retrieveCollection(id: String, completionHandler: @escaping (Result<CollectionOutputDTO, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }

    func updateCollection(id: String, name: String?, description: String?, pricing: Double?, completionHandler: @escaping (Result<CollectionOutputDTO, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }

    func trackCollectionAccess(id: String, completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }

    func searchCollections(query: String?, searchScope: String, visibility: String?, priceRange: PriceRangeDTO?, completionHandler: @escaping (Result<[CollectionOutputDTO], any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
}
