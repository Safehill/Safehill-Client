//
//  MockLocalServer.swift
//  Safehill-Client
//
//  Created by Gennaro on 5/19/25.
//

@testable import Safehill_Client
import Foundation
import Contacts

class MockLocalServer: SHLocalServerAPI {
    
    var stubbedAssets: [GlobalIdentifier: any SHEncryptedAsset] = [:]
    
    init(requestor: any SHLocalUserProtocol,
         stubbedAssets: [GlobalIdentifier : any SHEncryptedAsset]) {
        self.requestor = requestor
        self.stubbedAssets = stubbedAssets
    }
    
    func create(assets: [any SHEncryptedAsset], groupId: String, createdBy: any SHServerUser, createdAt: Date, createdFromThreadId: String?, permissions: Int?, filterVersions: [SHAssetQuality]?, overwriteFileIfExists: Bool, completionHandler: @escaping (Result<[SHServerAsset], any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func createOrUpdateUser(identifier: UserIdentifier, name: String, publicKeyData: Data, publicSignatureData: Data, completionHandler: @escaping (Result<any SHServerUser, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func update(user: SHRemoteUser, phoneNumber: SHPhoneNumber, linkedSystemContact: CNContact, completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func deleteUsers(withIdentifiers identifiers: [UserIdentifier], completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func removeLinkedSystemContact(from users: [SHRemoteUserLinkedToContact], completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func unshareAll(with userIdentifiers: [UserIdentifier], completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func removeGroupIds(_ groupIds: [String], completionHandler: @escaping (Result<Void, any Error>) -> Void) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func updateUserGroupInfo(basedOn sharingInfoByAssetId: [GlobalIdentifier : any SHDescriptorSharingInfo], versions: [SHAssetQuality]?, completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func updateGroupIds(_ groupInfoById: [String : GroupInfoDiff], completionHandler: @escaping (Result<Void, any Error>) -> Void) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func removeAssetRecipients(basedOn userIdsToRemoveFromAssetGid: [GlobalIdentifier : [UserIdentifier]], versions: [SHAssetQuality]?, completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func runDataMigrations(currentBuild: String?, completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func create(assets: [any SHEncryptedAsset], descriptorsByGlobalIdentifier: [GlobalIdentifier : any SHAssetDescriptor], uploadState: SHAssetDescriptorUploadState, overwriteFileIfExists: Bool, completionHandler: @escaping (Result<[SHServerAsset], any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func getAssetDescriptors(forAssetGlobalIdentifiers globalIdentifiers: [GlobalIdentifier], filteringGroupIds: [String]?, after: Date?, useCache: Bool, completionHandler: @escaping (Result<[any SHAssetDescriptor], any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func deleteAllAssets(completionHandler: @escaping (Result<[String], any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func setGroupTitle(encryptedTitle: String, groupId: String, completionHandler: @escaping (Result<Void, any Error>) -> Void) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func createOrUpdateThread(serverThread: ConversationThreadOutputDTO, completionHandler: @escaping (Result<ConversationThreadOutputDTO, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func listThreads(withIdentifiers: [String]?, completionHandler: @escaping (Result<[ConversationThreadOutputDTO], any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func updateThreads(from remoteThreads: [any ConversationThreadUpdate], completionHandler: @escaping (Result<Void, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func retrieveInteraction(anchorType: SHInteractionAnchor, anchorId: String, withId interactionIdentifier: String, completionHandler: @escaping (Result<InteractionsGroupDTO, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    var requestor: any SHLocalUserProtocol
    
    func createOrUpdateUser(name: String, completionHandler: @escaping (Result<any SHServerUser, any Error>) -> ()) {
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
    
    func getUsers(withIdentifiers: [String]?, completionHandler: @escaping (Result<[any SHServerUser], any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func countUploaded(completionHandler: @escaping (Result<Int, any Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func getAssetDescriptors(forAssetGlobalIdentifiers: [GlobalIdentifier], filteringGroupIds: [String]?, after: Date?, completionHandler: @escaping (Result<[any SHAssetDescriptor], any Error>) -> ()) {
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
    
    func markAsset(with assetGlobalIdentifier: GlobalIdentifier, quality: SHAssetQuality, as: SHAssetDescriptorUploadState, completionHandler: @escaping (Result<Void, any Error>) -> ()) {
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
    
    func getAssets(inThread threadId: String, completionHandler: @escaping (Result<ConversationThreadAssetsDTO, any Error>) -> ()) {
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
    
    func getAssets(withGlobalIdentifiers: [GlobalIdentifier], versions: [SHAssetQuality], completion: @escaping (Result<[GlobalIdentifier: any SHEncryptedAsset], Error>) -> ()) {
        completion(.success(stubbedAssets))
    }

    func create(assets: [any SHEncryptedAsset], descriptorsByGlobalIdentifier: [GlobalIdentifier: any SHAssetDescriptor], uploadState: SHAssetDescriptorUploadState, completion: @escaping (Result<Void, Error>) -> ()) {
        completion(.success(()))
    }
}
