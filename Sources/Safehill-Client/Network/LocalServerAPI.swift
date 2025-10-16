import Foundation
import Contacts

public protocol SHLocalServerAPI : SHServerAPI {
    
    func create(
        assets: [any SHEncryptedAsset],
        groupId: String,
        createdBy: any SHServerUser,
        createdAt: Date,
        createdFromThreadId: String?,
        permissions: Int?,
        filterVersions: [SHAssetQuality]?,
        overwriteFileIfExists: Bool,
        completionHandler: @escaping (Result<[SHServerAsset], Error>) -> ()
    )
    
    func createOrUpdateUser(
        identifier: UserIdentifier,
        name: String,
        publicKeyData: Data,
        publicSignatureData: Data,
        completionHandler: @escaping (Result<any SHServerUser, Error>) -> ()
    )
    
    func update(user: SHRemoteUser,
                phoneNumber: SHPhoneNumber,
                linkedSystemContact: CNContact,
                completionHandler: @escaping (Swift.Result<Void, Error>) -> ())
    
    func deleteUsers(withIdentifiers identifiers: [UserIdentifier],
                     completionHandler: @escaping (Result<Void, Error>) -> ())
    
    func removeLinkedSystemContact(from users: [SHRemoteUserLinkedToContact],
                                   completionHandler: @escaping (Swift.Result<Void, Error>) -> ())
    
    func unshareAll(with userIdentifiers: [UserIdentifier],
                    completionHandler: @escaping (Result<Void, Error>) -> ())
    
    func removeGroupIds(_ groupIds: [String],
                        completionHandler: @escaping (Result<Void, Error>) -> Void)
    
    func updateUserGroupInfo(
        basedOn sharingInfoByAssetId: [GlobalIdentifier: any SHDescriptorSharingInfo],
        versions: [SHAssetQuality]?,
        completionHandler: @escaping (Result<Void, Error>) -> ())
    
    func updateGroupIds(_ groupInfoById: [String: GroupInfoDiff],
                        completionHandler: @escaping (Result<Void, Error>) -> Void)
    
    func removeAssetRecipients(basedOn userIdsToRemoveFromAssetGid: [GlobalIdentifier: [UserIdentifier]],
                               versions: [SHAssetQuality]?,
                               completionHandler: @escaping (Result<Void, Error>) -> ())
    
    func runDataMigrations(
        currentBuild: String?,
        completionHandler: @escaping (Swift.Result<Void, Error>) -> ()
    )
    
    func create(assets: [any SHEncryptedAsset],
                descriptorsByGlobalIdentifier: [GlobalIdentifier: any SHAssetDescriptor],
                uploadState: SHAssetUploadState,
                overwriteFileIfExists: Bool,
                completionHandler: @escaping (Result<[SHServerAsset], Error>) -> ())
    
    func getAssetDescriptors(
        forAssetGlobalIdentifiers globalIdentifiers: [GlobalIdentifier],
        filteringGroupIds: [String]?,
        after: Date?,
        useCache: Bool,
        completionHandler: @escaping (Result<[any SHAssetDescriptor], Error>) -> ()
    )
    
    func deleteAllAssets(completionHandler: @escaping (Result<[String], Error>) -> ())
    
    func setGroupTitle(
        encryptedTitle: String,
        groupId: String,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    )
    
    func createOrUpdateThread(
        serverThread: ConversationThreadOutputDTO,
        completionHandler: @escaping (Result<ConversationThreadOutputDTO, Error>) -> ()
    )
    
    func listThreads(
        withIdentifiers: [String]?,
        completionHandler: @escaping (Result<[ConversationThreadOutputDTO], Error>) -> ()
    )
    
    func updateThreads(
        from remoteThreads: [ConversationThreadUpdate],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    )
    
    func retrieveInteraction(
        anchorType: SHInteractionAnchor,
        anchorId: String,
        withId interactionIdentifier: String,
        completionHandler: @escaping (Result<InteractionsGroupDTO, Error>) -> ()
    )
}
