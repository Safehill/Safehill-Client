//
//  CollectionInfoTests.swift
//  Safehill-Client
//

import XCTest
@testable import Safehill_Client

final class CollectionInfoTests: XCTestCase {

    // MARK: - Serialization Tests

    func testSerializeDBSecureSerializableAssetCollectionInfo() throws {
        let collectionInfo = DBSecureSerializableAssetCollectionInfo(
            collectionId: "collection-123",
            collectionName: "My Collection",
            visibility: "public",
            accessType: "granted",
            addedAt: "2024-01-15T10:30:00Z"
        )

        // Serialize
        let data = try NSKeyedArchiver.archivedData(withRootObject: collectionInfo, requiringSecureCoding: true)

        // Deserialize
        let deserialized = try DBSecureSerializableAssetCollectionInfo.from(data)

        // Verify
        XCTAssertEqual(collectionInfo.collectionId, deserialized.collectionId)
        XCTAssertEqual(collectionInfo.collectionName, deserialized.collectionName)
        XCTAssertEqual(collectionInfo.visibility, deserialized.visibility)
        XCTAssertEqual(collectionInfo.accessType, deserialized.accessType)
        XCTAssertEqual(collectionInfo.addedAt, deserialized.addedAt)
    }

    func testSerializeMultipleCollectionInfos() throws {
        let collectionInfos = [
            DBSecureSerializableAssetCollectionInfo(
                collectionId: "collection-1",
                collectionName: "Public Collection",
                visibility: "public",
                accessType: "granted",
                addedAt: "2024-01-15T10:30:00Z"
            ),
            DBSecureSerializableAssetCollectionInfo(
                collectionId: "collection-2",
                collectionName: "Confidential Collection",
                visibility: "confidential",
                accessType: "accessed",
                addedAt: "2024-01-16T11:00:00Z"
            )
        ]

        for original in collectionInfos {
            let data = try NSKeyedArchiver.archivedData(withRootObject: original, requiringSecureCoding: true)
            let deserialized = try DBSecureSerializableAssetCollectionInfo.from(data)

            XCTAssertEqual(original.collectionId, deserialized.collectionId)
            XCTAssertEqual(original.collectionName, deserialized.collectionName)
            XCTAssertEqual(original.visibility, deserialized.visibility)
        }
    }

    // MARK: - Diff Detection Tests

    func testDiffDetectsCollectionChanges() throws {
        let user = SHLocalUser.create(keychainPrefix: "com.gf.safehill.test.diff")
        let authedUser = SHAuthenticatedLocalUser(
            localUser: user,
            name: "Test User",
            phoneNumber: nil,
            encryptionProtocolSalt: kTestStaticProtocolSalt,
            authToken: "token"
        )

        let assetGid = "asset-123"

        // Local descriptor with no collections
        let localDescriptor = SHGenericAssetDescriptor(
            globalIdentifier: assetGid,
            localIdentifier: "local-123",
            creationDate: Date(),
            uploadState: .completed,
            sharingInfo: SHGenericDescriptorSharingInfo(
                sharedByUserIdentifier: user.identifier,
                groupIdsByRecipientUserIdentifier: [user.identifier: ["group-1"]],
                groupInfoById: [
                    "group-1": SHGenericAssetGroupInfo(
                        encryptedTitle: nil,
                        createdBy: user.identifier,
                        createdAt: Date(),
                        createdFromThreadId: nil,
                        invitedUsersPhoneNumbers: nil,
                        permissions: 0
                    )
                ],
                collectionInfoById: [:]
            )
        )

        // Remote descriptor with a new collection
        let remoteDescriptor = SHGenericAssetDescriptor(
            globalIdentifier: assetGid,
            localIdentifier: "local-123",
            creationDate: Date(),
            uploadState: .completed,
            sharingInfo: SHGenericDescriptorSharingInfo(
                sharedByUserIdentifier: user.identifier,
                groupIdsByRecipientUserIdentifier: [user.identifier: ["group-1"]],
                groupInfoById: [
                    "group-1": SHGenericAssetGroupInfo(
                        encryptedTitle: nil,
                        createdBy: user.identifier,
                        createdAt: Date(),
                        createdFromThreadId: nil,
                        invitedUsersPhoneNumbers: nil,
                        permissions: 0
                    )
                ],
                collectionInfoById: [
                    "collection-1": SHGenericAssetCollectionInfo(
                        collectionId: "collection-1",
                        collectionName: "My Collection",
                        visibility: "public",
                        accessType: "granted",
                        addedAt: "2024-01-15T10:30:00Z"
                    )
                ]
            )
        )

        let diff = AssetDescriptorsDiff.generateUsing(
            remote: [remoteDescriptor],
            local: [localDescriptor],
            for: authedUser
        )

        // Should detect collection change
        XCTAssertFalse(diff.collectionChangesByAssetGid.isEmpty)
        XCTAssertNotNil(diff.collectionChangesByAssetGid[assetGid])
    }

    func testDiffDetectsCollectionRemovals() throws {
        let user = SHLocalUser.create(keychainPrefix: "com.gf.safehill.test.diff2")
        let authedUser = SHAuthenticatedLocalUser(
            localUser: user,
            name: "Test User",
            phoneNumber: nil,
            encryptionProtocolSalt: kTestStaticProtocolSalt,
            authToken: "token"
        )

        let assetGid = "asset-456"

        // Local descriptor with a collection
        let localDescriptor = SHGenericAssetDescriptor(
            globalIdentifier: assetGid,
            localIdentifier: "local-456",
            creationDate: Date(),
            uploadState: .completed,
            sharingInfo: SHGenericDescriptorSharingInfo(
                sharedByUserIdentifier: user.identifier,
                groupIdsByRecipientUserIdentifier: [user.identifier: ["group-1"]],
                groupInfoById: [
                    "group-1": SHGenericAssetGroupInfo(
                        encryptedTitle: nil,
                        createdBy: user.identifier,
                        createdAt: Date(),
                        createdFromThreadId: nil,
                        invitedUsersPhoneNumbers: nil,
                        permissions: 0
                    )
                ],
                collectionInfoById: [
                    "collection-1": SHGenericAssetCollectionInfo(
                        collectionId: "collection-1",
                        collectionName: "Old Collection",
                        visibility: "public",
                        accessType: "granted",
                        addedAt: "2024-01-15T10:30:00Z"
                    )
                ]
            )
        )

        // Remote descriptor without the collection
        let remoteDescriptor = SHGenericAssetDescriptor(
            globalIdentifier: assetGid,
            localIdentifier: "local-456",
            creationDate: Date(),
            uploadState: .completed,
            sharingInfo: SHGenericDescriptorSharingInfo(
                sharedByUserIdentifier: user.identifier,
                groupIdsByRecipientUserIdentifier: [user.identifier: ["group-1"]],
                groupInfoById: [
                    "group-1": SHGenericAssetGroupInfo(
                        encryptedTitle: nil,
                        createdBy: user.identifier,
                        createdAt: Date(),
                        createdFromThreadId: nil,
                        invitedUsersPhoneNumbers: nil,
                        permissions: 0
                    )
                ],
                collectionInfoById: [:]
            )
        )

        let diff = AssetDescriptorsDiff.generateUsing(
            remote: [remoteDescriptor],
            local: [localDescriptor],
            for: authedUser
        )

        // Should detect collection removal
        XCTAssertFalse(diff.collectionRemovalsByAssetGid.isEmpty)
        XCTAssertNotNil(diff.collectionRemovalsByAssetGid[assetGid])
        XCTAssertTrue(diff.collectionRemovalsByAssetGid[assetGid]!.contains("collection-1"))
    }

    func testDiffIgnoresDropboxCollection() throws {
        let user = SHLocalUser.create(keychainPrefix: "com.gf.safehill.test.diff3")
        let authedUser = SHAuthenticatedLocalUser(
            localUser: user,
            name: "Test User",
            phoneNumber: nil,
            encryptionProtocolSalt: kTestStaticProtocolSalt,
            authToken: "token"
        )

        let assetGid = "asset-789"

        // Local descriptor with no collections (dropbox is implicit)
        let localDescriptor = SHGenericAssetDescriptor(
            globalIdentifier: assetGid,
            localIdentifier: "local-789",
            creationDate: Date(),
            uploadState: .completed,
            sharingInfo: SHGenericDescriptorSharingInfo(
                sharedByUserIdentifier: user.identifier,
                groupIdsByRecipientUserIdentifier: [user.identifier: ["group-1"]],
                groupInfoById: [
                    "group-1": SHGenericAssetGroupInfo(
                        encryptedTitle: nil,
                        createdBy: user.identifier,
                        createdAt: Date(),
                        createdFromThreadId: nil,
                        invitedUsersPhoneNumbers: nil,
                        permissions: 0
                    )
                ],
                collectionInfoById: [:]
            )
        )

        // Remote descriptor with only dropbox collection
        let remoteDescriptor = SHGenericAssetDescriptor(
            globalIdentifier: assetGid,
            localIdentifier: "local-789",
            creationDate: Date(),
            uploadState: .completed,
            sharingInfo: SHGenericDescriptorSharingInfo(
                sharedByUserIdentifier: user.identifier,
                groupIdsByRecipientUserIdentifier: [user.identifier: ["group-1"]],
                groupInfoById: [
                    "group-1": SHGenericAssetGroupInfo(
                        encryptedTitle: nil,
                        createdBy: user.identifier,
                        createdAt: Date(),
                        createdFromThreadId: nil,
                        invitedUsersPhoneNumbers: nil,
                        permissions: 0
                    )
                ],
                collectionInfoById: [
                    "dropbox-id": SHGenericAssetCollectionInfo(
                        collectionId: "dropbox-id",
                        collectionName: kSHDropboxCollectionName, // "Dropbox"
                        visibility: "not-shared",
                        accessType: "granted",
                        addedAt: "2024-01-15T10:30:00Z"
                    )
                ]
            )
        )

        let diff = AssetDescriptorsDiff.generateUsing(
            remote: [remoteDescriptor],
            local: [localDescriptor],
            for: authedUser
        )

        // Should NOT detect any collection changes because dropbox is ignored
        XCTAssertTrue(diff.collectionChangesByAssetGid.isEmpty, "Dropbox collection should be ignored in diff")
    }

    func testDiffNoChangeWhenCollectionsMatch() throws {
        let user = SHLocalUser.create(keychainPrefix: "com.gf.safehill.test.diff4")
        let authedUser = SHAuthenticatedLocalUser(
            localUser: user,
            name: "Test User",
            phoneNumber: nil,
            encryptionProtocolSalt: kTestStaticProtocolSalt,
            authToken: "token"
        )

        let assetGid = "asset-same"
        let addedAt = "2024-01-15T10:30:00Z"

        let collectionInfo = SHGenericAssetCollectionInfo(
            collectionId: "collection-1",
            collectionName: "Same Collection",
            visibility: "public",
            accessType: "granted",
            addedAt: addedAt
        )

        // Both local and remote have the same collection
        let localDescriptor = SHGenericAssetDescriptor(
            globalIdentifier: assetGid,
            localIdentifier: "local-same",
            creationDate: Date(),
            uploadState: .completed,
            sharingInfo: SHGenericDescriptorSharingInfo(
                sharedByUserIdentifier: user.identifier,
                groupIdsByRecipientUserIdentifier: [user.identifier: ["group-1"]],
                groupInfoById: [
                    "group-1": SHGenericAssetGroupInfo(
                        encryptedTitle: nil,
                        createdBy: user.identifier,
                        createdAt: Date(),
                        createdFromThreadId: nil,
                        invitedUsersPhoneNumbers: nil,
                        permissions: 0
                    )
                ],
                collectionInfoById: ["collection-1": collectionInfo]
            )
        )

        let remoteDescriptor = SHGenericAssetDescriptor(
            globalIdentifier: assetGid,
            localIdentifier: "local-same",
            creationDate: Date(),
            uploadState: .completed,
            sharingInfo: SHGenericDescriptorSharingInfo(
                sharedByUserIdentifier: user.identifier,
                groupIdsByRecipientUserIdentifier: [user.identifier: ["group-1"]],
                groupInfoById: [
                    "group-1": SHGenericAssetGroupInfo(
                        encryptedTitle: nil,
                        createdBy: user.identifier,
                        createdAt: Date(),
                        createdFromThreadId: nil,
                        invitedUsersPhoneNumbers: nil,
                        permissions: 0
                    )
                ],
                collectionInfoById: ["collection-1": collectionInfo]
            )
        )

        let diff = AssetDescriptorsDiff.generateUsing(
            remote: [remoteDescriptor],
            local: [localDescriptor],
            for: authedUser
        )

        // Should NOT detect any changes
        XCTAssertTrue(diff.collectionChangesByAssetGid.isEmpty, "No changes should be detected when collections match")
        XCTAssertTrue(diff.collectionRemovalsByAssetGid.isEmpty, "No removals should be detected when collections match")
    }

    // MARK: - canBeSavedOrShared Tests

    func testCanBeSavedOrSharedByOwner() throws {
        let ownerId = "owner-123"
        let recipientId = "recipient-456"

        let descriptor = SHGenericAssetDescriptor(
            globalIdentifier: "asset-1",
            localIdentifier: "local-1",
            creationDate: Date(),
            uploadState: .completed,
            sharingInfo: SHGenericDescriptorSharingInfo(
                sharedByUserIdentifier: ownerId,
                groupIdsByRecipientUserIdentifier: [
                    ownerId: ["group-1"],
                    recipientId: ["group-1"]
                ],
                groupInfoById: [
                    "group-1": SHGenericAssetGroupInfo(
                        encryptedTitle: nil,
                        createdBy: ownerId,
                        createdAt: Date(),
                        createdFromThreadId: nil,
                        invitedUsersPhoneNumbers: nil,
                        permissions: 0 // confidential
                    )
                ],
                collectionInfoById: [:]
            )
        )

        // Owner can always save/share
        XCTAssertTrue(descriptor.canBeSavedOrShared(by: ownerId))

        // Recipient cannot (confidential permissions, no public collection)
        XCTAssertFalse(descriptor.canBeSavedOrShared(by: recipientId))
    }

    func testCanBeSavedOrSharedByShareableGroup() throws {
        let ownerId = "owner-123"
        let recipientId = "recipient-456"

        let descriptor = SHGenericAssetDescriptor(
            globalIdentifier: "asset-2",
            localIdentifier: "local-2",
            creationDate: Date(),
            uploadState: .completed,
            sharingInfo: SHGenericDescriptorSharingInfo(
                sharedByUserIdentifier: ownerId,
                groupIdsByRecipientUserIdentifier: [
                    ownerId: ["group-1"],
                    recipientId: ["group-1"]
                ],
                groupInfoById: [
                    "group-1": SHGenericAssetGroupInfo(
                        encryptedTitle: nil,
                        createdBy: ownerId,
                        createdAt: Date(),
                        createdFromThreadId: nil,
                        invitedUsersPhoneNumbers: nil,
                        permissions: 1 // shareable
                    )
                ],
                collectionInfoById: [:]
            )
        )

        // Both can save/share due to shareable permissions
        XCTAssertTrue(descriptor.canBeSavedOrShared(by: ownerId))
        XCTAssertTrue(descriptor.canBeSavedOrShared(by: recipientId))
    }

    func testCanBeSavedOrSharedByPublicCollection() throws {
        let ownerId = "owner-123"
        let recipientId = "recipient-456"

        let descriptor = SHGenericAssetDescriptor(
            globalIdentifier: "asset-3",
            localIdentifier: "local-3",
            creationDate: Date(),
            uploadState: .completed,
            sharingInfo: SHGenericDescriptorSharingInfo(
                sharedByUserIdentifier: ownerId,
                groupIdsByRecipientUserIdentifier: [
                    ownerId: ["group-1"],
                    recipientId: ["group-1"]
                ],
                groupInfoById: [
                    "group-1": SHGenericAssetGroupInfo(
                        encryptedTitle: nil,
                        createdBy: ownerId,
                        createdAt: Date(),
                        createdFromThreadId: nil,
                        invitedUsersPhoneNumbers: nil,
                        permissions: 0 // confidential
                    )
                ],
                collectionInfoById: [
                    "collection-1": SHGenericAssetCollectionInfo(
                        collectionId: "collection-1",
                        collectionName: "Public Collection",
                        visibility: "public",
                        accessType: "granted",
                        addedAt: "2024-01-15T10:30:00Z"
                    )
                ]
            )
        )

        // Both can save/share due to public collection
        XCTAssertTrue(descriptor.canBeSavedOrShared(by: ownerId))
        XCTAssertTrue(descriptor.canBeSavedOrShared(by: recipientId))
    }

    func testCanBeSavedOrSharedUserNotInGroup() throws {
        let ownerId = "owner-123"
        let recipientId = "recipient-456"
        let outsiderId = "outsider-789"

        let descriptor = SHGenericAssetDescriptor(
            globalIdentifier: "asset-4",
            localIdentifier: "local-4",
            creationDate: Date(),
            uploadState: .completed,
            sharingInfo: SHGenericDescriptorSharingInfo(
                sharedByUserIdentifier: ownerId,
                groupIdsByRecipientUserIdentifier: [
                    ownerId: ["group-1"],
                    recipientId: ["group-1"]
                ],
                groupInfoById: [
                    "group-1": SHGenericAssetGroupInfo(
                        encryptedTitle: nil,
                        createdBy: ownerId,
                        createdAt: Date(),
                        createdFromThreadId: nil,
                        invitedUsersPhoneNumbers: nil,
                        permissions: 1 // shareable
                    )
                ],
                collectionInfoById: [:]
            )
        )

        // Owner and recipient can save/share
        XCTAssertTrue(descriptor.canBeSavedOrShared(by: ownerId))
        XCTAssertTrue(descriptor.canBeSavedOrShared(by: recipientId))

        // Outsider cannot (not in any group)
        XCTAssertFalse(descriptor.canBeSavedOrShared(by: outsiderId))
    }

    func testCanBeSavedOrSharedMostPermissiveWins() throws {
        let ownerId = "owner-123"
        let recipientId = "recipient-456"

        // Recipient is in two groups - one confidential, one shareable
        let descriptor = SHGenericAssetDescriptor(
            globalIdentifier: "asset-5",
            localIdentifier: "local-5",
            creationDate: Date(),
            uploadState: .completed,
            sharingInfo: SHGenericDescriptorSharingInfo(
                sharedByUserIdentifier: ownerId,
                groupIdsByRecipientUserIdentifier: [
                    ownerId: ["group-1"],
                    recipientId: ["group-1", "group-2"]
                ],
                groupInfoById: [
                    "group-1": SHGenericAssetGroupInfo(
                        encryptedTitle: nil,
                        createdBy: ownerId,
                        createdAt: Date(),
                        createdFromThreadId: nil,
                        invitedUsersPhoneNumbers: nil,
                        permissions: 0 // confidential
                    ),
                    "group-2": SHGenericAssetGroupInfo(
                        encryptedTitle: nil,
                        createdBy: ownerId,
                        createdAt: Date(),
                        createdFromThreadId: nil,
                        invitedUsersPhoneNumbers: nil,
                        permissions: 1 // shareable
                    )
                ],
                collectionInfoById: [:]
            )
        )

        // Recipient can save/share because they're in a shareable group
        XCTAssertTrue(descriptor.canBeSavedOrShared(by: recipientId))
    }
}
