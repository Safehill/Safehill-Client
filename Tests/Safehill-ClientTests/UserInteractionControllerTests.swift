import XCTest
@testable import Safehill_Client
import Safehill_Crypto

struct MockThreadDetails {
    let threadId: String
    let name: String?
    let userIds: [String]
    let encryptionDetails: [RecipientEncryptionDetailsDTO]
}

class SHMockServerProxyState {
    var threads: [MockThreadDetails]?
    
    init(threads: [MockThreadDetails]? = nil) {
        self.threads = threads
    }
}

struct SHMockServerProxy: SHServerProxyProtocol {
    
    let localServer: LocalServer
    let state: SHMockServerProxyState
    
    init(user: SHLocalUserProtocol) {
        self.localServer = LocalServer(requestor: user)
        self.state = SHMockServerProxyState()
    }
    
    init(user: SHLocalUser, threads: [MockThreadDetails]? = nil) {
        self.localServer = LocalServer(requestor: user)
        self.state = SHMockServerProxyState(threads: threads)
    }
    
    func setupGroupEncryptionDetails(groupId: String, recipientsEncryptionDetails: [RecipientEncryptionDetailsDTO], completionHandler: @escaping (Result<Void, Error>) -> ()) {
        self.localServer.setGroupEncryptionDetails(
            groupId: groupId,
            recipientsEncryptionDetails: recipientsEncryptionDetails,
            completionHandler: completionHandler
        )
    }
    
    func addReactions(_ reactions: [ReactionInput], inGroup groupId: String, completionHandler: @escaping (Result<[ReactionOutputDTO], Error>) -> ()) {
        self.localServer.addReactions(reactions, inGroup: groupId, completionHandler: completionHandler)
    }
    
    func addMessage(_ message: MessageInputDTO, inGroup groupId: String, completionHandler: @escaping (Result<MessageOutputDTO, Error>) -> ()) {
        let messageOutput = MessageOutputDTO(
            interactionId: UUID().uuidString,
            senderUserIdentifier: self.localServer.requestor.identifier,
            inReplyToAssetGlobalIdentifier: message.inReplyToAssetGlobalIdentifier,
            inReplyToInteractionId: message.inReplyToInteractionId,
            encryptedMessage: message.encryptedMessage,
            createdAt: Date().iso8601withFractionalSeconds
        )
        self.localServer.addMessages([messageOutput], inGroup: groupId) { result in
            switch result {
            case .success(let messages):
                completionHandler(.success(messages.first!))
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    func addMessage(_ message: MessageInputDTO, inThread threadId: String, completionHandler: @escaping (Result<MessageOutputDTO, Error>) -> ()) {
        let messageOutput = MessageOutputDTO(
            interactionId: UUID().uuidString,
            senderUserIdentifier: self.localServer.requestor.identifier,
            inReplyToAssetGlobalIdentifier: message.inReplyToAssetGlobalIdentifier,
            inReplyToInteractionId: message.inReplyToInteractionId,
            encryptedMessage: message.encryptedMessage,
            createdAt: Date().iso8601withFractionalSeconds
        )
        self.localServer.addMessages([messageOutput], inThread: threadId) { result in
            switch result {
            case .success(let messages):
                completionHandler(.success(messages.first!))
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    func retrieveInteractions(inGroup groupId: String, underMessage messageId: String?, per: Int, page: Int, completionHandler: @escaping (Result<InteractionsGroupDTO, Error>) -> ()) {
        self.localServer.retrieveInteractions(inGroup: groupId, underMessage: messageId, per: per, page: page, completionHandler: completionHandler)
    }
    
    func retrieveInteractions(inThread threadId: String, underMessage messageId: String?, per: Int, page: Int, completionHandler: @escaping (Result<InteractionsGroupDTO, Error>) -> ()) {
        self.localServer.retrieveInteractions(inThread: threadId, underMessage: messageId, per: per, page: page, completionHandler: completionHandler)
    }
    
    func countLocalInteractions(inGroup groupId: String, completionHandler: @escaping (Result<InteractionsCounts, Error>) -> ()) {
        self.localServer.countInteractions(inGroup: groupId, completionHandler: completionHandler)
    }
    
    func retrieveUserEncryptionDetails(forGroup groupId: String, completionHandler: @escaping (Result<RecipientEncryptionDetailsDTO?, Error>) -> ()) {
        self.localServer.retrieveUserEncryptionDetails(forGroup: groupId, completionHandler: completionHandler)
    }
    
    func deleteGroup(groupId: String, completionHandler: @escaping (Result<Void, Error>) -> ()) {
        self.localServer.deleteGroup(groupId: groupId, completionHandler: completionHandler)
    }
    
    func listThreads(completionHandler: @escaping (Result<[ConversationThreadOutputDTO], Error>) -> ()) {
        do {
            let threads: [ConversationThreadOutputDTO] = try self.state.threads?.map { mockThread in
                let encryptionDetails = mockThread.encryptionDetails
                guard let selfEncryptionDetails = encryptionDetails.first(where: { $0.recipientUserIdentifier == self.localServer.requestor.identifier }) else {
                    throw SHHTTPError.ClientError.badRequest("thread encryption details were not set up for some threads yet on the mock server")
                }
                return ConversationThreadOutputDTO(
                    threadId: mockThread.threadId,
                    name: mockThread.name,
                    membersPublicIdentifier: mockThread.userIds,
                    lastUpdatedAt: Date().iso8601withFractionalSeconds,
                    encryptionDetails: selfEncryptionDetails
                )
            } ?? []
            completionHandler(.success(threads))
        } catch {
            completionHandler(.failure(error))
        }
    }
    
    func listLocalThreads(completionHandler: @escaping (Result<[ConversationThreadOutputDTO], Error>) -> ()) {
        self.listThreads(completionHandler: completionHandler)
    }
    
    func createOrUpdateThread(name: String?, recipientsEncryptionDetails: [RecipientEncryptionDetailsDTO]?, completionHandler: @escaping (Result<ConversationThreadOutputDTO, Error>) -> ()) {
        if let encryptionDetails = recipientsEncryptionDetails {
            guard let selfEncryptionDetails = encryptionDetails.first(where: { $0.recipientUserIdentifier == self.localServer.requestor.identifier }) else {
                completionHandler(.failure(SHHTTPError.ClientError.badRequest("thread encryption details were not set up for some threads yet on the mock server")))
                return
            }
            
            let threadMembersId = encryptionDetails.map({ $0.recipientUserIdentifier })
            guard let threads = self.state.threads,
               let matchingThreadIdx = threads.firstIndex(where: { $0.userIds.count == threadMembersId.count && Set(threadMembersId).subtracting($0.userIds).isEmpty })
            else {
                completionHandler(.failure(SHHTTPError.ClientError.badRequest("thread encryption details for were not set up correctly on the mock server")))
                return
            }
            
            let matchingThread = threads[matchingThreadIdx]
            
            self.state.threads![matchingThreadIdx] = MockThreadDetails(
                threadId: matchingThread.threadId,
                name: matchingThread.name,
                userIds: matchingThread.userIds,
                encryptionDetails: encryptionDetails
            )
            
            let serverThread = ConversationThreadOutputDTO(
                threadId: matchingThread.threadId,
                name: name,
                membersPublicIdentifier: threadMembersId,
                lastUpdatedAt: Date().iso8601withFractionalSeconds,
                encryptionDetails: selfEncryptionDetails
            )
            
            self.localServer.createOrUpdateThread(serverThread: serverThread, completionHandler: completionHandler)
            
        } else {
            completionHandler(.failure(SHHTTPError.ClientError.badRequest("updating a thread with a mock server is not supported")))
        }
    }
    
    func deleteThread(withId threadId: String, completionHandler: @escaping (Result<Void, Error>) -> ()) {
        self.localServer.deleteThread(withId: threadId, completionHandler: completionHandler)
    }
    
    func retrieveUserEncryptionDetails(forThread threadId: String, completionHandler: @escaping (Result<RecipientEncryptionDetailsDTO?, Error>) -> ()) {
        self.localServer.getThread(withId: threadId) { result in
            switch result {
            case .success(let threadOutput):
                completionHandler(.success(threadOutput?.encryptionDetails))
            case .failure(let failure):
                completionHandler(.failure(failure))
            }
        }
    }
    
    func getThread(withUsers users: [any SHServerUser], completionHandler: @escaping (Result<ConversationThreadOutputDTO?, Error>) -> ()) {
        let threadMembersId = users.map({ $0.identifier })
        guard let threads = self.state.threads,
           let matchingThread = threads.first(where: { $0.userIds.count == threadMembersId.count && Set(threadMembersId).subtracting($0.userIds).isEmpty })
        else {
            completionHandler(.success(nil))
            return
        }
        
        guard let selfEncryptionDetails = matchingThread.encryptionDetails.first(where: { $0.recipientUserIdentifier == self.localServer.requestor.identifier }) else {
            completionHandler(.success(nil))
            return
        }
        
        let serverThread = ConversationThreadOutputDTO(
            threadId: matchingThread.threadId,
            name: matchingThread.name,
            membersPublicIdentifier: matchingThread.userIds,
            lastUpdatedAt: Date().iso8601withFractionalSeconds,
            encryptionDetails: selfEncryptionDetails
        )
        completionHandler(.success(serverThread))
    }
    
    func removeReaction(_ reaction: ReactionInput, inGroup groupId: String, completionHandler: @escaping (Result<Void, Error>) -> ()) {
        self.localServer.removeReactions([reaction], inGroup: groupId, completionHandler: completionHandler)
    }
}


final class Safehill_UserInteractionControllerTests: XCTestCase {
    
    let myUser = SHLocalUser(keychainPrefix: "")
    
    override func setUpWithError() throws {
        let _ = try SHDBManager.sharedInstance.userStore?.removeAll()
        let _ = try SHDBManager.sharedInstance.assetStore?.removeAll()
        let _ = try SHDBManager.sharedInstance.reactionStore?.removeAll()
        let _ = try SHDBManager.sharedInstance.messageQueue?.removeAll()
    }
    
    func testKeyStability() throws {
        ServerUserCache.shared.cache(
            users: [
                SHRemoteUser(identifier: myUser.identifier,
                             name: "myUser",
                             publicKeyData: myUser.publicKeyData,
                             publicSignatureData: myUser.publicSignatureData)
            ]
        )
        
        let groupId = "testGroupId"
        let recipient1 = SHLocalCryptoUser()
        
        ServerUserCache.shared.cache(
            users: [
                SHRemoteUser(identifier: recipient1.identifier,
                             name: "recipient1",
                             publicKeyData: recipient1.publicKeyData,
                             publicSignatureData: recipient1.publicSignatureData)
            ]
        )
        
        let serverProxy = SHMockServerProxy(user: myUser)
        
        let authenticatedUser = SHAuthenticatedLocalUser(
            localUser: myUser, 
            name: "myUser",
            encryptionProtocolSalt: kTestStaticProtocolSalt,
            authToken: ""
        )
        
        let controller = SHUserInteractionController(
            user: authenticatedUser,
            serverProxy: serverProxy
        )
        
        let expectation1 = XCTestExpectation(description: "initialize the group")
        controller.setupGroupEncryptionDetails(
            groupId: groupId,
            with: [
                SHRemoteUser(
                    identifier: recipient1.identifier,
                    name: "recipient1",
                    publicKeyData: recipient1.publicKeyData,
                    publicSignatureData: recipient1.publicSignatureData
                )
            ]
        ) { result in
            if case .failure(let err) = result {
                XCTFail(err.localizedDescription)
            }
            expectation1.fulfill()
        }
        
        wait(for: [expectation1], timeout: 5.0)
        
        let symmetricKey = try controller.fetchSymmetricKey(forAnchor: .group, anchorId: groupId)
        let recipientEncryptionDetails = try controller.fetchSelfEncryptionDetails(forAnchor: .group, anchorId: groupId)
        XCTAssertNotNil(recipientEncryptionDetails)
        guard let recipientEncryptionDetails else {
            XCTFail() ; return
        }
        
        let userStore = SHDBManager.sharedInstance.userStore!
        let kvs = try userStore.dictionaryRepresentation()
        XCTAssertEqual(kvs.count, 4)
        XCTAssertEqual(kvs["\(SHInteractionAnchor.group.rawValue)::\(groupId)::ephemeralPublicKey"] as! String, recipientEncryptionDetails.ephemeralPublicKey)
        XCTAssertEqual(kvs["\(SHInteractionAnchor.group.rawValue)::\(groupId)::secretPublicSignature"] as! String, recipientEncryptionDetails.secretPublicSignature)
        XCTAssertEqual(kvs["\(SHInteractionAnchor.group.rawValue)::\(groupId)::senderPublicSignature"] as! String, recipientEncryptionDetails.senderPublicSignature)
        XCTAssertEqual(kvs["\(SHInteractionAnchor.group.rawValue)::\(groupId)::encryptedSecret"] as! String, recipientEncryptionDetails.encryptedSecret)
     
        let expectation2 = XCTestExpectation(description: "initialize the group")
        controller.setupGroupEncryptionDetails(
            groupId: groupId,
            with: [
                SHRemoteUser(
                    identifier: recipient1.identifier,
                    name: "recipient1",
                    publicKeyData: recipient1.publicKeyData,
                    publicSignatureData: recipient1.publicSignatureData
                )
            ]
        ) { result in
            if case .failure(let err) = result {
                XCTFail(err.localizedDescription)
            }
            expectation2.fulfill()
        }
        
        wait(for: [expectation2], timeout: 5.0)
        
        let symmetricKey2 = try controller.fetchSymmetricKey(forAnchor: .group, anchorId: groupId)
        let recipientEncryptionDetails2 = try controller.fetchSelfEncryptionDetails(forAnchor: .group, anchorId: groupId)
        XCTAssertNotNil(recipientEncryptionDetails2)
        guard let recipientEncryptionDetails2 else {
            XCTFail() ; return
        }
        
        let kvs2 = try userStore.dictionaryRepresentation()
        XCTAssertEqual(kvs2.count, 4)
        XCTAssertEqual(kvs2["\(SHInteractionAnchor.group.rawValue)::\(groupId)::ephemeralPublicKey"] as! String, recipientEncryptionDetails2.ephemeralPublicKey)
        XCTAssertEqual(kvs2["\(SHInteractionAnchor.group.rawValue)::\(groupId)::secretPublicSignature"] as! String, recipientEncryptionDetails2.secretPublicSignature)
        XCTAssertEqual(kvs2["\(SHInteractionAnchor.group.rawValue)::\(groupId)::senderPublicSignature"] as! String, recipientEncryptionDetails2.senderPublicSignature)
        XCTAssertEqual(kvs2["\(SHInteractionAnchor.group.rawValue)::\(groupId)::encryptedSecret"] as! String, recipientEncryptionDetails2.encryptedSecret)
        
        XCTAssertEqual(recipientEncryptionDetails.ephemeralPublicKey, recipientEncryptionDetails2.ephemeralPublicKey)
        XCTAssertEqual(recipientEncryptionDetails.secretPublicSignature, recipientEncryptionDetails2.secretPublicSignature)
        XCTAssertEqual(recipientEncryptionDetails.encryptedSecret, recipientEncryptionDetails2.encryptedSecret)
        XCTAssertEqual(recipientEncryptionDetails.senderPublicSignature, recipientEncryptionDetails2.senderPublicSignature)
        
        XCTAssertEqual(symmetricKey, symmetricKey2)
    }
    
    func testSendMessageE2EEInGroup() throws {
        ServerUserCache.shared.cache(
            users: [
                SHRemoteUser(identifier: myUser.identifier,
                             name: "myUser",
                             publicKeyData: myUser.publicKeyData,
                             publicSignatureData: myUser.publicSignatureData)
                ]
            )
        
        let groupId = "testGroupId"
        let recipient1 = SHLocalCryptoUser()
        
        ServerUserCache.shared.cache(
            users: [
                SHRemoteUser(identifier: recipient1.identifier,
                             name: "recipient1",
                             publicKeyData: recipient1.publicKeyData,
                             publicSignatureData: recipient1.publicSignatureData)
                ]
            )
        
        let serverProxy = SHMockServerProxy(user: myUser)
        
        let authenticatedUser = SHAuthenticatedLocalUser(
            localUser: myUser, 
            name: "myUser",
            encryptionProtocolSalt: kTestStaticProtocolSalt,
            authToken: ""
        )
        
        let controller = SHUserInteractionController(
            user: authenticatedUser,
            serverProxy: serverProxy
        )
        
        let expectation1 = XCTestExpectation(description: "initialize the group")
        controller.setupGroupEncryptionDetails(
            groupId: groupId,
            with: [
                SHRemoteUser(
                    identifier: recipient1.identifier,
                    name: "recipient1",
                    publicKeyData: recipient1.publicKeyData,
                    publicSignatureData: recipient1.publicSignatureData
                )
            ]
        ) { result in
            if case .failure(let err) = result {
                XCTFail(err.localizedDescription)
            }
            expectation1.fulfill()
        }
        
        wait(for: [expectation1], timeout: 5.0)
        
        let userStore = SHDBManager.sharedInstance.userStore!
        let kvs = try userStore.dictionaryRepresentation()
        XCTAssertEqual(kvs.count, 4)
        XCTAssertNotNil(kvs["\(SHInteractionAnchor.group.rawValue)::\(groupId)::ephemeralPublicKey"])
        XCTAssertNotNil(kvs["\(SHInteractionAnchor.group.rawValue)::\(groupId)::secretPublicSignature"])
        XCTAssertNotNil(kvs["\(SHInteractionAnchor.group.rawValue)::\(groupId)::senderPublicSignature"])
        XCTAssertNotNil(kvs["\(SHInteractionAnchor.group.rawValue)::\(groupId)::encryptedSecret"])

        let messageText = "This is my first message"
        
        let expectation2 = XCTestExpectation(description: "send a message in the group")
        controller.send(
            message: messageText,
            inGroup: groupId
        ) { result in
            if case .failure(let err) = result {
                XCTFail(err.localizedDescription)
            }
            expectation2.fulfill()
        }

        wait(for: [expectation2], timeout: 5.0)
        
        let expectation3 = XCTestExpectation(description: "retrieve group interactions")
        controller.retrieveInteractions(inGroup: groupId, per: 10, page: 1) {
            result in
            switch result {
            case .failure(let err):
                XCTFail(err.localizedDescription)
            case .success(let groupInteractions):
                XCTAssertEqual(groupInteractions.groupId, groupId)
                XCTAssertEqual(groupInteractions.messages.count, 1)
                
                guard let message = groupInteractions.messages.first else {
                    XCTFail()
                    return
                }
                
                XCTAssertNotNil(message.interactionId)
                XCTAssertEqual(message.sender.identifier, self.myUser.identifier)
                XCTAssertEqual(message.inReplyToAssetGlobalIdentifier, nil)
                XCTAssertEqual(message.inReplyToInteractionId, nil)
                XCTAssertEqual(message.message, messageText)
            }
            expectation3.fulfill()
        }

        wait(for: [expectation3], timeout: 5.0)

    }
    
    func testSendMessageE2EEInThread() throws {
        
        /// Cache `myUser` in the `ServerUserCache`
        
        ServerUserCache.shared.cache(
            users: [
                SHRemoteUser(identifier: myUser.identifier,
                             name: "myUser",
                             publicKeyData: myUser.publicKeyData,
                             publicSignatureData: myUser.publicSignatureData)
                ]
            )
        
        let threadId = "testThreadId1"
        
        /// Create the other user `recipient1` and cache it
        
        let recipient1 = SHLocalUser(keychainPrefix: "other")
        
        ServerUserCache.shared.cache(
            users: [
                SHRemoteUser(identifier: recipient1.identifier,
                             name: "recipient1",
                             publicKeyData: recipient1.publicKeyData,
                             publicSignatureData: recipient1.publicSignatureData)
                ]
            )
        
        XCTAssertEqual(ServerUserCache.shared.user(with: myUser.identifier)?.publicSignatureData,
                       myUser.publicSignatureData)
        XCTAssertEqual(ServerUserCache.shared.user(with: recipient1.identifier)?.publicSignatureData,
                       recipient1.publicSignatureData)
        
        /// Create the thread in the mock server for `myUser`, with no encryption details for now
        
        let serverThreadDetails = [
            MockThreadDetails(
                threadId: threadId,
                name: nil,
                userIds: [myUser.identifier, recipient1.identifier],
                encryptionDetails: []
            )
        ]
        let serverProxy = SHMockServerProxy(user: myUser, threads: serverThreadDetails)
        
        let authenticatedUser1 = SHAuthenticatedLocalUser(
            localUser: myUser,
            name: "myUser",
            encryptionProtocolSalt: kTestStaticProtocolSalt,
            authToken: ""
        )
        
        let controller1 = SHUserInteractionController(
            user: authenticatedUser1,
            serverProxy: serverProxy
        )
        
        /// Ask the mock server for `myUser` to create a new thread with `recipient1`
        /// This will set up the encryption details in the mock server for the thread for both users
        
        let expectation1 = XCTestExpectation(description: "initialize the thread")
        controller1.setupThread(
            with: [
                SHRemoteUser(
                    identifier: recipient1.identifier,
                    name: "recipient1",
                    publicKeyData: recipient1.publicKeyData,
                    publicSignatureData: recipient1.publicSignatureData
                )
            ]
        ) {
            result in
            if case .failure(let err) = result {
                XCTFail(err.localizedDescription)
            }
            expectation1.fulfill()
        }
        
        wait(for: [expectation1], timeout: 5.0)
        
        /// Ensure the encryption details for `myUser` are now present in the local server
        
        guard let mockServerThread = serverProxy.state.threads?.first(where: { $0.threadId == threadId }) else {
            XCTFail() ; return
        }
        
        let userStore = SHDBManager.sharedInstance.userStore!
        let kvs = try userStore.dictionaryRepresentation()
        XCTAssertEqual(kvs.count, 5)
        XCTAssertNotNil(kvs["\(SHInteractionAnchor.thread.rawValue)::\(threadId)::ephemeralPublicKey"])
        XCTAssertNotNil(kvs["\(SHInteractionAnchor.thread.rawValue)::\(threadId)::secretPublicSignature"])
        XCTAssertNotNil(kvs["\(SHInteractionAnchor.thread.rawValue)::\(threadId)::senderPublicSignature"])
        XCTAssertNotNil(kvs["\(SHInteractionAnchor.thread.rawValue)::\(threadId)::encryptedSecret"])
        XCTAssertNotNil(kvs["\(SHInteractionAnchor.thread.rawValue)::\(threadId)::lastUpdatedAt"])
        XCTAssertNil(kvs["\(SHInteractionAnchor.thread.rawValue)::\(threadId)::name"])
        
        guard let mockServerMyUserEncryptionDetails = mockServerThread.encryptionDetails.first(where: { $0.recipientUserIdentifier == myUser.identifier })
        else {
            XCTFail() ; return
        }
        
        /// Ensure they match the encryption details for `myUser`
        
        XCTAssertEqual(kvs["\(SHInteractionAnchor.thread.rawValue)::\(threadId)::ephemeralPublicKey"] as? String, mockServerMyUserEncryptionDetails.ephemeralPublicKey)
        XCTAssertEqual(kvs["\(SHInteractionAnchor.thread.rawValue)::\(threadId)::secretPublicSignature"] as? String, mockServerMyUserEncryptionDetails.secretPublicSignature)
        XCTAssertEqual(kvs["\(SHInteractionAnchor.thread.rawValue)::\(threadId)::senderPublicSignature"] as? String, mockServerMyUserEncryptionDetails.senderPublicSignature)
        XCTAssertEqual(kvs["\(SHInteractionAnchor.thread.rawValue)::\(threadId)::encryptedSecret"] as? String, mockServerMyUserEncryptionDetails.encryptedSecret)
        
        ///
        /// Send a message from `myUser` to `recipient1`
        ///
        
        let messageText = "This is my first message"
        
        let expectation2 = XCTestExpectation(description: "send a message in the thread")
        controller1.send(
            message: messageText,
            inThread: threadId
        ) { result in
            if case .failure(let err) = result {
                XCTFail(err.localizedDescription)
            }
            expectation2.fulfill()
        }

        wait(for: [expectation2], timeout: 5.0)
        
        ///
        /// Ensure that message can be read from `myUser`
        ///
        
        let expectation3 = XCTestExpectation(description: "retrieve thread interactions")
        controller1.retrieveInteractions(inThread: threadId, per: 10, page: 1) {
            result in
            switch result {
            case .failure(let err):
                XCTFail(err.localizedDescription)
            case .success(let threadInteractions):
                XCTAssertEqual(threadInteractions.threadId, threadId)
                XCTAssertEqual(threadInteractions.messages.count, 1)
                
                guard let message = threadInteractions.messages.first else {
                    XCTFail()
                    expectation3.fulfill()
                    return
                }
                
                XCTAssertNotNil(message.interactionId)
                XCTAssertEqual(message.sender.identifier, self.myUser.identifier)
                XCTAssertEqual(message.inReplyToAssetGlobalIdentifier, nil)
                XCTAssertEqual(message.inReplyToInteractionId, nil)
                XCTAssertEqual(message.message, messageText)
            }
            expectation3.fulfill()
        }

        wait(for: [expectation3], timeout: 5.0)
        
        ///
        /// Ensure that message can be read from `recipient1`
        /// Because in testing the 2 users share a localServer with a local database, we have to update the KVS with the encryption details for `recipient1` first.
        /// At this point in time, they are stored for `myUser`
        ///
        
        guard let mockServerRecipient1EncryptionDetails = mockServerThread.encryptionDetails.first(where: { $0.recipientUserIdentifier == recipient1.identifier })
        else {
            XCTFail() ; return
        }
        
        XCTAssertNotEqual(mockServerRecipient1EncryptionDetails.ephemeralPublicKey, mockServerMyUserEncryptionDetails.ephemeralPublicKey)
        XCTAssertNotEqual(mockServerRecipient1EncryptionDetails.encryptedSecret, mockServerMyUserEncryptionDetails.encryptedSecret)
        XCTAssertNotEqual(mockServerRecipient1EncryptionDetails.secretPublicSignature, mockServerMyUserEncryptionDetails.secretPublicSignature)
        
        /// 
        /// Ensure sender signature is stable (the signature of the sender all the encryption details for all users were created by)
        ///
        XCTAssertEqual(mockServerRecipient1EncryptionDetails.senderPublicSignature, mockServerMyUserEncryptionDetails.senderPublicSignature)
        
        let writeBatch = userStore.writeBatch()
        writeBatch.set(value: mockServerRecipient1EncryptionDetails.ephemeralPublicKey, for: "\(SHInteractionAnchor.thread.rawValue)::\(threadId)::ephemeralPublicKey")
        writeBatch.set(value: mockServerRecipient1EncryptionDetails.encryptedSecret, for: "\(SHInteractionAnchor.thread.rawValue)::\(threadId)::encryptedSecret")
        writeBatch.set(value: mockServerRecipient1EncryptionDetails.secretPublicSignature, for: "\(SHInteractionAnchor.thread.rawValue)::\(threadId)::secretPublicSignature")
        writeBatch.set(value: mockServerRecipient1EncryptionDetails.senderPublicSignature, for: "\(SHInteractionAnchor.thread.rawValue)::\(threadId)::senderPublicSignature")
        try writeBatch.write()
        
        let expectation4 = XCTestExpectation(description: "retrieve thread interactions on the other side")

        let serverProxy2 = SHMockServerProxy(user: recipient1, threads: [mockServerThread])
        
        let authenticatedUser2 = SHAuthenticatedLocalUser(
            localUser: recipient1,
            name: "recipient1",
            encryptionProtocolSalt: kTestStaticProtocolSalt,
            authToken: ""
        )
        
        let controller2 = SHUserInteractionController(
            user: authenticatedUser2,
            serverProxy: serverProxy2
        )
        controller2.retrieveInteractions(inThread: threadId, per: 10, page: 1) {
            result in
            switch result {
            case .failure(let err):
                XCTFail(err.localizedDescription)
            case .success(let threadInteractions):
                XCTAssertEqual(threadInteractions.threadId, threadId)
                XCTAssertEqual(threadInteractions.messages.count, 1)
                
                guard let message = threadInteractions.messages.first else {
                    XCTFail()
                    expectation4.fulfill()
                    return
                }
                
                XCTAssertNotNil(message.interactionId)
                XCTAssertEqual(message.sender.identifier, self.myUser.identifier)
                XCTAssertEqual(message.inReplyToAssetGlobalIdentifier, nil)
                XCTAssertEqual(message.inReplyToInteractionId, nil)
                XCTAssertEqual(message.message, messageText)
            }
            expectation4.fulfill()
        }
        
        wait(for: [expectation4], timeout: 5.0)
        
        ///
        /// Now, send a message response from `recipient1` back to `myUser`
        ///
        
        let messageReplyText = "This is the reply to your first message"
        
        let expectation5 = XCTestExpectation(description: "send a reply in the thread")
        controller2.send(
            message: messageReplyText,
            inThread: threadId
        ) { result in
            if case .failure(let err) = result {
                XCTFail(err.localizedDescription)
            }
            expectation5.fulfill()
        }

        wait(for: [expectation5], timeout: 5.0)
        
        let expectation6 = XCTestExpectation(description: "retrieve interactions in thread from recipient1")
        
        controller2.retrieveInteractions(inThread: threadId, per: 10, page: 1) {
            result in
            switch result {
            case .failure(let err):
                XCTFail(err.localizedDescription)
            case .success(let threadInteractions):
                XCTAssertEqual(threadInteractions.threadId, threadId)
                XCTAssertEqual(threadInteractions.messages.count, 2)
                
                guard let lastMessage = threadInteractions.messages.last else {
                    XCTFail()
                    expectation6.fulfill()
                    return
                }
                
                XCTAssertNotNil(lastMessage.interactionId)
                XCTAssertEqual(lastMessage.sender.identifier, self.myUser.identifier)
                XCTAssertEqual(lastMessage.inReplyToAssetGlobalIdentifier, nil)
                XCTAssertEqual(lastMessage.inReplyToInteractionId, nil)
                XCTAssertEqual(lastMessage.message, messageText)
                
                guard let firstMessage = threadInteractions.messages.first else {
                    XCTFail()
                    expectation6.fulfill()
                    return
                }
                
                XCTAssertNotNil(firstMessage.interactionId)
                XCTAssertEqual(firstMessage.sender.identifier, recipient1.identifier)
                XCTAssertEqual(firstMessage.inReplyToAssetGlobalIdentifier, nil)
                XCTAssertEqual(firstMessage.inReplyToInteractionId, nil)
                XCTAssertEqual(firstMessage.message, messageReplyText)
            }
            expectation6.fulfill()
        }
        
        wait(for: [expectation6], timeout: 5.0)
        
        let expectation7 = XCTestExpectation(description: "retrieve interactions with limit")
        let expectation8 = XCTestExpectation(description: "retrieve interactions with offset")
        let expectation9 = XCTestExpectation(description: "retrieve interactions with limit and offset")
        
        controller2.retrieveInteractions(inThread: threadId, per: 1, page: 1) {
            result in
            switch result {
            case .failure(let err):
                XCTFail(err.localizedDescription)
            case .success(let threadInteractions):
                XCTAssertEqual(threadInteractions.threadId, threadId)
                XCTAssertEqual(threadInteractions.messages.count, 1)
                
                guard let firstMessage = threadInteractions.messages.first else {
                    XCTFail()
                    expectation7.fulfill()
                    return
                }
                
                XCTAssertNotNil(firstMessage.interactionId)
                XCTAssertEqual(firstMessage.sender.identifier, recipient1.identifier)
                XCTAssertEqual(firstMessage.inReplyToAssetGlobalIdentifier, nil)
                XCTAssertEqual(firstMessage.inReplyToInteractionId, nil)
                XCTAssertEqual(firstMessage.message, messageReplyText)
            }
            expectation7.fulfill()
        }
        
        controller2.retrieveInteractions(inThread: threadId, per: 1, page: 2) {
            result in
            switch result {
            case .failure(let err):
                XCTFail(err.localizedDescription)
            case .success(let threadInteractions):
                XCTAssertEqual(threadInteractions.threadId, threadId)
                XCTAssertEqual(threadInteractions.messages.count, 1)
                
                guard let lastMessage = threadInteractions.messages.last else {
                    XCTFail()
                    expectation8.fulfill()
                    return
                }
                
                XCTAssertNotNil(lastMessage.interactionId)
                XCTAssertEqual(lastMessage.sender.identifier, self.myUser.identifier)
                XCTAssertEqual(lastMessage.inReplyToAssetGlobalIdentifier, nil)
                XCTAssertEqual(lastMessage.inReplyToInteractionId, nil)
                XCTAssertEqual(lastMessage.message, messageText)
            }
            expectation8.fulfill()
        }
        
        controller2.retrieveInteractions(inThread: threadId, per: 2, page: 2) {
            result in
            switch result {
            case .failure(let err):
                XCTFail(err.localizedDescription)
            case .success(let threadInteractions):
                XCTAssertEqual(threadInteractions.threadId, threadId)
                XCTAssertEqual(threadInteractions.messages.count, 0)
            }
            expectation9.fulfill()
        }
        
        wait(for: [expectation7, expectation8, expectation9], timeout: 5.0)
//        
        let expectation10 = XCTestExpectation(description: "retrieve interactions from myUser")
        
        let writeBatchMyUser = userStore.writeBatch()
        writeBatchMyUser.set(value: mockServerMyUserEncryptionDetails.ephemeralPublicKey, for: "\(SHInteractionAnchor.thread.rawValue)::\(threadId)::ephemeralPublicKey")
        writeBatchMyUser.set(value: mockServerMyUserEncryptionDetails.encryptedSecret, for: "\(SHInteractionAnchor.thread.rawValue)::\(threadId)::encryptedSecret")
        writeBatchMyUser.set(value: mockServerMyUserEncryptionDetails.secretPublicSignature, for: "\(SHInteractionAnchor.thread.rawValue)::\(threadId)::secretPublicSignature")
        writeBatchMyUser.set(value: mockServerMyUserEncryptionDetails.senderPublicSignature, for: "\(SHInteractionAnchor.thread.rawValue)::\(threadId)::senderPublicSignature")
        try writeBatchMyUser.write()
        
        controller1.retrieveInteractions(inThread: threadId, per: 3, page: 1) {
            result in
            switch result {
            case .failure(let err):
                XCTFail(err.localizedDescription)
            case .success(let threadInteractions):
                XCTAssertEqual(threadInteractions.threadId, threadId)
                XCTAssertEqual(threadInteractions.messages.count, 2)
                
                guard let firstMessage = threadInteractions.messages.first else {
                    XCTFail()
                    expectation10.fulfill()
                    return
                }
                
                XCTAssertNotNil(firstMessage.interactionId)
                XCTAssertEqual(firstMessage.sender.identifier, recipient1.identifier)
                XCTAssertEqual(firstMessage.inReplyToAssetGlobalIdentifier, nil)
                XCTAssertEqual(firstMessage.inReplyToInteractionId, nil)
                XCTAssertEqual(firstMessage.message, messageReplyText)
                
                guard let lastMessage = threadInteractions.messages.last else {
                    XCTFail()
                    expectation10.fulfill()
                    return
                }
                
                XCTAssertNotNil(lastMessage.interactionId)
                XCTAssertEqual(lastMessage.sender.identifier, self.myUser.identifier)
                XCTAssertEqual(lastMessage.inReplyToAssetGlobalIdentifier, nil)
                XCTAssertEqual(lastMessage.inReplyToInteractionId, nil)
                XCTAssertEqual(lastMessage.message, messageText)
            }
            expectation10.fulfill()
        }
        
        wait(for: [expectation10], timeout: 5.0)
    }
}
