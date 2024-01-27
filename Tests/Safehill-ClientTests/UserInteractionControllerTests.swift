import XCTest
@testable import Safehill_Client
import Safehill_Crypto

struct MockThreadDetails {
    let threadId: String
    let name: String?
    let userIds: [String]
    let selfEncryptionDetails: RecipientEncryptionDetailsDTO?
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
    
    init(user: SHLocalUser) {
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
            interactionId: "interactionId",
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
            interactionId: "interactionId",
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
    
    func createOrUpdateThread(name: String?, recipientsEncryptionDetails: [RecipientEncryptionDetailsDTO]?, completionHandler: @escaping (Result<ConversationThreadOutputDTO, Error>) -> ()) {
        if let recipientsEncryptionDetails {
            let threadMembersId = recipientsEncryptionDetails.map({ $0.userIdentifier })
            guard let threads = self.state.threads,
               let matchingThreadIdx = threads.firstIndex(where: { $0.userIds.count == threadMembersId.count && Set(threadMembersId).subtracting($0.userIds).isEmpty })
            else {
                completionHandler(.failure(SHHTTPError.ClientError.badRequest("thread encryption details for were not set up correctly on the mock server")))
                return
            }
            
            let matchingThread = threads[matchingThreadIdx]
            
            if let providedSelfEncryptionDetails = recipientsEncryptionDetails.first(where: { $0.userIdentifier == self.localServer.requestor.identifier }) {
                self.state.threads![matchingThreadIdx] = MockThreadDetails(
                    threadId: matchingThread.threadId,
                    name: matchingThread.name,
                    userIds: matchingThread.userIds,
                    selfEncryptionDetails: providedSelfEncryptionDetails
                )
            }
            
            let serverThread = ConversationThreadOutputDTO(
                threadId: matchingThread.threadId,
                name: name,
                membersPublicIdentifier: recipientsEncryptionDetails.map({ $0.userIdentifier }),
                lastUpdatedAt: Date(),
                encryptionDetails: self.state.threads![matchingThreadIdx].selfEncryptionDetails!
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
        
        guard let selfEncryptionDetails = matchingThread.selfEncryptionDetails else {
            completionHandler(.success(nil))
            return
        }
        
        let serverThread = ConversationThreadOutputDTO(
            threadId: matchingThread.threadId,
            name: matchingThread.name,
            membersPublicIdentifier: matchingThread.userIds,
            lastUpdatedAt: Date(),
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
        let _ = try SHDBManager.sharedInstance.userStore().removeAll()
        let _ = try SHDBManager.sharedInstance.assetStore().removeAll()
        let _ = try SHDBManager.sharedInstance.reactionStore().removeAll()
        let _ = try SHDBManager.sharedInstance.messageQueue().removeAll()
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
        
        let controller = SHUserInteractionController(
            user: myUser,
            protocolSalt: kTestStaticProtocolSalt,
            serverProxy: serverProxy
        )
        
        var expectation = XCTestExpectation(description: "initialize the group")
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
            expectation.fulfill()
        }
        
        wait(for: [expectation], timeout: 5.0)
        
        let userStore = try SHDBManager.sharedInstance.userStore()
        let kvs = try userStore.dictionaryRepresentation()
        XCTAssertEqual(kvs.count, 3)
        XCTAssertNotNil(kvs["\(InteractionAnchor.group.rawValue)::\(groupId)::ephemeralPublicKey"])
        XCTAssertNotNil(kvs["\(InteractionAnchor.group.rawValue)::\(groupId)::secretPublicSignature"])
        XCTAssertNotNil(kvs["\(InteractionAnchor.group.rawValue)::\(groupId)::encryptedSecret"])

        let messageText = "This is my first message"
        
        expectation = XCTestExpectation(description: "send a message in the group")
        controller.send(
            message: messageText,
            inGroup: groupId
        ) { result in
            if case .failure(let err) = result {
                XCTFail(err.localizedDescription)
            }
            expectation.fulfill()
        }

        wait(for: [expectation], timeout: 5.0)
        
        expectation = XCTestExpectation(description: "retrieve group interactions")
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
            expectation.fulfill()
        }

        wait(for: [expectation], timeout: 5.0)

    }
    
    func testSendMessageE2EEInThread() throws {
        ServerUserCache.shared.cache(
            users: [
                SHRemoteUser(identifier: myUser.identifier,
                             name: "myUser",
                             publicKeyData: myUser.publicKeyData,
                             publicSignatureData: myUser.publicSignatureData)
                ]
            )
        
        let threadId = "testThreadId1"
        let recipient1 = SHLocalCryptoUser()
        
        ServerUserCache.shared.cache(
            users: [
                SHRemoteUser(identifier: recipient1.identifier,
                             name: "recipient1",
                             publicKeyData: recipient1.publicKeyData,
                             publicSignatureData: recipient1.publicSignatureData)
                ]
            )
        
        let serverThreadDetails = [
            MockThreadDetails(
                threadId: threadId,
                name: nil,
                userIds: [myUser.identifier, recipient1.identifier],
                selfEncryptionDetails: nil
            )
        ]
        let serverProxy = SHMockServerProxy(user: myUser, threads: serverThreadDetails)
        
        let controller = SHUserInteractionController(
            user: myUser,
            protocolSalt: kTestStaticProtocolSalt,
            serverProxy: serverProxy
        )
        
        var expectation = XCTestExpectation(description: "initialize the thread")
        controller.setupThread(
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
            expectation.fulfill()
        }
        
        wait(for: [expectation], timeout: 5.0)
        
        let userStore = try SHDBManager.sharedInstance.userStore()
        let kvs = try userStore.dictionaryRepresentation()
        XCTAssertEqual(kvs.count, 4)
        XCTAssertNotNil(kvs["\(InteractionAnchor.thread.rawValue)::\(threadId)::ephemeralPublicKey"])
        XCTAssertNotNil(kvs["\(InteractionAnchor.thread.rawValue)::\(threadId)::secretPublicSignature"])
        XCTAssertNotNil(kvs["\(InteractionAnchor.thread.rawValue)::\(threadId)::encryptedSecret"])
        XCTAssertNotNil(kvs["\(InteractionAnchor.thread.rawValue)::\(threadId)::lastUpdatedAt"])
        XCTAssertNil(kvs["\(InteractionAnchor.thread.rawValue)::\(threadId)::name"])

        let messageText = "This is my first message"
        
        expectation = XCTestExpectation(description: "send a message in the thread")
        controller.send(
            message: messageText,
            inThread: threadId
        ) { result in
            if case .failure(let err) = result {
                XCTFail(err.localizedDescription)
            }
            expectation.fulfill()
        }

        wait(for: [expectation], timeout: 5.0)
        
        expectation = XCTestExpectation(description: "retrieve thread interactions")
        controller.retrieveInteractions(inThread: threadId, per: 10, page: 1) {
            result in
            switch result {
            case .failure(let err):
                XCTFail(err.localizedDescription)
            case .success(let threadInteractions):
                XCTAssertEqual(threadInteractions.threadId, threadId)
                XCTAssertEqual(threadInteractions.messages.count, 1)
                
                guard let message = threadInteractions.messages.first else {
                    XCTFail()
                    return
                }
                
                XCTAssertNotNil(message.interactionId)
                XCTAssertEqual(message.sender.identifier, self.myUser.identifier)
                XCTAssertEqual(message.inReplyToAssetGlobalIdentifier, nil)
                XCTAssertEqual(message.inReplyToInteractionId, nil)
                XCTAssertEqual(message.message, messageText)
            }
            expectation.fulfill()
        }

        wait(for: [expectation], timeout: 5.0)

    }
}
