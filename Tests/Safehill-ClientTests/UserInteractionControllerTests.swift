import XCTest
@testable import Safehill_Client
import Safehill_Crypto

struct SHMockServerProxy: SHServerProxyProtocol {

    let localServer: LocalServer
    
    init(user: SHLocalUser) {
        self.localServer = LocalServer(requestor: user)
    }
    
    func createGroup(groupId: String, recipientsEncryptionDetails: [Safehill_Client.RecipientEncryptionDetailsDTO], completionHandler: @escaping (Result<Void, Error>) -> ()) {
        self.localServer.createGroup(
            groupId: groupId,
            recipientsEncryptionDetails: recipientsEncryptionDetails,
            completionHandler: completionHandler
        )
    }
    
    func addToGroup(groupId: String, recipientsEncryptionDetails: [Safehill_Client.RecipientEncryptionDetailsDTO], completionHandler: @escaping (Result<Void, Error>) -> ()) {
        self.localServer.addToGroup(
            groupId: groupId,
            recipientsEncryptionDetails: recipientsEncryptionDetails,
            completionHandler: completionHandler
        )
    }
    
    func addReactions(_ reactions: [Safehill_Client.ReactionInput], toGroupId groupId: String, completionHandler: @escaping (Result<[ReactionOutputDTO], Error>) -> ()) {
        self.localServer.addReactions(reactions, toGroupId: groupId, completionHandler: completionHandler)
    }
    
    func removeReaction(withIdentifier interactionId: String, fromGroupId groupId: String, completionHandler: @escaping (Result<Void, Error>) -> ()) {
        self.localServer.removeReaction(withIdentifier: interactionId, fromGroupId: groupId, completionHandler: completionHandler)
    }
    
    func addMessage(_ message: Safehill_Client.MessageInputDTO, toGroupId groupId: String, completionHandler: @escaping (Result<Safehill_Client.MessageOutputDTO, Error>) -> ()) {
        let messageOutput = MessageOutputDTO(
            interactionId: "interactionId",
            senderUserIdentifier: self.localServer.requestor.identifier,
            inReplyToAssetGlobalIdentifier: message.inReplyToAssetGlobalIdentifier,
            inReplyToInteractionId: message.inReplyToInteractionId,
            encryptedMessage: message.encryptedMessage,
            createdAt: Date().iso8601withFractionalSeconds
        )
        self.localServer.addMessages([messageOutput], toGroupId: groupId) { result in
            switch result {
            case .success(let messages):
                completionHandler(.success(messages.first!))
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    func retrieveInteractions(inGroup groupId: String, per: Int, page: Int, completionHandler: @escaping (Result<Safehill_Client.InteractionsGroupDTO, Error>) -> ()) {
        self.localServer.retrieveInteractions(inGroup: groupId, per: per, page: page, completionHandler: completionHandler)
    }
    
    func retrieveGroupUserEncryptionDetails(forGroup groupId: String, completionHandler: @escaping (Result<Safehill_Client.RecipientEncryptionDetailsDTO, Error>) -> ()) {
        self.localServer.retrieveGroupUserEncryptionDetails(forGroup: groupId, completionHandler: completionHandler)
    }
    
    func countLocalInteractions(inGroup groupId: String, completionHandler: @escaping (Result<(reactions: [ReactionType : Int], messages: Int), Error>) -> ()) {
        self.localServer.countInteractions(inGroup: groupId, completionHandler: completionHandler)
    }
}


final class Safehill_UserInteractionControllerTests: XCTestCase {
    
    let myUser = SHLocalUser(cryptoUser: SHLocalCryptoUser())
    
    override func setUpWithError() throws {
        let _ = try SHDBManager.sharedInstance.assetStore().removeAll()
        let _ = try SHDBManager.sharedInstance.reactionStore().removeAll()
        let _ = try SHDBManager.sharedInstance.messageQueue().removeAll()
    }
    
    func testSendMessageE2EE() throws {
        ServerUserCache.shared.cache(
            SHRemoteUser(identifier: myUser.identifier,
                         name: "myUser",
                         publicKeyData: myUser.publicKeyData,
                         publicSignatureData: myUser.publicSignatureData)
        )
        
        let groupId = "testGroupId"
        let recipient1 = SHLocalCryptoUser()
        let recipient2 = SHLocalCryptoUser()
        
        ServerUserCache.shared.cache(
            SHRemoteUser(identifier: recipient1.identifier,
                         name: "recipient1",
                         publicKeyData: recipient1.publicKeyData,
                         publicSignatureData: recipient1.publicSignatureData)
        )
        
        let serverProxy = SHMockServerProxy(user: myUser)
        
        let controller = SHUserInteractionController(
            user: myUser,
            protocolSalt: kTestStaticProtocolSalt,
            serverProxy: serverProxy
        )
        
        var expectation = XCTestExpectation(description: "initialize the group")
        controller.initializeGroup(
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
        
        let assetStore = try SHDBManager.sharedInstance.assetStore()
        let kvs = try assetStore.dictionaryRepresentation()
        XCTAssert(kvs.count == 3)
        XCTAssertNotNil(kvs["\(groupId)::ephemeralPublicKey"])
        XCTAssertNotNil(kvs["\(groupId)::secretPublicSignature"])
        XCTAssertNotNil(kvs["\(groupId)::encryptedSecret"])

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
}
