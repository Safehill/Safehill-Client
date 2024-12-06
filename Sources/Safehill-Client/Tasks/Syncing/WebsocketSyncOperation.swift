import Foundation
import os

public class SHWebsocketSyncOperation: Operation, @unchecked Sendable {
    
    public typealias OperationResult = Result<Void, Error>
    
    private static var isWebSocketConnected = false
    private static let memberAccessQueue = DispatchQueue(label: "WebsocketSyncOperation.memberAccessQueue")
    
    public let log = Logger(subsystem: "com.safehill", category: "WS-SYNC")
    
    let delegatesQueue = DispatchQueue(label: "com.safehill.ws.delegates")
    
    let user: SHAuthenticatedLocalUser
    let deviceId: String
    
    let socket: WebSocketAPI
    
    private var websocketConnectionDelegates: [WebSocketDelegate]
    internal let interactionsSyncDelegates: [SHInteractionsSyncingDelegate]
    private let userConnectionsDelegates: [SHUserConnectionRequestDelegate]
    internal let userConversionDelegates: [SHUserConversionDelegate]
    
    private var retryDelay: UInt64 = 1
    private let maxRetryDelay: UInt64 = 8
    
    public init(
        user: SHAuthenticatedLocalUser,
        deviceId: String,
        websocketConnectionDelegates: [WebSocketDelegate],
        interactionsSyncDelegates: [SHInteractionsSyncingDelegate],
        userConnectionsDelegates: [SHUserConnectionRequestDelegate],
        userConversionDelegates: [SHUserConversionDelegate]
    ) throws {
        self.user = user
        self.deviceId = deviceId
        self.websocketConnectionDelegates = websocketConnectionDelegates
        self.interactionsSyncDelegates = interactionsSyncDelegates
        self.userConnectionsDelegates = userConnectionsDelegates
        self.userConversionDelegates = userConversionDelegates
        self.socket = WebSocketAPI()
        
        super.init()
        
        self.websocketConnectionDelegates.append(self)
    }
    
    deinit {
        self.websocketConnectionDelegates.forEach {
            $0.didDisconnect(error: nil)
        }
    }
    
    var serverProxy: SHServerProxy { self.user.serverProxy }
    
    private func startWebSocket() async throws {
        var isAlreadyConnected = false
        Self.memberAccessQueue.sync {
            isAlreadyConnected = Self.isWebSocketConnected
        }
        
        guard isAlreadyConnected == false else {
            return
        }
        
        try await socket.connect(to: "ws/messages",
                                 as: self.user,
                                 from: self.deviceId,
                                 keepAliveIntervalInSeconds: 5.0)
        
        do {
            for try await message in await socket.receive() {
                
                self.processMessage(message)
                
                if self.isCancelled {
                    await self.stopWebSocket(error: nil)
                    break
                }
            }
        } catch {
            log.error("[ws] websocket failure: \(error)")
            throw error
        }
    }
    
    private func startWebSocketAndReconnectOnFailure() async throws {
        do {
            try await self.startWebSocket()
        } catch let error {
            if let error = error as? WebSocketConnectionError {
                switch error {
                case .disconnected, .connectionError, .transportError:
                    log.info("[ws] websocket connection error: \(error.localizedDescription)")
                    
                    /// Disconnect if not already disconnected (this sets the `socket.webSocketTask` to `nil`
                    await self.stopWebSocket(error: error)
                    
                    /// Exponential retry with backoff
                    try await Task.sleep(nanoseconds: self.retryDelay * 1_000_000_000)
                    self.retryDelay = max(self.maxRetryDelay, self.retryDelay * 2)
                    try await self.startWebSocketAndReconnectOnFailure()
                default:
                    break
                }
            }
            log.error("[ws] failed to connect to websocket: \(error.localizedDescription)")
        }
    }
    
    public func stopWebSocket(error: Error?) async {
        await self.socket.disconnect()
        self.log.debug("[ws] DISCONNECTED")
        
        websocketConnectionDelegates.forEach {
            $0.didDisconnect(error: error)
        }
        
        Self.memberAccessQueue.sync {
            Self.isWebSocketConnected = false
        }
    }
    
    private func processMessage(_ message: WebSocketMessage) {
        guard let contentData = message.content.data(using: .utf8) else {
            log.critical("[ws] unable to parse message content")
            return
        }
        
        let interactionsSyncDelegates = self.interactionsSyncDelegates
        let userConnectionsDelegates = self.userConnectionsDelegates
        
        self.delegatesQueue.async {
            
            switch message.type {
                
            case .connectionAck:
                guard let encoded = try? JSONDecoder().decode(WebSocketMessage.ConnectionAck.self, from: contentData) else {
                    return
                }
                self.log.debug("[ws] CONNECTED: userPublicId=\(encoded.userPublicIdentifier), deviceId=\(encoded.deviceId)")
                
                self.websocketConnectionDelegates.forEach {
                    $0.didConnect()
                }
                
                Self.memberAccessQueue.sync {
                    Self.isWebSocketConnected = true
                }
                
            case .userConversionManifest:
                guard let encoded = try? JSONDecoder().decode(WebSocketMessage.UserConversionManifest.self, from: contentData) else {
                    return
                }
                self.log.debug("[ws] CONVERSION-REQUEST: newUser=\(encoded.newUser.name) assetIdsByGroupId=\(encoded.assetIdsByGroupId), threadIds=\(encoded.threadIds)")
                
                Task {
                    do {
                        try await SHAssetSharingController(localUser: self.user).convertUser(
                            encoded.newUser,
                            assetIdsByGroupId: encoded.assetIdsByGroupId,
                            threadIds: encoded.threadIds
                        )
                    } catch {
                        self.log.error("[ws] CONVERSION-REQUEST: failed to convert newUser=\(encoded.newUser.name) assetIdsByGroupId=\(encoded.assetIdsByGroupId), threadIds=\(encoded.threadIds). \(error.localizedDescription)")
                    }
                }
                
                self.userConversionDelegates.forEach({
                    $0.didRequestUserConversion(
                        assetIdsByGroupId: encoded.assetIdsByGroupId,
                        threadIds: encoded.threadIds
                    )
                })
                
            case .threadUserConverted:
                guard let threadIds = try? JSONDecoder().decode([String].self, from: contentData) else {
                    return
                }
                self.log.debug("[ws] THREADS-USER-CONVERTED: threadIds=\(threadIds)")
                
                self.userConversionDelegates.forEach({
                    $0.didConvertUserInThreads(with: threadIds)
                })
                
            case .connectionRequest:
                guard let encoded = try? JSONDecoder().decode(WebSocketMessage.NewUserConnection.self, from: contentData) else {
                    return
                }
                
                let requestor = encoded.requestor
                
                self.log.debug("[ws] USERCONNECTION: request from \(requestor.name)")
                
                let serverUser = SHRemoteUser(
                    identifier: requestor.identifier,
                    name: requestor.name,
                    phoneNumber: requestor.phoneNumber,
                    publicKeyData: requestor.publicKeyData,
                    publicSignatureData: requestor.publicSignatureData
                )
                
                userConnectionsDelegates.forEach({
                    $0.didReceiveAuthorizationRequest(from: serverUser)
                })
                
            case .message:
                
                guard let textMessage = try? JSONDecoder().decode(WebSocketMessage.TextMessage.self, from: contentData),
                      let interactionId = textMessage.interactionId else {
                    self.log.critical("[ws] server sent a \(message.type.rawValue) message via WebSockets but the client can't validate it. This is not supposed to happen. \(message.content)")
                    return
                }
                
                self.log.debug("[ws] NEWMESSAGE: interaction id \(interactionId)")
                
                let messageOutput = MessageOutputDTO(
                    interactionId: interactionId,
                    senderPublicIdentifier: textMessage.senderPublicIdentifier,
                    inReplyToAssetGlobalIdentifier: textMessage.inReplyToAssetGlobalIdentifier,
                    inReplyToInteractionId: textMessage.inReplyToInteractionId,
                    encryptedMessage: textMessage.encryptedMessage,
                    createdAt: textMessage.sentAt
                )
                
                interactionsSyncDelegates.forEach({
                    switch SHInteractionAnchor(rawValue: textMessage.anchorType) {
                    case .group:
                        $0.didReceiveTextMessages([messageOutput], inGroup: textMessage.anchorId)
                    case .thread:
                        $0.didReceiveTextMessages([messageOutput], inThread: textMessage.anchorId)
                    case .none:
                        self.log.critical("[ws] invalid anchor type from server: \(textMessage.anchorType)")
                    }
                })
                
            case .reactionAdd:
                
                guard let reaction = try? JSONDecoder().decode(WebSocketMessage.Reaction.self, from: contentData),
                      let interactionId = reaction.interactionId,
                      let reactionType = ReactionType(rawValue: reaction.reactionType)
                else {
                    self.log.critical("[ws] server sent a \(message.type.rawValue) message via WebSockets that can't be parsed or without an ID, or invalid reaction type. This is not supposed to happen. \(message.content)")
                    return
                }
                
                self.log.debug("[ws] NEWREACTION: interaction id \(interactionId)")
                
                let reactionOutput = ReactionOutputDTO(
                    interactionId: interactionId,
                    senderPublicIdentifier: reaction.senderPublicIdentifier,
                    inReplyToAssetGlobalIdentifier: reaction.inReplyToAssetGlobalIdentifier,
                    inReplyToInteractionId: reaction.inReplyToInteractionId,
                    reactionType: reactionType,
                    addedAt: reaction.updatedAt
                )
                
                interactionsSyncDelegates.forEach({
                    switch SHInteractionAnchor(rawValue: reaction.anchorType) {
                    case .group:
                        $0.didAddReaction(reactionOutput, toGroup: reaction.anchorId)
                    case .thread:
                        $0.didAddReaction(reactionOutput, toThread: reaction.anchorId)
                    case .none:
                        self.log.critical("[ws] invalid anchor type from server: \(reaction.anchorType)")
                    }
                })
                
            case .reactionRemove:
                
                guard let reaction = try? JSONDecoder().decode(WebSocketMessage.Reaction.self, from: contentData),
                      let reactionType = ReactionType(rawValue: reaction.reactionType)
                else {
                    self.log.critical("[ws] server sent a \(message.type.rawValue) message via WebSockets that can't be parsed. This is not supposed to happen. \(message.content)")
                    return
                }
                
                self.log.debug("[ws] REMOVEREACTION")
                
                interactionsSyncDelegates.forEach({
                    switch SHInteractionAnchor(rawValue: reaction.anchorType) {
                    case .group:
                        $0.didRemoveReaction(
                            reactionType,
                            addedBy: reaction.senderPublicIdentifier,
                            inReplyToAssetGlobalIdentifier: reaction.inReplyToAssetGlobalIdentifier,
                            inReplyToInteractionId: reaction.inReplyToInteractionId,
                            fromGroup: reaction.anchorId
                        )
                    case .thread:
                        $0.didRemoveReaction(
                            reactionType,
                            addedBy: reaction.senderPublicIdentifier,
                            inReplyToAssetGlobalIdentifier: reaction.inReplyToAssetGlobalIdentifier,
                            inReplyToInteractionId: reaction.inReplyToInteractionId,
                            fromThread: reaction.anchorId
                        )
                    case .none:
                        self.log.critical("[ws] invalid anchor type from server: \(reaction.anchorType)")
                    }
                })
                
            case .threadAdd:
                
                guard let threadOutputDTO = try? JSONDecoder().decode(ConversationThreadOutputDTO.self, from: contentData) else {
                    self.log.critical("[ws] server sent a \(message.type.rawValue) message via WebSockets that can't be parsed. This is not supposed to happen. \(message.content)")
                    return
                }
                
                self.log.debug("[ws] NEWTHREAD: thread id \(threadOutputDTO.threadId)")
                
                Task {
                    let _ = await self.serverProxy.createThreadsLocally(
                        [threadOutputDTO]
                    )
                }
                
                interactionsSyncDelegates.forEach {
                    $0.didAddThread(threadOutputDTO)
                }
                
            case .threadRemove:
                
                guard let threadId = try? JSONDecoder().decode(String.self, from: contentData) else {
                    self.log.critical("[ws] server sent a \(message.type.rawValue) message via WebSockets that can't be parsed. This is not supposed to happen. \(message.content)")
                    return
                }
                
                self.log.debug("[ws] REMOVE-THREAD: thread id \(threadId)")
                
                self.serverProxy.deleteLocalThread(withId: threadId) { res in
                    if case .failure(let failure) = res {
                        self.log.error("failed to remove thread from local server. Thread sync will attempt this again. \(failure.localizedDescription)")
                    }
                }
                
                interactionsSyncDelegates.forEach {
                    $0.didRemoveThread(with: threadId)
                }
                
            case .threadUpdate:
                
                guard let threadUpdateWsMessage = try? JSONDecoder().decode(WebSocketMessage.ThreadUpdate.self, from: contentData) else {
                    self.log.critical("[ws] server sent a \(message.type.rawValue) message via WebSockets that can't be parsed. This is not supposed to happen. \(message.content)")
                    return
                }
                
                Task {
                    do {
                        try await self.serverProxy.updateLocalThread(from: threadUpdateWsMessage)
                        
                        self.serverProxy.getThread(withId: threadUpdateWsMessage.threadId) { result in
                            if case .success(let threadOutputDTO) = result, let threadOutputDTO {
                                interactionsSyncDelegates.forEach {
                                    $0.didAddThread(threadOutputDTO)
                                }
                            } else {
                                self.log.critical("[ws] error retrieving thread from DB after \(message.type.rawValue) message via WebSockets")
                            }
                        }
                    } catch {
                        self.log.critical("[ws] error updating DB after \(message.type.rawValue) message via WebSockets. \(error.localizedDescription)")
                    }
                }
                
            case .threadAssetsShare, .groupAssetsShare:
                
                if let threadAssetsWsMessage = try? JSONDecoder().decode(WebSocketMessage.ThreadAssets.self, from: contentData) {
                    
                    self.log.debug("[ws] ASSETSHARE \(message.type.rawValue): thread id \(threadAssetsWsMessage.threadId)")
                    
                    let threadId = threadAssetsWsMessage.threadId
                    let threadAssets = threadAssetsWsMessage.assets
                    
                    interactionsSyncDelegates.forEach({
                        if message.type == .threadAssetsShare {
                            $0.didReceivePhotoMessages(threadAssets, in: threadId)
                        } else {
                            $0.didReceivePhotos(threadAssets)
                        }
                    })
                    
                    return
                    
                } else {
                    ///
                    /// BACKWARD COMPATIBILITY:
                    /// group-assets-share type messages were sent with `ThreadAssets` as content
                    /// but since late August 2024 it's been sent as a `[ConversationThreadAssetDTO]`
                    /// hence the fallthrough
                    ///
                    if message.type != .groupAssetsShare {
                        self.log.critical("[ws] server sent a \(message.type.rawValue) message via WebSockets that can't be parsed. This is not supposed to happen. \(message.content)")
                    }
                    fallthrough
                }
                
            case .groupAssetsShare:
                
                guard let groupAssets = try? JSONDecoder().decode([ConversationThreadAssetDTO].self, from: contentData) else {
                    self.log.critical("[ws] server sent a \(message.type.rawValue) message via WebSockets that can't be parsed. This is not supposed to happen. \(message.content)")
                    return
                }
                
                self.log.debug("[ws] ASSETSHARE \(message.type.rawValue): assets in group \(groupAssets.map({ ($0.globalIdentifier, $0.groupId) }))")
                
                interactionsSyncDelegates.forEach({
                    $0.didReceivePhotos(groupAssets)
                })
            }
        }
    }
    
    /// Main run.
    /// 1. Sync the threads list between local and remote
    /// 2. Pull the summary for threads and groups
    /// 3. Start the WEBSOCKET connection for updates
    public override func start() {
        super.start()
        
        Task(priority: .high) {
            do {
                log.debug("[SHInteractionsSyncOperation] starting websocket")
                
                ///
                /// Start syncing interactions via the web socket
                ///
                try await self.startWebSocketAndReconnectOnFailure()
            } catch {
                log.error("\(error.localizedDescription)")
            }
        }
    }
}
