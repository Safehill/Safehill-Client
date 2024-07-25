import Foundation
import os

/// 
/// Responsible for syncing:
/// - full list of threads with server
/// - LAST `ThreadLastInteractionSyncLimit` interactions in each
///
public class SHInteractionsSyncOperation: Operation {
    
    public typealias OperationResult = Result<Void, Error>
    
    private static var isWebSocketConnected = false
    private static let memberAccessQueue = DispatchQueue(label: "SHInteractionsSyncOperation.memberAccessQueue")
    
    public let log = Logger(subsystem: "com.safehill", category: "BG-INTERACTIONS-SYNC")
    
    let delegatesQueue = DispatchQueue(label: "com.safehill.threads-interactions-sync.delegates")
    
    let user: SHAuthenticatedLocalUser
    let deviceId: String
    
    let socket: WebSocketAPI
    
    let websocketConnectionDelegates: [WebSocketDelegate]
    let interactionsSyncDelegates: [SHInteractionsSyncingDelegate]
    let userConnectionsDelegates: [SHUserConnectionRequestDelegate]
    
    private var retryDelay: UInt64 = 1
    private let maxRetryDelay: UInt64 = 8
    
    public init(
        user: SHAuthenticatedLocalUser,
        deviceId: String,
        websocketConnectionDelegates: [WebSocketDelegate],
        interactionsSyncDelegates: [SHInteractionsSyncingDelegate],
        userConnectionsDelegates: [SHUserConnectionRequestDelegate]
    ) throws {
        self.user = user
        self.deviceId = deviceId
        self.websocketConnectionDelegates = websocketConnectionDelegates
        self.interactionsSyncDelegates = interactionsSyncDelegates
        self.userConnectionsDelegates = userConnectionsDelegates
        self.socket = WebSocketAPI()
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
                case .disconnected, .closed, .connectionError, .transportError:
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
                
            case .connectionRequest:
                guard let encoded = try? JSONDecoder().decode(WebSocketMessage.NewUserConnection.self, from: contentData) else {
                    return
                }
                
                let requestor = encoded.requestor
                
                self.log.debug("[ws] USERCONNECTION: request from \(requestor.name)")
                
                let serverUser = SHRemoteUser(
                    identifier: requestor.identifier,
                    name: requestor.name,
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
                    await self.serverProxy.createThreadsLocally(
                        [threadOutputDTO]
                    )
                }
                
            case .threadAssetsShare, .groupAssetsShare:
                
                guard let threadAssetsWsMessage = try? JSONDecoder().decode(WebSocketMessage.ThreadAssets.self, from: contentData) else {
                    self.log.critical("[ws] server sent a \(message.type.rawValue) message via WebSockets that can't be parsed. This is not supposed to happen. \(message.content)")
                    return
                }
                
                self.log.debug("[ws] ASSETSHARE \(message.type.rawValue): thread id \(threadAssetsWsMessage.threadId)")
                
                let threadId = threadAssetsWsMessage.threadId
                let threadAssets = threadAssetsWsMessage.assets
                
                interactionsSyncDelegates.forEach({
                    if message.type == .threadAssetsShare {
                        $0.didReceivePhotoMessages(threadAssets, in: threadId)
                    } else {
                        $0.didReceivePhotos(threadAssets, in: threadId)
                    }
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
        
        Task {
            do {
                try await self.syncInteractionSummaries()
                log.debug("[SHInteractionsSyncOperation] done syncing interaction summaries")
            } catch {
                log.error("\(error.localizedDescription)")
            }
        }
        
        Task {
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
