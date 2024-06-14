import Foundation
import os

/// 
/// Responsible for syncing:
/// - full list of threads with server
/// - LAST `ThreadLastInteractionSyncLimit` interactions in each
///
public class SHInteractionsSyncOperation: Operation {
    
    public typealias OperationResult = Result<Void, Error>
    
    public let log = Logger(subsystem: "com.safehill", category: "BG-INTERACTIONS-SYNC")
    
    let delegatesQueue = DispatchQueue(label: "com.safehill.threads-interactions-sync.delegates")
    
    let user: SHAuthenticatedLocalUser
    let deviceId: String
    
    let socket: WebSocketAPI
    
    let interactionsSyncDelegates: [SHInteractionsSyncingDelegate]
    let userConnectionsDelegates: [SHUserConnectionRequestDelegate]
    
    private var retryDelay: UInt64 = 1
    private let maxRetryDelay: UInt64 = 8
    
    public init(
        user: SHAuthenticatedLocalUser,
        deviceId: String,
        interactionsSyncDelegates: [SHInteractionsSyncingDelegate],
        userConnectionsDelegates: [SHUserConnectionRequestDelegate]
    ) throws {
        self.user = user
        self.deviceId = deviceId
        self.interactionsSyncDelegates = interactionsSyncDelegates
        self.userConnectionsDelegates = userConnectionsDelegates
        self.socket = WebSocketAPI()
    }
    
    deinit {
        Task {
            await self.stopWebSocket()
        }
    }
    
    var serverProxy: SHServerProxy { self.user.serverProxy }
    
    private func startWebSocket() async throws {
        try await socket.connect(to: "ws/messages", as: self.user, from: self.deviceId)
        
        for try await message in await socket.receive() {
            
            self.processMessage(message)
            
            if self.isCancelled {
                await self.stopWebSocket()
                break
            }
        }
    }
    
    private func startWebSocketAndReconnectOnFailure() async throws {
        do {
            try await self.startWebSocket()
        } catch let error {
            if let error = error as? WebSocketConnectionError {
                switch error {
                case .disconnected, .closed, .connectionError, .transportError:
                    /// Disconnect if not already disconnected (this sets the `socket.webSocketTask` to `nil`
                    await self.stopWebSocket()
                    
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
    
    public func stopWebSocket() async {
        await self.socket.disconnect()
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
                
                
            case .connectionRequest:
                guard let encoded = try? JSONDecoder().decode(WebSocketMessage.NewUserConnection.self, from: contentData) else {
                    return
                }
                
                let requestor = encoded.requestor
                
                guard let publicKeyData = requestor.publicKey.data(using: .utf8),
                      let publicSignatureData = requestor.publicSignature.data(using: .utf8)
                else {
                    self.log.error("failed to decode user requesting a connection. Public key or signature can not be decoded")
                    return
                }
                
                let serverUser = SHRemoteUser(
                    identifier: requestor.identifier,
                    name: requestor.name,
                    publicKeyData: publicKeyData, 
                    publicSignatureData: publicSignatureData
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
                
            case .reactionAdd, .reactionRemove:
                
                guard let reaction = try? JSONDecoder().decode(WebSocketMessage.Reaction.self, from: contentData),
                      let interactionId = reaction.interactionId,
                      let reactionType = ReactionType(rawValue: reaction.reactionType)
                else {
                    self.log.critical("[ws] server sent a \(message.type.rawValue) message via WebSockets that can't be parsed or without an ID, or invalid reaction type. This is not supposed to happen. \(message.content)")
                    return
                }
                
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
                        if message.type == .reactionAdd {
                            $0.didAddReaction(reactionOutput, inGroup: reaction.anchorId)
                        } else {
                            $0.didRemoveReaction(reactionOutput, inGroup: reaction.anchorId)
                        }
                    case .thread:
                        if message.type == .reactionAdd {
                            $0.didAddReaction(reactionOutput, inThread: reaction.anchorId)
                        } else {
                            $0.didRemoveReaction(reactionOutput, inThread: reaction.anchorId)
                        }
                    case .none:
                        self.log.critical("[ws] invalid anchor type from server: \(reaction.anchorType)")
                    }
                })
                
            case .threadAdd:
                
                guard let threadOutputDTO = try? JSONDecoder().decode(ConversationThreadOutputDTO.self, from: contentData) else {
                    self.log.critical("[ws] server sent a \(message.type.rawValue) message via WebSockets that can't be parsed. This is not supposed to happen. \(message.content)")
                    return
                }
                
                Task {
                    await self.syncThreads(
                        remoteThreads: [threadOutputDTO]
                    )
                }
                
            case .threadAssetsShare, .groupAssetsShare:
                
                guard let threadAssetsWsMessage = try? JSONDecoder().decode(WebSocketMessage.ThreadAssets.self, from: contentData) else {
                    self.log.critical("[ws] server sent a \(message.type.rawValue) message via WebSockets that can't be parsed. This is not supposed to happen. \(message.content)")
                    return
                }
                
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
                ///
                /// Sync the threads (creates, removals)
                /// based on the list from server
                ///
                let _ = try await self.syncThreads(qos: .default)
                
                ///
                /// Get the summary to update the latest messages and interactions
                /// in threads and groups
                let summary = try await self.serverProxy.topLevelInteractionsSummaryFromRemote()
                
                self.delegatesQueue.async { [weak self] in
                    self?.interactionsSyncDelegates.forEach {
                        $0.didFetchRemoteThreadSummary(summary.summaryByThreadId)
                        $0.didFetchRemoteGroupSummary(summary.summaryByGroupId)
                    }
                }
                
                self.updateThreadsInteractions(using: summary.summaryByThreadId)
                self.updateGroupsInteractions(using: summary.summaryByGroupId)
                
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
