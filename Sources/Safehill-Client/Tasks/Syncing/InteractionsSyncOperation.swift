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
    
    public init(user: SHAuthenticatedLocalUser,
                deviceId: String,
                interactionsSyncDelegates: [SHInteractionsSyncingDelegate]) throws {
        self.user = user
        self.deviceId = deviceId
        self.interactionsSyncDelegates = interactionsSyncDelegates
        self.socket = WebSocketAPI()
    }
    
    deinit {
        Task {
            await self.stopWebSocket()
        }
    }
    
    var serverProxy: SHServerProxy { self.user.serverProxy }
    
    private func startWebSocket() async throws {
        try await socket.connect(as: self.user, from: self.deviceId)
        
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
            if error is WebSocketConnectionError {
                try await self.startWebSocketAndReconnectOnFailure()
            }
            log.error("failed to connect to websocket: \(error.localizedDescription)")
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
        
        self.delegatesQueue.async {
            
            switch message.type {
            case .connectionAck:
                guard let encoded = try? JSONDecoder().decode(WebSocketMessage.ConnectionAck.self, from: contentData) else {
                    return
                }
                self.log.debug("CONNECTED: userPublicId=\(encoded.userPublicIdentifier), deviceId=\(encoded.deviceId)")
                
            case .message:
                
                guard let textMessage = try? JSONDecoder().decode(WebSocketMessage.TextMessage.self, from: contentData),
                      let interactionId = textMessage.interactionId else {
                    self.log.critical("server sent a \(message.type.rawValue) message via WebSockets but the client can't validate it. This is not supposed to happen. \(message.content)")
                    return
                }
                
                let messageOutput = MessageOutputDTO(
                    interactionId: interactionId,
                    senderUserIdentifier: textMessage.senderPublicIdentifier,
                    inReplyToAssetGlobalIdentifier: textMessage.inReplyToAssetGlobalIdentifier,
                    inReplyToInteractionId: textMessage.inReplyToInteractionId,
                    encryptedMessage: textMessage.encryptedMessage,
                    createdAt: textMessage.sentAt
                )
                
                interactionsSyncDelegates.forEach({
                    switch SHInteractionAnchor(rawValue: textMessage.anchorType) {
                    case .group:
                        $0.didReceiveMessages([messageOutput], inGroup: textMessage.anchorId)
                    case .thread:
                        $0.didReceiveMessages([messageOutput], inThread: textMessage.anchorId)
                    case .none:
                        self.log.critical("invalid anchor type from server: \(textMessage.anchorType)")
                    }
                })
                
            case .reactionAdd, .reactionRemove:
                
                guard let reaction = try? JSONDecoder().decode(WebSocketMessage.Reaction.self, from: contentData),
                      let interactionId = reaction.interactionId,
                      let reactionType = ReactionType(rawValue: reaction.reactionType)
                else {
                    self.log.critical("server sent a \(message.type.rawValue) message via WebSockets that can't be parsed or without an ID, or invalid reaction type. This is not supposed to happen. \(message.content)")
                    return
                }
                
                let reactionOutput = ReactionOutputDTO(
                    interactionId: interactionId,
                    senderUserIdentifier: reaction.senderPublicIdentifier,
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
                        self.log.critical("invalid anchor type from server: \(reaction.anchorType)")
                    }
                })
                
            case .threadAdd:
                
                guard let threadOutputDTO = try? JSONDecoder().decode(ConversationThreadOutputDTO.self, from: contentData) else {
                    self.log.critical("server sent a \(message.type.rawValue) message via WebSockets that can't be parsed. This is not supposed to happen. \(message.content)")
                    return
                }
                
                self.syncThreadsFromAuthorizedUsers(
                    remoteThreads: [threadOutputDTO],
                    qos: .utility
                ) { _ in }
            }
        }
    }
    
    public func run(
        for anchor: SHInteractionAnchor,
        anchorId: String,
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        switch anchor {
        case .group:
            self.syncGroupInteractions(groupId: anchorId, qos: qos) { result in
                switch result {
                case .failure(let err):
                    self.log.error("failed to sync interactions in \(anchor.rawValue) \(anchorId): \(err.localizedDescription)")
                    completionHandler(.failure(err))
                case .success:
                    completionHandler(.success(()))
                }
            }
        case .thread:
            self.serverProxy.remoteServer.getThread(withId: anchorId) { getThreadResult in
                switch getThreadResult {
                case .failure(let error):
                    self.log.error("failed to get thread with id \(anchorId) from server")
                    completionHandler(.failure(error))
                case .success(let serverThread):
                    guard let serverThread else {
                        self.log.warning("no such thread with id \(anchorId) from server")
                        completionHandler(.success(()))
                        return
                    }
                    self.syncThreadInteractions(serverThread: serverThread, qos: qos) { syncResult in
                        switch syncResult {
                        case .failure(let err):
                            self.log.error("failed to sync interactions in \(anchor.rawValue) \(anchorId): \(err.localizedDescription)")
                            completionHandler(.failure(err))
                        case .success:
                            completionHandler(.success(()))
                        }
                    }
                }
            }
        }
    }
    
    /// Main run.
    /// 1. Pull the latest threads from remote, and sync the latest interactions via REST
    /// 2. Start the WEBSOCKET connection for updates
    public override func start() {
        super.start()
        
        Task {
            do {
                try await self.syncThreadsAndLastInteractions(qos: .default)
            } catch {
                self.log.critical("failure syncing thread interactions: \(error.localizedDescription)")
                throw error
            }
            
            try await self.startWebSocketAndReconnectOnFailure()
        }
    }
}
