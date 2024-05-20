import Foundation
import os

/// 
/// Responsible for syncing:
/// - full list of threads with server
/// - LAST `ThreadLastInteractionSyncLimit` interactions in each
///
public class SHThreadsInteractionsSyncOperation: Operation, SHBackgroundOperationProtocol {
    
    public typealias OperationResult = Result<Void, Error>
    
    public let log = Logger(subsystem: "com.safehill", category: "BG-INTERACTIONS-SYNC")
    
    let delegatesQueue = DispatchQueue(label: "com.safehill.threads-interactions-sync.delegates")
    
    let user: SHAuthenticatedLocalUser
    let deviceId: String
    
    let socket: WebSocketAPI
    
    let assetsSyncDelegates: [SHAssetSyncingDelegate]
    let threadsSyncDelegates: [SHThreadSyncingDelegate]
    
    public init(user: SHAuthenticatedLocalUser,
                deviceId: String,
                assetsSyncDelegates: [SHAssetSyncingDelegate],
                threadsSyncDelegates: [SHThreadSyncingDelegate]) throws {
        self.user = user
        self.deviceId = deviceId
        self.assetsSyncDelegates = assetsSyncDelegates
        self.threadsSyncDelegates = threadsSyncDelegates
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
        } catch is WebSocketConnectionError {
            try await self.startWebSocketAndReconnectOnFailure()
        }
    }
    
    private func stopWebSocket() async {
        await self.socket.disconnect()
    }
    
    public func run(qos: DispatchQoS.QoSClass,
                    completionHandler: @escaping (Result<Void, Error>) -> Void) {
        let syncOperation = SHSyncOperation(
            user: self.user,
            assetsDelegates: self.assetsSyncDelegates,
            threadsDelegates: self.threadsSyncDelegates
        )
        syncOperation.syncLastThreadInteractions(qos: qos) {
            result in
            
            if case .failure(let failure) = result {
                self.log.critical("failure syncing thread interactions: \(failure.localizedDescription)")
                completionHandler(.failure(failure))
            }
            completionHandler(.success(()))
            
            Task(priority: qos.toTaskPriority()) {
                try await self.startWebSocketAndReconnectOnFailure()
            }
        }
    }
    
    private func processMessage(_ message: WebSocketMessage) {
        guard let contentData = message.content.data(using: .utf8) else {
            log.critical("[ws] unable to parse message content")
            return
        }
        
        let threadsSyncDelegates = self.threadsSyncDelegates
        self.delegatesQueue.async {
            
            switch message.type {
            case .message:
                
                guard let textMessage = try? JSONDecoder().decode(WebSocketMessage.TextMessage.self, from: contentData),
                      let interactionId = textMessage.interactionId else {
                    self.log.critical("server sent a \(message.type.rawValue) message via WebSockets without an ID. This is not supposed to happen. \(message.content)")
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
                
                threadsSyncDelegates.forEach({
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
                
                threadsSyncDelegates.forEach({
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
                
                threadsSyncDelegates.forEach({
                    $0.didAddThread(threadOutputDTO)
                })
            }
        }
    }
}
