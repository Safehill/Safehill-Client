import Foundation

extension SHWebsocketSyncOperation {
    
    func syncReactions(
        anchor: SHInteractionAnchor,
        anchorId: String,
        localReactions: [ReactionOutputDTO],
        remoteReactions: [ReactionOutputDTO],
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping () -> Void
    ) {
        var reactionsToUpdate = [ReactionOutputDTO]()
        var reactionsToRemove = [ReactionOutputDTO]()
        for remoteReaction in remoteReactions {
            let existing = localReactions.first(where: {
                $0.senderPublicIdentifier == remoteReaction.senderPublicIdentifier
                && $0.inReplyToInteractionId == remoteReaction.inReplyToInteractionId
                && $0.inReplyToAssetGlobalIdentifier == remoteReaction.inReplyToAssetGlobalIdentifier
                && $0.reactionType == remoteReaction.reactionType
            })
            if existing == nil {
                reactionsToUpdate.append(remoteReaction)
            }
        }
        
        for localReaction in localReactions {
            let existingOnRemote = remoteReactions.first(where: {
                $0.senderPublicIdentifier == localReaction.senderPublicIdentifier
                && $0.inReplyToInteractionId == localReaction.inReplyToInteractionId
                && $0.inReplyToAssetGlobalIdentifier == localReaction.inReplyToAssetGlobalIdentifier
                && $0.reactionType == localReaction.reactionType
            })
            if existingOnRemote == nil {
                reactionsToRemove.append(localReaction)
            }
        }
        
        let dispatchGroup = DispatchGroup()
        
        if reactionsToUpdate.count > 0 {
            dispatchGroup.enter()
            
            log.debug("[reaction-sync] syncing reactions in \(anchor.rawValue) \(anchorId). toUpdate=\(reactionsToUpdate.count)")
            
            let callback = { (addReactionsResult: Result<[ReactionOutputDTO], Error>) in
                if case .failure(let failure) = addReactionsResult {
                    self.log.warning("[reaction-sync] failed to add reactions retrieved from server on local. \(failure.localizedDescription)")
                }
                dispatchGroup.leave()
            }
            
            switch anchor {
            case .group:
                serverProxy.addLocalReactions(
                    reactionsToUpdate,
                    toGroup: anchorId,
                    completionHandler: callback
                )
            case .thread:
                serverProxy.addLocalReactions(
                    reactionsToUpdate,
                    toThread: anchorId,
                    completionHandler: callback
                )
            }
        }
        
        if reactionsToRemove.count > 0 {
            switch anchor {
            case .thread:
                for reaction in reactionsToRemove {
                    guard let senderPublicIdentifier = reaction.senderPublicIdentifier else {
                        self.log.warning("[reaction-sync] No sender information in reaction to remove from thread \(anchorId) on local")
                        continue
                    }
                    
                    dispatchGroup.enter()
                    serverProxy.localServer.removeReaction(
                        reaction.reactionType,
                        senderPublicIdentifier: senderPublicIdentifier,
                        inReplyToAssetGlobalIdentifier: reaction.inReplyToAssetGlobalIdentifier,
                        inReplyToInteractionId: reaction.inReplyToInteractionId,
                        fromThread: anchorId
                    ) { result in
                        if case .failure(let failure) = result {
                            self.log.warning("[reaction-sync] failed to add reactions retrieved from server to thread \(anchorId) on local. \(failure.localizedDescription)")
                        }
                        dispatchGroup.leave()
                    }
                }
            case .group:
                for reaction in reactionsToRemove {
                    guard let senderPublicIdentifier = reaction.senderPublicIdentifier else {
                        self.log.warning("[reaction-sync] No sender information in reaction to remove from thread \(anchorId) on local")
                        continue
                    }
                    
                    dispatchGroup.enter()
                    serverProxy.localServer.removeReaction(
                        reaction.reactionType,
                        senderPublicIdentifier: senderPublicIdentifier,
                        inReplyToAssetGlobalIdentifier: reaction.inReplyToAssetGlobalIdentifier,
                        inReplyToInteractionId: reaction.inReplyToInteractionId,
                        fromGroup: anchorId
                    ) { result in
                        if case .failure(let failure) = result {
                            self.log.warning("failed to add reactions retrieved from server to group \(anchorId) on local. \(failure.localizedDescription)")
                        }
                        dispatchGroup.leave()
                    }
                }
            }
        }
        
        dispatchGroup.notify(queue: DispatchQueue.global(qos: qos)) {
            let interactionsSyncDelegates = self.interactionsSyncDelegates
            self.delegatesQueue.async {
                switch anchor {
                case .thread:
                    interactionsSyncDelegates.forEach({ $0.reactionsDidChange(inThread: anchorId) })
                case .group:
                    interactionsSyncDelegates.forEach({ $0.reactionsDidChange(inGroup: anchorId) })
                }
            }
            
            completionHandler()
        }
    }
    
}
