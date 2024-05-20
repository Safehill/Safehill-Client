import Foundation

extension SHInteractionsSyncOperation {
    
    func syncReactions(
        anchor: SHInteractionAnchor,
        anchorId: String,
        localReactions: [ReactionOutputDTO],
        remoteReactions: [ReactionOutputDTO],
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        var reactionsToUpdate = [ReactionOutputDTO]()
        var reactionsToRemove = [ReactionOutputDTO]()
        for remoteReaction in remoteReactions {
            let existing = localReactions.first(where: {
                $0.senderUserIdentifier == remoteReaction.senderUserIdentifier
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
                $0.senderUserIdentifier == localReaction.senderUserIdentifier
                && $0.inReplyToInteractionId == localReaction.inReplyToInteractionId
                && $0.inReplyToAssetGlobalIdentifier == localReaction.inReplyToAssetGlobalIdentifier
                && $0.reactionType == localReaction.reactionType
            })
            if existingOnRemote == nil {
                reactionsToRemove.append(localReaction)
            }
        }
        
        let dispatchGroup = DispatchGroup()
        var errors = [Error]()
        var anyChanged = false
        
        if reactionsToUpdate.count > 0 {
            dispatchGroup.enter()
            
            log.debug("[sync] syncing reactions in \(anchor.rawValue) \(anchorId). toUpdate=\(reactionsToUpdate.count)")
            
            let callback = { (addReactionsResult: Result<[ReactionOutputDTO], Error>) in
                if case .failure(let failure) = addReactionsResult {
                    self.log.warning("failed to add reactions retrieved from server on local. \(failure.localizedDescription)")
                    errors.append(failure)
                } else {
                    anyChanged = true
                }
                dispatchGroup.leave()
            }
            
            switch anchor {
            case .group:
                serverProxy.addLocalReactions(
                    reactionsToUpdate,
                    inGroup: anchorId,
                    completionHandler: callback
                )
            case .thread:
                serverProxy.addLocalReactions(
                    reactionsToUpdate,
                    inThread: anchorId,
                    completionHandler: callback
                )
            }
        }
        
        if reactionsToRemove.count > 0 {
            let callback = { (removeReactionsResult: Result<Void, Error>) in
                if case .failure(let failure) = removeReactionsResult {
                    self.log.warning("failed to remove reactions from local. \(failure.localizedDescription)")
                    errors.append(failure)
                } else {
                    anyChanged = true
                }
                dispatchGroup.leave()
            }
            
            dispatchGroup.enter()
            switch anchor {
            case .thread:
                serverProxy.localServer.removeReactions(
                    reactionsToRemove,
                    inThread: anchorId,
                    completionHandler: callback
                )
            case .group:
                serverProxy.localServer.removeReactions(
                    reactionsToRemove,
                    inGroup: anchorId,
                    completionHandler: callback
                )
            }
            
        }
        
        if anyChanged {
            let interactionsSyncDelegates = self.interactionsSyncDelegates
            self.delegatesQueue.async {
                switch anchor {
                case .thread:
                    interactionsSyncDelegates.forEach({ $0.reactionsDidChange(inThread: anchorId) })
                case .group:
                    interactionsSyncDelegates.forEach({ $0.reactionsDidChange(inGroup: anchorId) })
                }
            }
        }
        
        dispatchGroup.notify(queue: DispatchQueue.global(qos: qos)) {
            if errors.isEmpty {
                completionHandler(.success(()))
            } else {
                completionHandler(.failure(errors.first!))
            }
        }
    }
    
}
