import Foundation

extension SHSyncOperation {
    
    func syncReactions(
        anchor: SHInteractionAnchor,
        anchorId: String,
        localReactions: [ReactionOutputDTO],
        remoteReactions: [ReactionOutputDTO]
    ) throws {
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
        var anyChanged = false
        
        if reactionsToUpdate.count > 0 {
            dispatchGroup.enter()
            
            let callback = { (addReactionsResult: Result<[ReactionOutputDTO], Error>) in
                if case .failure(let failure) = addReactionsResult {
                    self.log.warning("failed to add reactions retrieved from server on local. \(failure.localizedDescription)")
                } else {
                    anyChanged = true
                }
                dispatchGroup.leave()
            }
            
            switch anchor {
            case .group:
                serverProxy.localServer.addReactions(
                    reactionsToUpdate,
                    inGroup: anchorId,
                    completionHandler: callback
                )
            case .thread:
                serverProxy.localServer.addReactions(
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
            self.delegatesQueue.async { [weak self] in
                switch anchor {
                case .thread:
                    self?.threadsDelegates.forEach({ $0.reactionsDidChange(inThread: anchorId) })
                case .group:
                    self?.threadsDelegates.forEach({ $0.reactionsDidChange(inGroup: anchorId) })
                }
            }
        }
        
        let dispatchResult = dispatchGroup.wait(timeout: .now() + .milliseconds(SHDefaultNetworkTimeoutInMilliseconds))
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
    }
    
}
