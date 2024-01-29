import Foundation

extension SHSyncOperation {
    
    func syncMessages(
        anchor: InteractionAnchor,
        anchorId: String,
        localMessages: [MessageOutputDTO],
        remoteMessages: [MessageOutputDTO]
    ) throws {
        var messagesToUpdate = [MessageOutputDTO]()
        for remoteMessage in remoteMessages {
            let existing = localMessages.first(where: {
                $0.interactionId == remoteMessage.interactionId
            })
            if existing == nil {
                messagesToUpdate.append(remoteMessage)
            }
        }
        
        let dispatchGroup = DispatchGroup()
        
        if messagesToUpdate.count > 0 {
            let callback = { (addMessagesResult: Result<[MessageOutputDTO], Error>) in
                switch addMessagesResult {
                case .failure(let failure):
                    self.log.warning("failed to add messages retrieved from server on local. \(failure.localizedDescription)")
                case .success(let messages):
                    switch anchor {
                    case .group:
                        self.threadsDelegates.forEach({ $0.didReceiveMessages(messages, inGroup: anchorId) })
                    case .thread:
                        self.threadsDelegates.forEach({ $0.didReceiveMessages(messages, inThread: anchorId) })
                    }
                }
                dispatchGroup.leave()
            }
            
            dispatchGroup.enter()
            
            switch anchor {
            case .group:
                serverProxy.localServer.addMessages(
                    messagesToUpdate,
                    inGroup: anchorId,
                    completionHandler: callback
                )
            case .thread:
                serverProxy.localServer.addMessages(
                    messagesToUpdate,
                    inThread: anchorId,
                    completionHandler: callback
                )
            }
        }
        
        let dispatchResult = dispatchGroup.wait(timeout: .now() + .milliseconds(SHDefaultNetworkTimeoutInMilliseconds))
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
    }
    
}
