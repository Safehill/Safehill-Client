import Foundation

extension SHSyncOperation {
    
    func syncMessages(
        anchor: SHInteractionAnchor,
        anchorId: String,
        localMessages: [MessageOutputDTO],
        remoteMessages: [MessageOutputDTO],
        encryptionDetails: EncryptionDetailsClass
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
        
        log.debug("[sync] syncing messages in \(anchor.rawValue) \(anchorId). toUpdate=\(messagesToUpdate.count)")
        let dispatchGroup = DispatchGroup()
        
        if messagesToUpdate.count > 0 {
            let callback = { (addMessagesResult: Result<[MessageOutputDTO], Error>) in
                switch addMessagesResult {
                case .failure(let failure):
                    self.log.warning("failed to add messages retrieved from server on local. \(failure.localizedDescription)")
                case .success(let messages):
                    self.delegatesQueue.async { [weak self] in
                        switch anchor {
                        case .group:
                            self?.threadsDelegates.forEach({
                                $0.didReceiveMessages(
                                    messages,
                                    inGroup: anchorId,
                                    encryptionDetails: encryptionDetails
                                )
                            })
                        case .thread:
                            self?.threadsDelegates.forEach({
                                $0.didReceiveMessages(
                                    messages,
                                    inThread: anchorId,
                                    encryptionDetails: encryptionDetails
                                )
                            })
                        }
                    }
                }
            }
            
            dispatchGroup.enter()
            switch anchor {
            case .group:
                serverProxy.localServer.addMessages(
                    messagesToUpdate,
                    inGroup: anchorId
                ) {
                    self.log.debug("[sync] done syncing messages in \(anchor.rawValue) \(anchorId)")
                    callback($0)
                    dispatchGroup.leave()
                }
            case .thread:
                serverProxy.localServer.addMessages(
                    messagesToUpdate,
                    inThread: anchorId
                ) {
                    self.log.debug("[sync] done syncing messages in \(anchor.rawValue) \(anchorId)")
                    callback($0)
                    dispatchGroup.leave()
                }
            }
        }
        
        let dispatchResult = dispatchGroup.wait(timeout: .now() + .milliseconds(SHDefaultNetworkTimeoutInMilliseconds))
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
    }
    
}
