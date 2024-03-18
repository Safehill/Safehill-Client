import Foundation

extension SHSyncOperation {
    
    func syncMessages(
        anchor: SHInteractionAnchor,
        anchorId: String,
        localMessages: [MessageOutputDTO],
        remoteMessages: [MessageOutputDTO],
        encryptionDetails: EncryptionDetailsClass,
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        var errors = [Error]()
        
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
                    errors.append(failure)
                case .success(let messages):
                    let threadsDelegates = self.threadsDelegates
                    self.delegatesQueue.async {
                        switch anchor {
                        case .group:
                            threadsDelegates.forEach({
                                $0.didReceiveMessages(
                                    messages,
                                    inGroup: anchorId,
                                    encryptionDetails: encryptionDetails
                                )
                            })
                        case .thread:
                            threadsDelegates.forEach({
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
        
        dispatchGroup.notify(queue: .global(qos: qos)) {
            guard errors.isEmpty else {
                self.log.warning("error while syncing messages in \(anchor.rawValue) \(anchorId): \(errors.map({ $0.localizedDescription }))")
                completionHandler(.failure(errors.first!))
                return
            }
            
            completionHandler(.success(()))
        }
    }
    
}