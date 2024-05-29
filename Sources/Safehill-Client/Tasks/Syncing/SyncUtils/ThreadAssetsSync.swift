import Foundation
import KnowledgeBase

extension SHInteractionsSyncOperation {
    
    func syncThreadAssets(
        serverThread: ConversationThreadOutputDTO,
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        guard let userStore = SHDBManager.sharedInstance.userStore else {
            completionHandler(.failure(KBError.databaseNotReady))
            return
        }
        
        log.debug("[sync] syncing assets in thread \(serverThread.threadId)")
        
        let threadId = serverThread.threadId
        
        let dispatchGroup = DispatchGroup()
        var error: Error? = nil
        
        ///
        /// Retrieve the REMOTE thread assets photo messages
        ///
        var remoteThreadAssets = [ConversationThreadAssetDTO]()
        dispatchGroup.enter()
        self.serverProxy.remoteServer.getAssets(
            inThread: threadId
        ) { result in
            switch result {
            case .failure(let err):
                error = err
            case .success(let threadAssetsDTO):
                remoteThreadAssets = threadAssetsDTO.photoMessages
            }
            dispatchGroup.leave()
        }
        
        ///
        /// Retrieve the LOCAL thread assets photo messages
        ///
        var localThreadAssets = [ConversationThreadAssetDTO]()
        dispatchGroup.enter()
        self.serverProxy.localServer.getAssets(
            inThread: threadId
        ) { localResult in
            switch localResult {
            case .failure(let err):
                error = err
                self.log.error("failed to retrieve local assets for thread \(threadId)")
            case .success(let threadAssetsDTO):
                localThreadAssets = threadAssetsDTO.photoMessages
            }
            dispatchGroup.leave()
        }
        
        dispatchGroup.notify(queue: .global(qos: qos)) {
            guard error == nil else {
                self.log.error("error syncing assets for thread \(threadId). \(error!.localizedDescription)")
                completionHandler(.failure(error!))
                return
            }
            
            var assetsToUpdate = [ConversationThreadAssetDTO]()
            var assetsToRemove = [GlobalIdentifier]()
            for remoteThreadAsset in remoteThreadAssets {
                let existing = localThreadAssets.first(where: {
                    $0.globalIdentifier == remoteThreadAsset.globalIdentifier
                })
                if existing == nil {
                    assetsToUpdate.append(remoteThreadAsset)
                }
            }
            
            for localThreadAsset in localThreadAssets {
                let existingOnRemote = remoteThreadAssets.first(where: {
                    $0.globalIdentifier == localThreadAsset.globalIdentifier
                })
                if existingOnRemote == nil {
                    assetsToRemove.append(localThreadAsset.globalIdentifier)
                }
            }
            
            if assetsToUpdate.count > 0 {
                let writeBatch = userStore.writeBatch()
                for assetToUpdate in assetsToUpdate {
                    writeBatch.set(
                        value: [
                            "addedByUserIdentifier": assetToUpdate.addedByUserIdentifier,
                            "groupId": assetToUpdate.groupId
                        ],
                        for: "\(SHInteractionAnchor.thread.rawValue)::\(threadId)::assets::\(assetToUpdate.globalIdentifier)"
                    )
                }
                
                dispatchGroup.enter()
                writeBatch.write { _ in
                    dispatchGroup.leave()
                }
            }
            
            if assetsToRemove.count > 0 {
                dispatchGroup.enter()
                let assetGidsToRemove = assetsToRemove.map {
                    "\(SHInteractionAnchor.thread.rawValue)::\(threadId)::assets::\($0)"
                }
                userStore.removeValues(for: assetGidsToRemove, completionHandler: { (result: Result<Void, Error>) in
                    dispatchGroup.leave()
                })
            }
            
            dispatchGroup.notify(queue: .global(qos: qos)) {
                completionHandler(.success(()))
            }
        }
    }
}
