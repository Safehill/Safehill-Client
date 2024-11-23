import Foundation

extension SHLocalDownloadOperation {
    
    ///
    /// For all the descriptors whose originator user is _this_ user, notify the restoration delegate
    /// about all groups that need to be restored.
    /// Uploads and shares will be reported separately, according to the contract in the delegate.
    /// Because these assets exist in the local server, the assumption is that they will also be present in the
    /// upload and history queues, so they don't need to be re-created.
    /// The recreation only happens in the sister method:
    /// `SHRemoteDownloadOperation::restoreQueueItems(descriptorsByGlobalIdentifier:qos:completionHandler:)`
    ///
    /// - Parameters:
    ///   - descriptorsByGlobalIdentifier: all the descriptors keyed by asset global identifier
    ///   - globalIdentifiersSharedBySelf: the asset global identifiers shared by self
    ///   - completionHandler: the callback method
    ///
    func restoreQueueItems(
        descriptorsByGlobalIdentifier original: [GlobalIdentifier: any SHAssetDescriptor],
        filteringKeys globalIdentifiersSharedBySelf: [GlobalIdentifier],
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        guard original.count > 0 else {
            completionHandler(.success(()))
            return
        }
        
        guard globalIdentifiersSharedBySelf.count > 0 else {
            completionHandler(.success(()))
            return
        }
        
        let descriptors = original.values.filter({
            globalIdentifiersSharedBySelf.contains($0.globalIdentifier)
        })
        
        guard descriptors.count > 0 else {
            completionHandler(.success(()))
            return
        }
        
        let userIdsToFetch = descriptors.allReferencedUserIds()
        
        self.getUsers(withIdentifiers: Array(userIdsToFetch)) { getUsersResult in
            switch getUsersResult {
                
            case .failure(let error):
                completionHandler(.failure(error))
            
            case .success(let usersDict):
                
                Task {
                    let (
                        groupIdToUploadItems,
                        groupIdToShareItems
                    ) = await self.historyItems(
                        from: descriptors,
                        usersDict: usersDict
                    )
                    
                    let restorationDelegate = self.restorationDelegate
                    self.delegatesQueue.async {
                        restorationDelegate.restoreUploadHistoryItems(from: groupIdToUploadItems)
                        restorationDelegate.restoreShareHistoryItems(from: groupIdToShareItems)
                    }
                    
                    completionHandler(.success(()))
                }
            }
        }
    }
}
