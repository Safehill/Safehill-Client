import Foundation

extension SHRemoteDownloadOperation {
    
    internal func recreateLocalAssets(
        descriptorsByGlobalIdentifier original: [GlobalIdentifier: any SHAssetDescriptor],
        filteringKeys globalIdentifiersSharedBySelf: [GlobalIdentifier],
        qos: DispatchQoS.QoSClass,
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
        
        let descriptorsByGlobalIdentifier = original.filter({
            globalIdentifiersSharedBySelf.contains($0.value.globalIdentifier)
            && $0.value.localIdentifier != nil
        })
        
        self.log.debug("[\(type(of: self))] recreating local assets and queue items for \(globalIdentifiersSharedBySelf)")
        
        ///
        /// Get the `.lowResolution` assets data from the remote server
        ///
        self.serverProxy.getAssetsAndCache(
            withGlobalIdentifiers: globalIdentifiersSharedBySelf,
            versions: [.lowResolution],
            synchronousFetch: true
        ) { fetchResult in
            switch fetchResult {
            case .success:
                ///
                /// **Remember:** saving a `.lowResolution` version only
                /// will remove the `.midResolution` and the `.hiResolution`
                /// in the cache.
                ///
                /// Notify the delegates about successful upload and share queue items
                ///
                self.restoreQueueItems(
                    descriptorsByGlobalIdentifier: descriptorsByGlobalIdentifier,
                    qos: qos,
                    completionHandler: completionHandler
                )
            case .failure(let error):
                self.log.error("[\(type(of: self))] failed to fetch assets from remote server. Assets in the local library but uploaded will not be marked as such. This operation will be attempted again. \(error.localizedDescription)")
                completionHandler(.failure(error))
            }
        }
    }
    
    ///
    /// For all the descriptors whose originator user is _this_ user notify the restoration delegate about the change.
    /// Uploads and shares will be reported separately, according to the contract in the delegate.
    ///
    /// - Parameters:
    ///   - descriptorsByGlobalIdentifier: all the descriptors keyed by asset global identifier
    ///   - qos: the quality of service
    ///   - completionHandler: the callback method
    ///
    private func restoreQueueItems(
        descriptorsByGlobalIdentifier: [GlobalIdentifier: any SHAssetDescriptor],
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<Void, Error>) -> Void
    ) {
        guard descriptorsByGlobalIdentifier.count > 0 else {
            completionHandler(.success(()))
            return
        }
        
        let userIdsToFetch = Array(descriptorsByGlobalIdentifier.values).allReferencedUserIds()
        
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
                        from: Array(descriptorsByGlobalIdentifier.values),
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
    
    /// Returns nil if the encrypted title is nil
    /// Throws if the decryption fails or some required details are missing
    /// - Parameters:
    ///   - groupInfo: the GroupInfo in the descriptor
    ///   - groupId: the group id
    /// - Returns: 
    private func decryptTitle(
        groupInfo: any SHAssetGroupInfo,
        groupId: String
    ) async throws -> String? {
        if let encryptedTitle = groupInfo.encryptedTitle,
           let groupCreatorPublicId = groupInfo.createdBy
        {
            let interactionsController = SHUserInteractionController(user: self.user)
            return try await interactionsController.decryptTitle(
                encryptedTitle: encryptedTitle,
                createdBy: groupCreatorPublicId,
                groupId: groupId
            )
        }
        
        return nil
    }
    
    /// For each descriptor (there's one per asset), create in-memory representation of:
    /// - `SHUploadHistoryItem`s, aka the upload event (by this user)
    /// - `SHShareHistoryItem`s, aka all share events for the asset (performed by this user)
    ///
    /// Such representation is keyed by groupId.
    /// In other words, for each group there is as many history items as many assets were shared in the group.
    ///
    /// - Parameters:
    ///   - descriptors: the descriptors to process
    ///   - usersDict: the users mentioned in the descriptors, keyed by identifier
    ///
    internal func historyItems(
        from descriptors: [any SHAssetDescriptor],
        usersDict: [UserIdentifier: any SHServerUser]
    ) async -> (
        [String: [(SHUploadHistoryItem, Date)]],
        [String: [(SHShareHistoryItem, Date)]]
    ) {
        var groupIdToUploadItems = [String: [(SHUploadHistoryItem, Date)]]()
        var groupIdToShareItems = [String: [(SHShareHistoryItem, Date)]]()
        
        for descriptor in descriptors {
            
            guard let senderUser = usersDict[descriptor.sharingInfo.sharedByUserIdentifier] else {
                self.log.critical("[\(type(of: self))] inconsistency between user ids referenced in descriptors and user objects returned from server. No user for id \(descriptor.sharingInfo.sharedByUserIdentifier)")
                continue
            }
            
            var otherUserIdsSharedWithByGroupId = [String: [(with: any SHServerUser, at: Date)]]()
            
            for (recipientUserId, groupId) in descriptor.sharingInfo.sharedWithUserIdentifiersInGroup {
                
                guard let groupInfo = descriptor.sharingInfo.groupInfoById[groupId] else {
                    self.log.critical("[\(type(of: self))] no group info in descriptor for id \(groupId)")
                    continue
                }
                
                guard let groupCreationDate = groupInfo.createdAt else {
                    self.log.critical("[\(type(of: self))] no group creation date in descriptor for id \(groupId)")
                    continue
                }
                
                if recipientUserId == self.user.identifier {
                    let clearTitle: String?
                    do {
                        clearTitle = try await self.decryptTitle(groupInfo: groupInfo, groupId: groupId)
                    } catch {
                        self.log.critical("Unable to decrypt title for groupId=\(groupId): \(error.localizedDescription)")
                        continue
                    }
                    
                    let item = SHUploadHistoryItem(
                        asset: SHUploadableAsset(
                            localIdentifier: descriptor.localIdentifier,
                            globalIdentifier: descriptor.globalIdentifier,
                            creationDate: descriptor.creationDate,
                            data: [:]
                        ),
                        versions: [.lowResolution, .hiResolution],
                        groupId: groupId,
                        groupTitle: clearTitle,
                        eventOriginator: senderUser,
                        sharedWith: [],
                        invitedUsers: Array((groupInfo.invitedUsersPhoneNumbers ?? [:]).keys),
                        asPhotoMessageInThreadId: groupInfo.createdFromThreadId,
                        isBackground: false
                    )
                    
                    if groupIdToUploadItems[groupId] == nil {
                        groupIdToUploadItems[groupId] = [(item, groupCreationDate)]
                    } else {
                        groupIdToUploadItems[groupId]!.append((item, groupCreationDate))
                    }
                    
                } else {
                    guard let recipient = usersDict[recipientUserId] else {
                        self.log.critical("[\(type(of: self))] inconsistency between user ids referenced in descriptors and user objects returned from server. No user for id \(recipientUserId)")
                        continue
                    }
                    if otherUserIdsSharedWithByGroupId[groupId] == nil {
                        otherUserIdsSharedWithByGroupId[groupId] = [(with: recipient, at: groupCreationDate)]
                    } else {
                        otherUserIdsSharedWithByGroupId[groupId]?.append(
                            missingContentsFrom: [(with: recipient, at: groupCreationDate)],
                            compareUsing: { $0.with.identifier == $1.with.identifier }
                        )
                    }
                }
            }
            
            for (groupId, shareInfo) in otherUserIdsSharedWithByGroupId {
                guard let groupInfo = descriptor.sharingInfo.groupInfoById[groupId] else {
                    self.log.critical("[\(type(of: self))] no group info in descriptor for id \(groupId)")
                    continue
                }
                
                let clearTitle: String?
                do {
                    clearTitle = try await self.decryptTitle(groupInfo: groupInfo, groupId: groupId)
                } catch {
                    self.log.critical("Unable to decrypt title for groupId=\(groupId): \(error.localizedDescription)")
                    continue
                }
                
                let item = SHShareHistoryItem(
                    asset: SHUploadableAsset(
                        localIdentifier: descriptor.localIdentifier,
                        globalIdentifier: descriptor.globalIdentifier,
                        creationDate: descriptor.creationDate,
                        data: [:]
                    ),
                    versions: [.lowResolution, .hiResolution],
                    groupId: groupId,
                    groupTitle: clearTitle,
                    eventOriginator: senderUser,
                    sharedWith: shareInfo.map({ $0.with }),
                    invitedUsers: Array((descriptor.sharingInfo.groupInfoById[groupId]?.invitedUsersPhoneNumbers ?? [:]).keys),
                    asPhotoMessageInThreadId: descriptor.sharingInfo.groupInfoById[groupId]?.createdFromThreadId,
                    isBackground: false
                )
                
                let maxDate: Date = shareInfo.reduce(Date.distantPast) {
                    (currentMax, tuple) in
                    if currentMax.compare(tuple.at) == .orderedAscending {
                        return tuple.at
                    }
                    return currentMax
                }
                
                if groupIdToShareItems[groupId] == nil {
                    groupIdToShareItems[groupId] = [(item, maxDate)]
                } else {
                    groupIdToShareItems[groupId]!.append((item, maxDate))
                }
            }
        }
        
        return (
            groupIdToUploadItems,
            groupIdToShareItems
        )
    }
}
