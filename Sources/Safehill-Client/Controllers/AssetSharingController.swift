import Foundation

public class SHAssetSharingController {
    
    public let localUser: SHAuthenticatedLocalUser
    
    public init(localUser: SHAuthenticatedLocalUser) {
        self.localUser = localUser
    }
    
    private var serverProxy: SHServerProxy {
        self.localUser.serverProxy
    }
    
    internal func createGroupEncryptionDetails(
        for recipients: [any SHServerUser],
        in groupId: String,
        updateGroupTitleTo groupTitle: String? = nil
    ) async throws {
        try await withUnsafeThrowingContinuation { (continuation: UnsafeContinuation<Void, any Error>) in
            
            let interactionsController = SHUserInteractionController(user: localUser)
            
            log.debug("creating or updating group for request groupId=\(groupId)")
            interactionsController.setupGroup(
                title: groupTitle,
                groupId: groupId,
                with: recipients
            ) { result in
                switch result {
                case .failure(let error):
                    log.error("failed to initialize group. \(error.localizedDescription)")
                    continuation.resume(throwing: error)
                case .success:
                    continuation.resume(returning: ())
                }
            }
        }
    }
    
    internal func shareAsset(
        globalIdentifier: GlobalIdentifier,
        versions: [SHAssetQuality],
        createdBy: any SHServerUser,
        with recipients: [any SHServerUser],
        via groupId: String,
        asPhotoMessageInThreadId: String? = nil,
        permissions: Int?,
        isBackground: Bool = false
    ) async throws {
        log.info("generating encrypted assets for asset with id \(globalIdentifier) for users \(recipients.map({ $0.identifier }))")
        
        try await withUnsafeThrowingContinuation { (continuation: UnsafeContinuation<Void, any Error>) in
            
            self.localUser.shareableEncryptedAsset(
                globalIdentifier: globalIdentifier,
                versions: versions,
                createdBy: createdBy,
                with: recipients,
                groupId: groupId
            ) { result in
                switch result {
                
                case .failure(let error):
                    log.error("failed to create shareable asset for \(globalIdentifier). \(error.localizedDescription)")
                    continuation.resume(throwing: error)
                
                case .success(let shareableEncryptedAsset):
                    self.serverProxy.share(
                        shareableEncryptedAsset,
                        asPhotoMessageInThreadId: asPhotoMessageInThreadId,
                        permissions: permissions,
                        suppressNotification: isBackground
                    ) { shareResult in
                        switch shareResult {
                        
                        case .failure(let err):
                            continuation.resume(throwing: err)
                            
                        case .success:
                            if isBackground == false {
                                do {
                                    // Ingest into the graph
                                    try SHKGQuery.ingestShare(
                                        of: globalIdentifier,
                                        from: self.localUser.identifier,
                                        to: recipients.map({ $0.identifier })
                                    )
                                } catch {
                                    log.warning("failed to update the local graph with sharing information")
                                }
                                
                                /// After remote sharing is successful, add `receiver::` rows in local server
                                self.serverProxy.shareAssetLocally(
                                    shareableEncryptedAsset,
                                    asPhotoMessageInThreadId: asPhotoMessageInThreadId,
                                    permissions: permissions
                                ) { _ in
                                    continuation.resume()
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    public func convertUser(
        _ userToConvert: any SHServerUser,
        assetIdsByGroupId: [String: [GlobalIdentifier]],
        threadIds: [String]
    ) async throws {
        for (groupId, assetIds) in assetIdsByGroupId {
            try await self.createGroupEncryptionDetails(for: [userToConvert], in: groupId)
            for globalIdentifier in assetIds {
                try await self.shareAsset(
                    globalIdentifier: globalIdentifier,
                    versions: [.lowResolution, .hiResolution],
                    createdBy: self.localUser,
                    with: [userToConvert],
                    via: groupId,
                    permissions: nil
                )
            }
        }
        
    }
}
