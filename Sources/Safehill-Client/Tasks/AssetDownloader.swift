
import Foundation
import Safehill_Crypto
import KnowledgeBase
import os
import CryptoKit

struct DownloadBlacklist {
    static var shared = DownloadBlacklist()
    
    /// Give up retrying after a download for an asset after this many attempts
    static let Threshold = 6
    
    var repeatedDownloadFailuresByAssetId = [String: Int]()
    
    mutating func recordFailedAttempt(globalIdentifier: String) {
        if repeatedDownloadFailuresByAssetId[globalIdentifier] == nil {
            repeatedDownloadFailuresByAssetId[globalIdentifier] = 1
        } else {
            repeatedDownloadFailuresByAssetId[globalIdentifier]! += 1
        }
    }
    
    mutating func blacklist(globalIdentifier: String) {
        repeatedDownloadFailuresByAssetId[globalIdentifier] = DownloadBlacklist.Threshold
    }
    
    mutating func remove(globalIdentifier: String) {
        repeatedDownloadFailuresByAssetId.removeValue(forKey: globalIdentifier)
    }
    
    func isBlacklisted(globalIdentifier: String) -> Bool {
        return DownloadBlacklist.Threshold == repeatedDownloadFailuresByAssetId[globalIdentifier]
    }
}


public class SHDownloadOperation: SHAbstractBackgroundOperation, SHBackgroundOperationProtocol {
    
    public let log = Logger(subsystem: "com.safehill", category: "BG-DOWNLOAD")
    
    public let hiResFetchQueue = DispatchQueue(label: "com.safehill.SHDownloadOperation.hiResFetch", qos: .background)
    
    public let limit: Int?
    let user: SHLocalUser
    let delegate: SHAssetDownloaderDelegate
    
    public init(user: SHLocalUser,
                delegate: SHAssetDownloaderDelegate,
                limitPerRun limit: Int? = nil) {
        self.user = user
        self.limit = limit
        self.delegate = delegate
    }
    
    public var serverProxy: SHServerProxy {
        SHServerProxy(user: self.user)
    }
    
    public func clone() -> SHBackgroundOperationProtocol {
        SHDownloadOperation(user: self.user,
                            delegate: self.delegate,
                            limitPerRun: self.limit)
    }
    
    private func getUsers(withIdentifiers userIdentifiers: [String]) throws -> [SHServerUser] {
        var error: Error? = nil
        var users = [SHServerUser]()
        let group = DispatchGroup()
        
        group.enter()
        serverProxy.getUsers(
            withIdentifiers: userIdentifiers
        ) { result in
            switch result {
            case .success(let serverUsers):
                users = serverUsers
            case .failure(let err):
                error = err
            }
            group.leave()
        }
        
        let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDefaultNetworkTimeoutInMilliseconds))
        guard dispatchResult == .success else {
            throw SHBackgroundOperationError.timedOut
        }
        guard error == nil else {
            throw error!
        }
        return users
    }
    
    private func decrypt(encryptedAsset: (SHAssetDescriptor, SHEncryptedAsset), quality: SHAssetQuality) throws -> SHDecryptedAsset {
        let descriptor = encryptedAsset.0
        let asset = encryptedAsset.1
        
        var sender: SHServerUser? = nil
        if descriptor.sharingInfo.sharedByUserIdentifier == self.user.identifier {
            sender = self.user
        } else {
            let users = try self.getUsers(withIdentifiers: [descriptor.sharingInfo.sharedByUserIdentifier])
            guard users.count == 1, let serverUser = users.first,
                  serverUser.identifier == descriptor.sharingInfo.sharedByUserIdentifier
            else {
                throw SHBackgroundOperationError.unexpectedData(users)
            }
            sender = serverUser
        }
        
        return try self.user.decrypt(asset, quality: quality, receivedFrom: sender!)
    }
    
    private func fetchRemoteAssets(withGlobalIdentifiers globalIdentifiers: [String],
                                   quality: SHAssetQuality,
                                   request: SHDownloadRequestQueueItem,
                                   completionHandler: @escaping (Result<[String: Error], Error>) -> Void) {
        let start = CFAbsoluteTimeGetCurrent()
        
        var errorsByAssetId = [String: Error]()
        
        log.info("downloading assets with identifiers \(globalIdentifiers) version \(quality.rawValue)")
        serverProxy.getAssets(
            withGlobalIdentifiers: globalIdentifiers,
            versions: [quality],
            saveLocallyWithSenderIdentifier: request.assetDescriptor.sharingInfo.sharedByUserIdentifier
        )
        { result in
            switch result {
            case .success(let assetsDict):
                if assetsDict.count > 0 {
                    for (assetId, asset) in assetsDict {
                        let encryptedAssetAndDescriptor = (request.assetDescriptor, asset)
                        do {
                            let decryptedAssets = try self.decrypt(
                                encryptedAsset: encryptedAssetAndDescriptor,
                                quality: quality
                            )
                            
                            DownloadBlacklist.shared.remove(globalIdentifier: assetId)
                            
                            switch quality {
                            case .lowResolution:
                                self.delegate.handleLowResAsset(decryptedAssets, groupId: request.groupId)
                            case .hiResolution:
                                self.delegate.handleHiResAsset(decryptedAssets, groupId: request.groupId)
                            }
                        }
                        catch {
                            errorsByAssetId[assetId] = error
                            
                            // Record the failure for the asset
                            if case CryptoKitError.authenticationFailure = error {
                                DownloadBlacklist.shared.blacklist(globalIdentifier: assetId)
                            } else {
                                DownloadBlacklist.shared.recordFailedAttempt(globalIdentifier: assetId)
                            }
                            
                            // Call the delegate if the failure has occurred enough times
                            if DownloadBlacklist.shared.isBlacklisted(globalIdentifier: assetId) {
                                self.delegate.unrecoverableDownloadFailure(for: assetId, groupId: request.groupId)
                            }
                        }
                    }
                }
                completionHandler(.success(errorsByAssetId))
            case .failure(let err):
                self.log.critical("Unable to download assets \(globalIdentifiers) version \(SHAssetQuality.hiResolution.rawValue) from server: \(err)")
                completionHandler(.failure(err))
            }
            let end = CFAbsoluteTimeGetCurrent()
            self.log.debug("[PERF] \(CFAbsoluteTime(end - start)) for version \(quality.rawValue)")
        }
    }
    
    public func content(ofQueueItem item: KBQueueItem) throws -> SHSerializableQueueItem {
        guard let data = item.content as? Data else {
            throw KBError.unexpectedData(item.content)
        }
        
        let unarchiver: NSKeyedUnarchiver
        if #available(macOS 10.13, *) {
            unarchiver = try NSKeyedUnarchiver(forReadingFrom: data)
        } else {
            unarchiver = NSKeyedUnarchiver(forReadingWith: data)
        }
        
        guard let downloadRequest = unarchiver.decodeObject(of: SHDownloadRequestQueueItem.self, forKey: NSKeyedArchiveRootObjectKey) else {
            throw KBError.unexpectedData(item)
        }
        
        return downloadRequest
    }
    
    private func downloadDescriptors(completionHandler: @escaping (Swift.Result<Void, Error>) -> Void) {
        /// Fetching assets from the ServerProxy is a 2-step process
        /// 1. Get the descriptors (no data) to determine which assets to pull. This calls the delegate with Assets with empty `encryptedData` (resulting in `downloadInProgress` to be `true`
        /// 2. Get the assets data for the assets not already downloaded (based on the descriptors), and call the delegate with Assets with the low-rez `encryptedData` (resulting in `downloadInProgress` to be `false`)
        serverProxy.getAssetDescriptors { result in
            switch result {
            case .success(let descriptors):
                
                /// Do not download:
                /// - assets that have already been downloaded (are in `existingGlobalIdentifiers`)
                /// - assets that have a corresponding local asset (are in `existingLocalIdentifiers`)
                ///
                let existingGlobalIdentifiers = self.delegate.globalIdentifiersInCache()
                let existingLocalIdentifiers = self.delegate.localIdentifiersInCache()
                
                var globalIdentifiersToDownload = [String]()
                var globalIdentifiersNotReadyForDownload = [String]()
                var descriptorsByLocalIdentifier = [String: SHAssetDescriptor]()
                for descriptor in descriptors {
                    if let localIdentifier = descriptor.localIdentifier,
                       existingLocalIdentifiers.contains(localIdentifier) {
                        descriptorsByLocalIdentifier[localIdentifier] = descriptor
                    } else {
                        guard existingGlobalIdentifiers.contains(descriptor.globalIdentifier) == false else {
                            continue
                        }
                        
                        if descriptor.uploadState == .completed {
                            globalIdentifiersToDownload.append(descriptor.globalIdentifier)
                        } else {
                            globalIdentifiersNotReadyForDownload.append(descriptor.globalIdentifier)
                        }
                    }
                }
                
                if descriptorsByLocalIdentifier.count > 0 {
                    self.delegate.markLocalAssetsAsDownloaded(descriptorsByLocalIdentifier: descriptorsByLocalIdentifier)
                } else {
                    self.delegate.noAssetsToDownload()
                }
                
                if globalIdentifiersToDownload.count == 0 {
                    completionHandler(.success(()))
                    return
                }
                
                self.log.info("found \(descriptors.count) assets on the server. Need to download \(globalIdentifiersToDownload.count)")
                
                let descriptorsForAssetsToDownload = descriptors.filter {
                    globalIdentifiersToDownload.contains($0.globalIdentifier)
                }
                
                let start = CFAbsoluteTimeGetCurrent()
                
                // Fetch users information from server
                // and call the delegate for assets that will be downloaded using Assets with empty data, created based on their descriptor
                
                var userIdentifiers = Set(descriptorsForAssetsToDownload.flatMap { $0.sharingInfo.sharedWithUserIdentifiersInGroup.keys })
                userIdentifiers.formUnion(Set(descriptorsForAssetsToDownload.compactMap { $0.sharingInfo.sharedByUserIdentifier }))
                
                do {
                    let users = try self.getUsers(withIdentifiers: Array(userIdentifiers))
                    self.delegate.handleAssetDescriptorResults(for: descriptorsForAssetsToDownload, users: users)
                } catch {
                    self.log.error("Unable to fetch users from server: \(error.localizedDescription)")
                    completionHandler(.failure(error))
                    return
                }

                // DO NOT call the delegate for assets that won't be downloaded (as they are still being uploaded on the other side)
                
                // Create items in the DownloadQueue, one per descriptor
                for newDescriptor in descriptorsForAssetsToDownload {
                    guard DownloadBlacklist.shared.isBlacklisted(globalIdentifier: newDescriptor.globalIdentifier) == false else {
                        self.log.info("Skipping item \(newDescriptor.globalIdentifier) because it was attempted too many times")
                        continue
                    }

                    let queueItemIdentifier = newDescriptor.globalIdentifier
                    guard let existingItemIdentifiers = try? DownloadQueue.keys(matching: KBGenericCondition(.equal, value: queueItemIdentifier)),
                          existingItemIdentifiers.isEmpty else {
                        self.log.info("Not enqueuing item \(queueItemIdentifier) in the DOWNLOAD queue as a request with the same identifier hasn't been fulfilled yet")
                        continue
                    }
                    
                    let queueItem = SHDownloadRequestQueueItem(
                        assetDescriptor: newDescriptor,
                        receiverUserIdentifier: self.user.identifier
                    )
                    self.log.info("enqueuing item \(queueItemIdentifier) in the DOWNLOAD queue")
                    do {
                        try queueItem.enqueue(in: DownloadQueue, with: queueItemIdentifier)
                    } catch {
                        self.log.error("error enqueueing in the DOWNLOAD queue. \(error.localizedDescription)")
                        continue
                    }
                }
                completionHandler(.success(()))
                
                let end = CFAbsoluteTimeGetCurrent()
                self.log.debug("[PERF] it took \(CFAbsoluteTime(end - start)) to fetch \(descriptorsForAssetsToDownload.count) descriptors and enqueue download requests")
                
            case .failure(let err):
                self.log.error("Unable to download descriptors from server: \(err.localizedDescription)")
                completionHandler(.failure(err))
            }
        }
    }
    
    private func downloadAssets(completionHandler: @escaping (Swift.Result<Void, Error>) -> Void) {
        do {
            var count = 1
            
            while let item = try DownloadQueue.peek() {
                if let limit = limit {
                    guard count <= limit else {
                        break
                    }
                }
                
                let start = CFAbsoluteTimeGetCurrent()
                
                log.info("downloading assets from descriptors in item \(count), with identifier \(item.identifier) created at \(item.createdAt)")
                
                guard let downloadRequest = try? content(ofQueueItem: item) as? SHDownloadRequestQueueItem else {
                    log.error("unexpected data found in DOWNLOAD queue. Dequeueing")
                    
                    do { _ = try DownloadQueue.dequeue() }
                    catch {
                        log.warning("dequeuing failed of unexpected data in DOWNLOAD. ATTENTION: this operation will be attempted again.")
                    }
                    
                    self.delegate.didFailDownloadAttempt(errorsByAssetIdentifier: nil)
                    continue
                }
                
                guard DownloadBlacklist.shared.isBlacklisted(globalIdentifier: downloadRequest.assetDescriptor.globalIdentifier) == false else {
                    self.log.info("Skipping item \(downloadRequest.assetDescriptor.globalIdentifier) because it was attempted too many times")
                    
                    do { _ = try DownloadQueue.dequeue() }
                    catch {
                        log.warning("dequeuing failed of unexpected data in DOWNLOAD. ATTENTION: this operation will be attempted again.")
                    }
                    
                    self.delegate.didFailDownloadAttempt(errorsByAssetIdentifier: nil)
                    continue
                }
                
                let globalIdentifiersToDownload = [downloadRequest.assetDescriptor.globalIdentifier]
                
                // MARK: Start
                
                self.delegate.didStart(globalIdentifier: downloadRequest.assetId, groupId: downloadRequest.groupId)
                
                let group = DispatchGroup()
                var shouldContinue = true
                
                // MARK: Get Low Res asset
                
                group.enter()
                self.fetchRemoteAssets(withGlobalIdentifiers: globalIdentifiersToDownload,
                                       quality: .lowResolution,
                                       request: downloadRequest) { result in
                    switch result {
                    case .success(let errorsByAssetId):
                        if errorsByAssetId.isEmpty == false {
                            self.delegate.didFailDownloadAttempt(errorsByAssetIdentifier: errorsByAssetId)
                        }
                    case .failure(let error):
                        shouldContinue = false
                        self.delegate.didFailDownloadAttempt(errorsByAssetIdentifier: globalIdentifiersToDownload.reduce([:], { partialResult, assetId in
                            var result = partialResult
                            result[assetId] = error
                            return result
                        }))
                    }
                    group.leave()
                }
                
                // MARK: Get Hi Res asset (asynchronously)
                
                self.hiResFetchQueue.async {
                    self.fetchRemoteAssets(withGlobalIdentifiers: globalIdentifiersToDownload,
                                           quality: .hiResolution,
                                           request: downloadRequest) { _ in }
                }
                
                let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDownloadTimeoutInMilliseconds))
                guard dispatchResult == .success, shouldContinue == true else {
                    do { _ = try DownloadQueue.dequeue() }
                    catch {
                        log.warning("dequeuing failed of unexpected data in DOWNLOAD. ATTENTION: this operation will be attempted again.")
                    }
                    
                    continue
                }
                
                let end = CFAbsoluteTimeGetCurrent()
                log.debug("[PERF] it took \(CFAbsoluteTime(end - start)) to download the asset")
                
                do { _ = try DownloadQueue.dequeue() }
                catch {
                    log.warning("asset \(globalIdentifiersToDownload) was downloaded but dequeuing failed, so this operation will be attempted again.")
                }
                
                count += 1
                
                guard !self.isCancelled else {
                    log.info("download task cancelled. Finishing")
                    state = .finished
                    break
                }
            }
            
            completionHandler(.success(()))
            
        } catch {
            log.error("error executing download task: \(error.localizedDescription)")
            completionHandler(.failure(error))
        }
    }
    
    private func runOnce(completionHandler: @escaping (Swift.Result<Void, Error>) -> Void) {
        
        ///
        /// Get all asset descriptors associated with this user from the server.
        /// Descriptors serve as a manifest to determine what to download.
        ///
        self.downloadDescriptors() { result in
            switch result {
            case .failure(let error):
                self.log.error("failed to download descriptors: \(error.localizedDescription)")
                completionHandler(.failure(error))
            case .success():                
                ///
                /// Get all asset descriptors associated with this user from the server.
                /// Descriptors serve as a manifest to determine what to download
                ///
                self.downloadAssets { result in
                    if case .failure(let error) = result {
                        self.log.error("failed to download assets: \(error.localizedDescription)")
                        completionHandler(.failure(error))
                    } else {
                        completionHandler(.success(()))
                    }
                }
            }
        }
    }
    
    public override func main() {
        guard !self.isCancelled else {
            state = .finished
            return
        }
        
        state = .executing
        
        self.runOnce { result in
            self.delegate.downloadOperationFinished(result)
            self.state = .finished
        }
    }
}


public class SHAssetsDownloadQueueProcessor : SHOperationQueueProcessor<SHDownloadOperation> {
    
    public static var shared = SHAssetsDownloadQueueProcessor(
        delayedStartInSeconds: 1,
        dispatchIntervalInSeconds: 8
    )
    private override init(delayedStartInSeconds: Int,
                          dispatchIntervalInSeconds: Int? = nil) {
        super.init(delayedStartInSeconds: delayedStartInSeconds, dispatchIntervalInSeconds: dispatchIntervalInSeconds)
    }
}
