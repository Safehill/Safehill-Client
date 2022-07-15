//
//  AssetDownloader.swift
//  Safehill-Client
//
//  Created by Gennaro Frazzingaro on 9/12/21.
//

import Foundation
import Safehill_Crypto
import KnowledgeBase
import os
import Async

public class SHDownloadOperation: SHAbstractBackgroundOperation, SHBackgroundOperationProtocol {
    
    public let log = Logger(subsystem: "com.safehill", category: "BG-DOWNLOAD")
    
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
    
    private func decrypt(encryptedAssets: [(SHAssetDescriptor, SHEncryptedAsset)], quality: SHAssetQuality) throws -> [SHDecryptedAsset] {
        var decryptedAssets = [SHDecryptedAsset]()
        for (descriptor, asset) in encryptedAssets {
            var sender: SHServerUser? = nil
            if descriptor.sharingInfo.sharedByUserIdentifier == self.user.identifier {
                sender = self.user
            } else {
                var error: Error? = nil
                let group = AsyncGroup()
                
                group.enter()
                serverProxy.getUsers(
                    withIdentifiers: [descriptor.sharingInfo.sharedByUserIdentifier]
                ) { result in
                    switch result {
                    case .success(let serverUsers):
                        guard serverUsers.count == 1,
                              let serverUser = serverUsers.first,
                              serverUser.identifier == descriptor.sharingInfo.sharedByUserIdentifier
                        else {
                            error = SHBackgroundOperationError.unexpectedData(serverUsers)
                            group.leave()
                            return
                        }
                        sender = serverUser
                    case .failure(let err):
                        error = err
                    }
                    group.leave()
                }
                
                let dispatchResult = group.wait()
                guard dispatchResult != .timedOut else {
                    throw SHBackgroundOperationError.timedOut
                }
                guard error == nil else {
                    throw error!
                }
            }
            
            let decryptedAsset = try self.user.decrypt(asset, quality: quality, receivedFrom: sender!)
            decryptedAssets.append(decryptedAsset)
        }
        return decryptedAssets
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
                var descriptorsByLocalIdentifier = [String: SHAssetDescriptor]()
                for descriptor in descriptors {
                    guard existingGlobalIdentifiers.contains(descriptor.globalIdentifier) == false else {
                        continue
                    }
                    
                    if let localIdentifier = descriptor.localIdentifier,
                       existingLocalIdentifiers.contains(localIdentifier) {
                        descriptorsByLocalIdentifier[localIdentifier] = descriptor
                    } else {
                        globalIdentifiersToDownload.append(descriptor.globalIdentifier)
                    }
                }
                
                if descriptorsByLocalIdentifier.count > 0 {
                    self.delegate.markLocalAssetsAsDownloaded(descriptorsByLocalIdentifier: descriptorsByLocalIdentifier)
                }
                
                if globalIdentifiersToDownload.count == 0 {
                    self.log.debug("no assets to download")
                    completionHandler(.success(()))
                    return
                }
                
                self.log.info("found \(descriptors.count) assets on the server. Need to download \(globalIdentifiersToDownload.count)")
                
                let newDescriptors = descriptors.filter {
                    globalIdentifiersToDownload.contains($0.globalIdentifier)
                }
                
                // Call the delegate using Assets with empty data, created based on their descriptor
                self.delegate.handleAssetDescriptorResults(for: newDescriptors)
                
                // Create items in the DownloadQueue, one per descriptor
                for newDescriptor in newDescriptors {
                    let queueItem = SHDownloadRequestQueueItem(
                        assetDescriptor: newDescriptor,
                        receiverUserIdentifier: self.user.identifier
                    )
                    let queueItemIdentifier = newDescriptor.globalIdentifier
                    if let existingItemIdentifiers = try? DownloadQueue.keys(matching: KBGenericCondition(.equal, value: queueItemIdentifier)),
                       existingItemIdentifiers.isEmpty {
                        self.log.info("enqueuing item \(queueItemIdentifier) in the DOWNLOAD queue")
                        do {
                            try queueItem.enqueue(in: DownloadQueue, with: queueItemIdentifier)
                        } catch {
                            completionHandler(.failure(error))
                            return
                        }
                    } else {
                        self.log.info("Not enqueuing item \(queueItemIdentifier) in the DOWNLOAD queue as a request with the same identifier hasn't been fulfilled yet")
                    }
                }
                completionHandler(.success(()))
                
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
                    guard count < limit else {
                        break
                    }
                }
                
                log.info("downloading assets from descriptors in item \(count), with identifier \(item.identifier) created at \(item.createdAt)")
                
                guard let downloadRequest = try? content(ofQueueItem: item) as? SHDownloadRequestQueueItem else {
                    log.error("unexpected data found in DOWNLOAD queue. Dequeueing")
                    
                    do { _ = try DownloadQueue.dequeue() }
                    catch {
                        log.fault("dequeuing failed of unexpected data in DOWNLOAD. ATTENTION: this operation will be attempted again.")
                        throw error
                    }
                    
                    throw KBError.unexpectedData(item.content)
                }
                
                let globalIdentifiersToDownload = [downloadRequest.assetDescriptor.globalIdentifier]
                
                self.delegate.didStartDownload(of: globalIdentifiersToDownload)
                
                var lowResErrorsByAssetId = [String: Error](), hiResErrorsByAssetId = [String: Error]()
                let group = AsyncGroup()
                
                group.enter()
                log.info("downloading assets with identifiers \(globalIdentifiersToDownload) version \(SHAssetQuality.lowResolution.rawValue)")
                serverProxy.getAssets(
                    withGlobalIdentifiers: globalIdentifiersToDownload,
                    versions: [.lowResolution],
                    saveLocallyAsOwnedByUserIdentifier: downloadRequest.assetDescriptor.sharingInfo.sharedByUserIdentifier
                )
                { result in
                    switch result {
                    case .success(let assetsDict):
                        if assetsDict.count > 0 {
                            for (assetId, asset) in assetsDict {
                                let encryptedAssetAndDescriptor = (downloadRequest.assetDescriptor, asset)
                                do {
                                    let decryptedAssets = try self.decrypt(
                                        encryptedAssets: [encryptedAssetAndDescriptor],
                                        quality: .lowResolution
                                    )
                                    
                                    // Call the delegate again using low res Assets, populating the data field this time
                                    self.delegate.handleLowResAssetResults(for: decryptedAssets)
                                } catch {
                                    lowResErrorsByAssetId[assetId] = error
                                    break
                                }
                            }
                        }
                        group.leave()
                    case .failure(let err):
                        print("Unable to download assets \(globalIdentifiersToDownload) version \(SHAssetQuality.lowResolution.rawValue) from server: \(err)")
                        for assetId in globalIdentifiersToDownload {
                            lowResErrorsByAssetId[assetId] = err
                        }
                    }
                }
                
                group.enter()
                log.info("downloading assets with identifiers \(globalIdentifiersToDownload) version \(SHAssetQuality.hiResolution.rawValue)")
                serverProxy.getAssets(
                    withGlobalIdentifiers: globalIdentifiersToDownload,
                    versions: [.hiResolution],
                    saveLocallyAsOwnedByUserIdentifier: downloadRequest.assetDescriptor.sharingInfo.sharedByUserIdentifier
                )
                { result in
                    switch result {
                    case .success(let assetsDict):
                        if assetsDict.count > 0 {
                            for (assetId, asset) in assetsDict {
                                let encryptedAssetAndDescriptor = (downloadRequest.assetDescriptor, asset)
                                do {
                                    let decryptedAssets = try self.decrypt(
                                        encryptedAssets: [encryptedAssetAndDescriptor],
                                        quality: .hiResolution
                                    )
                                    
                                    // Call the delegate again using hi res Assets, populating the data field this time
                                    self.delegate.handleHiResAssetResults(for: decryptedAssets)
                                }
                                catch {
                                    hiResErrorsByAssetId[assetId] = error
                                    break
                                }
                            }
                        }
                        group.leave()
                    case .failure(let err):
                        print("Unable to download assets \(globalIdentifiersToDownload) version \(SHAssetQuality.hiResolution.rawValue) from server: \(err)")
                        for assetId in globalIdentifiersToDownload {
                            hiResErrorsByAssetId[assetId] = err
                        }
                    }
                }
                
                let dispatchResult = group.wait()
                guard dispatchResult != .timedOut else {
                    self.delegate.didFailDownload(of: globalIdentifiersToDownload, errorsByAssetIdentifier: nil)
                    return completionHandler(.failure(SHBackgroundOperationError.timedOut))
                }
                guard lowResErrorsByAssetId.count + hiResErrorsByAssetId.count == 0 else {
                    self.delegate.didFailDownload(of: Array(lowResErrorsByAssetId.keys), errorsByAssetIdentifier: lowResErrorsByAssetId)
                    self.delegate.didFailDownload(of: Array(hiResErrorsByAssetId.keys), errorsByAssetIdentifier: hiResErrorsByAssetId)
                    return completionHandler(.failure(lowResErrorsByAssetId.values.first!))
                }
                
                self.delegate.didCompleteDownload(of: globalIdentifiersToDownload)
                
                do { _ = try DownloadQueue.dequeue() }
                catch {
                    log.warning("asset \(globalIdentifiersToDownload) was downloaded but dequeuing failed, so this operation will be attempted again.")
                    throw error
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
            self.delegate.completionHandler(result)
            self.state = .finished
        }
    }
}


public class SHAssetsDownloadQueueProcessor : SHOperationQueueProcessor<SHDownloadOperation> {
    
    public static var shared = SHAssetsDownloadQueueProcessor(
        delayedStartInSeconds: 2,
        dispatchIntervalInSeconds: 3
    )
    private override init(delayedStartInSeconds: Int,
                          dispatchIntervalInSeconds: Int? = nil) {
        super.init(delayedStartInSeconds: delayedStartInSeconds, dispatchIntervalInSeconds: dispatchIntervalInSeconds)
    }
}
