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

public class SHDownloadOperation: SHAbstractBackgroundOperation, SHBackgroundOperationProtocol {
    
    public let log = Logger(subsystem: "com.safehill", category: "BG-DOWNLOAD")
    
    public let limit: Int?
    let user: SHLocalUser
    let delegate: SHAssetDownloaderDelegate
    
    public init(user: SHLocalUser,
                limitPerRun limit: Int? = nil,
                delegate: SHAssetDownloaderDelegate) {
        self.user = user
        self.limit = limit
        self.delegate = delegate
    }
    
    public var serverProxy: SHServerProxy {
        SHServerProxy(user: self.user)
    }
    
    public func clone() -> SHBackgroundOperationProtocol {
        SHDownloadOperation(user: self.user,
                            limitPerRun: self.limit,
                            delegate: self.delegate)
    }
    
    private func decrypt(encryptedAssets: [(SHAssetDescriptor, SHEncryptedAsset)], quality: SHAssetQuality) throws -> [SHDecryptedAsset] {
        var decryptedAssets = [SHDecryptedAsset]()
        for (descriptor, asset) in encryptedAssets {
            var user: SHServerUser? = nil
            if descriptor.sharedByUserIdentifier == self.user.identifier {
                user = self.user
            } else {
                let dispatch = KBTimedDispatch()
                SHServerProxy(user: self.user).getUsers(
                    withIdentifiers: descriptor.sharedWithUserIdentifiers
                ) { result in
                    switch result {
                    case .success(let serverUsers):
                        if let serverUser = serverUsers.first, serverUser.identifier == descriptor.sharedByUserIdentifier {
                            user = serverUser
                            dispatch.semaphore.signal()
                        }
                    case .failure(let error):
                        dispatch.interrupt(error)
                    }
                }
                try dispatch.wait()
            }
            let decryptedAsset = try self.user.decrypt(asset, quality: quality, receivedFrom: user!)
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
                var localIdentifiersInTheCloud = [String: String]()
                for descriptor in descriptors {
                    guard existingGlobalIdentifiers.contains(descriptor.globalIdentifier) == false else {
                        continue
                    }
                    
                    if let localIdentifier = descriptor.localIdentifier,
                       existingLocalIdentifiers.contains(localIdentifier) {
                        localIdentifiersInTheCloud[localIdentifier] = descriptor.globalIdentifier
                    } else {
                        globalIdentifiersToDownload.append(descriptor.globalIdentifier)
                    }
                }
                
                if localIdentifiersInTheCloud.count > 0 {
                    self.delegate.markLocalAssetsAsDownloaded(localToGlobalIdentifiers: localIdentifiersInTheCloud)
                }
                
                if globalIdentifiersToDownload.count == 0 {
//                    self.log.debug("no assets to download")
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
                    let queueItem = SHDownloadRequestQueueItem(assetDescriptor: newDescriptor)
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
                print("Unable to download descriptors from server: \(err)")
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
                    throw KBError.unexpectedData(item.content)
                }
                
                let globalIdentifiersToDownload = [downloadRequest.assetDescriptor.globalIdentifier]
                
                self.delegate.didStartDownload(of: globalIdentifiersToDownload)
                
                let dispatch = KBTimedDispatch()
                
                dispatch.group.enter()
                log.info("downloading assets with identifiers \(globalIdentifiersToDownload) version \(SHAssetQuality.lowResolution.rawValue)")
                serverProxy.getAssets(withGlobalIdentifiers: globalIdentifiersToDownload,
                                      versions: [.lowResolution]) { result in
                    switch result {
                    case .success(let assetsDict):
                        if assetsDict.count > 0 {
                            for (_, asset) in assetsDict {
                                let encryptedAssetAndDescriptor = (downloadRequest.assetDescriptor, asset)
                                do {
                                    let decryptedAssets = try self.decrypt(
                                        encryptedAssets: [encryptedAssetAndDescriptor],
                                        quality: .lowResolution
                                    )
                                    
                                    // Call the delegate again using low res Assets, populating the data field this time
                                    self.delegate.handleLowResAssetResults(for: decryptedAssets)
                                } catch {
                                    dispatch.interrupt(error)
                                    return
                                }
                            }
                        }
                        dispatch.group.leave()
                    case .failure(let err):
                        print("Unable to download assets \(globalIdentifiersToDownload) version \(SHAssetQuality.lowResolution.rawValue) from server: \(err)")
                        dispatch.interrupt(err)
                    }
                }
                
                dispatch.group.enter()
                log.info("downloading assets with identifiers \(globalIdentifiersToDownload) version \(SHAssetQuality.hiResolution.rawValue)")
                serverProxy.getAssets(withGlobalIdentifiers: globalIdentifiersToDownload,
                                      versions: [.hiResolution]) { result in
                    switch result {
                    case .success(let assetsDict):
                        if assetsDict.count > 0 {
                            for (_, asset) in assetsDict {
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
                                    dispatch.interrupt(error)
                                }
                            }
                        }
                        dispatch.group.leave()
                    case .failure(let err):
                        print("Unable to download assets \(globalIdentifiersToDownload) version \(SHAssetQuality.hiResolution.rawValue) from server: \(err)")
                        dispatch.interrupt(err)
                    }
                }
                
                do {
                    try dispatch.wait()
                    completionHandler(.success(()))
                } catch {
                    completionHandler(.failure(error))
                    return
                }

                count += 1
                
                guard !self.isCancelled else {
                    log.info("encrypt task cancelled. Finishing")
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
    
    public func runOnce(completionHandler: @escaping (Swift.Result<Void, Error>) -> Void) {
        
        ///
        /// Get all asset descriptors associated with this user from the server.
        /// Descriptors serve as a manifest to determine what to download.
        ///
        self.downloadDescriptors() { result in
            if case .failure(let error) = result {
                self.log.error("failed to download descriptors: \(error.localizedDescription)")
                self.delegate.completionHandler(.failure(error))
            } else {
                ///
                /// Get all asset descriptors associated with this user from the server.
                /// Descriptors serve as a manifest to determine what to download
                ///
                self.downloadAssets { result in
                    if case .failure(let error) = result {
                        self.log.error("failed to download assets: \(error.localizedDescription)")
                        self.delegate.completionHandler(.failure(error))
                    } else {
                        self.delegate.completionHandler(.success(()))
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
        
        self.runOnce(completionHandler: self.delegate.completionHandler)
        
        state = .finished
    }
}


public class SHAssetsDownloadQueueProcessor : SHOperationQueueProcessor<SHDownloadOperation> {
    
    public static var shared = SHAssetsDownloadQueueProcessor(
        delayedStartInSeconds: 4,
        dispatchIntervalInSeconds: 3
    )
    private override init(delayedStartInSeconds: Int,
                          dispatchIntervalInSeconds: Int? = nil) {
        super.init(delayedStartInSeconds: delayedStartInSeconds, dispatchIntervalInSeconds: dispatchIntervalInSeconds)
    }
}
