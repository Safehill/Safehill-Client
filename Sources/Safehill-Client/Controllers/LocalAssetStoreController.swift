import Foundation
import Safehill_Crypto

public typealias LocalIdentifier = String


public struct SHLocalAssetStoreController {
    
    private let user: SHLocalUserProtocol
    
    public init(user: SHLocalUserProtocol) {
        self.user = user
    }
    
    private var serverProxy: SHServerProxy {
        self.user.serverProxy
    }
    
    public func encryptedAsset(
        with globalIdentifier: GlobalIdentifier,
        versions: [SHAssetQuality]? = nil,
        synchronousFetch: Bool,
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<any SHEncryptedAsset, Error>) -> Void
    ) {
        self.encryptedAssets(
            with: [globalIdentifier],
            versions: versions,
            synchronousFetch: synchronousFetch,
            qos: qos
        ) { result in
            switch result {
            case .failure(let error):
                completionHandler(.failure(error))
            case .success(let dict):
                guard let asset = dict[globalIdentifier] else {
                    completionHandler(.failure(SHBackgroundOperationError.missingAssetInLocalServer(globalIdentifier)))
                    return
                }
                completionHandler(.success(asset))
            }
        }
    }
    
    public func encryptedAssets(
        with globalIdentifiers: [GlobalIdentifier],
        versions: [SHAssetQuality]? = nil,
        synchronousFetch: Bool,
        qos: DispatchQoS.QoSClass,
        completionHandler: @escaping (Result<[GlobalIdentifier: any SHEncryptedAsset], Error>) -> Void
    ) {
        self.serverProxy.getAssetsAndCache(
            withGlobalIdentifiers: globalIdentifiers,
            versions: versions ?? SHAssetQuality.all,
            synchronousFetch: synchronousFetch,
            completionHandler: completionHandler
        )
    }
    
    private func decryptedAssetInternal(
        encryptedAsset: any SHEncryptedAsset,
        versions: [SHAssetQuality],
        descriptor: any SHAssetDescriptor,
        completionHandler: @escaping (Result<any SHDecryptedAsset, Error>) -> Void
    ) {
        if descriptor.sharingInfo.sharedByUserIdentifier == self.user.identifier {
            do {
                let decryptedAsset = try self.user.decrypt(
                    encryptedAsset,
                    versions: versions,
                    receivedFrom: self.user
                )
                log.debug("[\(type(of: self))] successfully decrypted asset \(decryptedAsset.globalIdentifier)")
                completionHandler(.success(decryptedAsset))
            } catch {
                log.error("[\(type(of: self))] failed decrypting asset \(encryptedAsset.globalIdentifier): \(error.localizedDescription)")
                completionHandler(.failure(error))
            }
        } else {
            SHUsersController(localUser: self.user).getUsers(
                withIdentifiers: [descriptor.sharingInfo.sharedByUserIdentifier]
            ) { result in
                switch result {
                case .failure(let error):
                    log.error("[\(type(of: self))] failed to fetch user \(descriptor.sharingInfo.sharedByUserIdentifier) details for decrypting asset \(encryptedAsset.globalIdentifier): \(error.localizedDescription)")
                    completionHandler(.failure(error))
                case .success(let usersDict):
                    guard usersDict.count == 1, let serverUser = usersDict.values.first,
                          serverUser.identifier == descriptor.sharingInfo.sharedByUserIdentifier
                    else {
                        log.error("[\(type(of: self))] failed to fetch user details for decrypting asset \(encryptedAsset.globalIdentifier). No user \(descriptor.sharingInfo.sharedByUserIdentifier)")
                        completionHandler(.failure(SHBackgroundOperationError.unexpectedData(usersDict)))
                        return
                    }
                    
                    do {
                        let decryptedAsset = try self.user.decrypt(
                            encryptedAsset,
                            versions: versions,
                            receivedFrom: serverUser
                        )
                        log.debug("[\(type(of: self))] successfully decrypted asset \(decryptedAsset.globalIdentifier)")
                        completionHandler(.success(decryptedAsset))
                    } catch {
                        log.error("[\(type(of: self))] failed decrypting asset \(encryptedAsset.globalIdentifier): \(error.localizedDescription)")
                        completionHandler(.failure(error))
                    }
                }
            }
        }
    }
    
    /// Decrypt an asset version (quality) given its encrypted counterpart and its descriptor
    /// ```
    /// let localAssetsStore = SHLocalAssetStoreController(user: self.user)
    ///
    /// do {
    ///     let encryptedAsset = try localAssetsStore.encryptedAsset(
    ///         with: globalIdentifier,
    ///         filteringVersions: [quality]
    ///     )
    ///     let decryptedAsset = try localAssetsStore.decryptedAsset(
    ///         encryptedAsset: encryptedAsset,
    ///         descriptor: request.assetDescriptor,
    ///         quality: quality
    ///     )
    /// } catch {
    ///    // Handle
    /// }
    /// ```
    /// - Parameters:
    ///   - encryptedAsset: the encrypted asset
    ///   - descriptor: the asset descriptor. If none is provided, it's fetched from the local server
    ///   - versions: the versions requested
    /// - Returns: the decrypted asset
    ///
    public func decryptedAsset(
        encryptedAsset: any SHEncryptedAsset,
        versions: [SHAssetQuality],
        descriptor: (any SHAssetDescriptor)? = nil,
        completionHandler: @escaping (Result<any SHDecryptedAsset, Error>) -> Void
    ) {
        if let descriptor {
            self.decryptedAssetInternal(
                encryptedAsset: encryptedAsset,
                versions: versions,
                descriptor: descriptor,
                completionHandler: completionHandler
            )
        } else {
            self.serverProxy.getAssetDescriptor(
                for: encryptedAsset.globalIdentifier
            ) { result in
                switch result {
                
                case .success(let descriptor):
                    guard let foundDescriptor = descriptor else {
                        completionHandler(.failure(SHAssetStoreError.noEntries))
                        return
                    }
                    self.decryptedAssetInternal(
                        encryptedAsset: encryptedAsset,
                        versions: versions,
                        descriptor: foundDescriptor,
                        completionHandler: completionHandler
                    )
                    
                case .failure(let error):
                    completionHandler(.failure(error))
                }
            }
        }
    }
}


extension SHLocalAssetStoreController {
    ///
    /// Get the private secret for the asset.
    /// The same secret should be used when encrypting for sharing with other users.
    /// Encrypting that secret again with the recipient's public key guarantees the privacy of that secret.
    ///
    /// By the time this method is called, the encrypted secret should be present in the local server,
    /// as the asset was previously encrypted on this device by the SHEncryptionOperation.
    ///
    /// Note that secrets are encrypted at rest, wheres the in-memory data is its decrypted (clear) version.
    ///
    /// - Returns: the decrypted shared secret for this asset
    /// - Throws: SHBackgroundOperationError if the shared secret couldn't be retrieved, other errors if the asset couldn't be retrieved from the Photos library
    ///
    func retrieveCommonEncryptionKey(
        for globalIdentifier: String,
        completionHandler: @escaping (Result<Data, Error>) -> Void
    ) {
        guard let encryptionProtocolSalt = self.user.maybeEncryptionProtocolSalt else {
            completionHandler(.failure(SHLocalUserError.missingProtocolSalt))
            return
        }
        
        /// Common encryption key (private secret) is constant across all versions. Any SHAssetQuality will return the same value
        let quality = SHAssetQuality.lowResolution
        
        self.encryptedAsset(
            with: globalIdentifier,
            versions: [quality],
            synchronousFetch: true,
            qos: .default
        ) { result in
            switch result {
            case .failure(let error):
                completionHandler(.failure(error))
            case .success(let encryptedAsset):
                guard let version = encryptedAsset.encryptedVersions[quality] else {
                    log.error("failed to retrieve shared secret for asset \(globalIdentifier)")
                    completionHandler(.failure(SHBackgroundOperationError.missingAssetInLocalServer(globalIdentifier)))
                    return
                }
                
                let encryptedSecret = SHShareablePayload(
                    ephemeralPublicKeyData: version.publicKeyData,
                    cyphertext: version.encryptedSecret,
                    signature: version.publicSignatureData
                )
                
                do {
                    let encryptionKey = try SHUserContext(user: self.user.shUser).decryptSecret(
                        usingEncryptedSecret: encryptedSecret,
                        protocolSalt: encryptionProtocolSalt,
                        signedWith: self.user.publicSignatureData
                    )
                    
                    completionHandler(.success(encryptionKey))
                } catch {
                    completionHandler(.failure(error))
                }
            }
        }
    }
}
