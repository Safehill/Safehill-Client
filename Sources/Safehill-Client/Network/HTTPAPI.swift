import Foundation
import KnowledgeBase
import Safehill_Crypto
import CryptoKit

public class SHNetwork {
    public static let shared = SHNetwork()
    
    private var _bytesPerSecond: Double = 0.0
    
    public var speedInBytesPerSecond: Double {
        self._bytesPerSecond
    }
    
    internal func setSpeed(bytesPerSecond: Double) {
        self._bytesPerSecond = bytesPerSecond
    }
}

public let SHDefaultNetworkTimeoutInMilliseconds = 30000 // 30 seconds
public let SHUploadTimeoutInMilliseconds = 300000 // 5 minutes
public let SHDownloadTimeoutInMilliseconds = 300000 // 5 minutes

extension ISO8601DateFormatter {
    convenience init(_ formatOptions: Options) {
        self.init()
        self.formatOptions = formatOptions
    }
}
extension Formatter {
    static let iso8601withFractionalSeconds = ISO8601DateFormatter([.withInternetDateTime, .withFractionalSeconds])
}
extension Date {
    var iso8601withFractionalSeconds: String { return Formatter.iso8601withFractionalSeconds.string(from: self) }
}
extension String {
    var iso8601withFractionalSeconds: Date? { return Formatter.iso8601withFractionalSeconds.date(from: self) }
}

struct NoReply: Decodable {}

struct GenericSuccessResponse: Decodable {
    let status: String
}

struct GenericFailureResponse: Decodable {
    let error: Bool
    let reason: String?
}

struct SHServerHTTPAPI : SHServerAPI {
    
    let requestor: SHLocalUser
    
    init(requestor: SHLocalUser) {
        self.requestor = requestor
    }
    
    func requestURL(route: String, urlParameters: [String: String]? = nil) -> URL {
        var components = SafehillServerURLComponents

        components.path = "/\(route)"
        var queryItems = [URLQueryItem]()
        
        // URL parameters
        if let keysAndValues = urlParameters {
            for (paramKey, paramValue) in keysAndValues {
                queryItems.append(URLQueryItem(name: paramKey, value: paramValue))
            }
        }
        components.queryItems = queryItems
        
        components.percentEncodedQuery = components.percentEncodedQuery?.replacingOccurrences(of: "+", with: "%2B")
        
        return components.url!
    }
    
    static func makeRequest<T: Decodable>(request: URLRequest,
                                          decodingResponseAs type: T.Type,
                                          completionHandler: @escaping (Result<T, Error>) -> Void) {
//        log.trace("""
//"\(request.httpMethod!) \(request.url!),
//headers=\(request.allHTTPHeaderFields ?? [:]),
//body=\(request.httpBody != nil ? String(data: request.httpBody!, encoding: .utf8) ?? "some" : "nil")
//""")
        
        let startTime = CFAbsoluteTimeGetCurrent()
        var stopTime = startTime
        var bytesReceived = 0
        
        URLSession(configuration: SafehillServerDefaultURLSessionConfiguration).dataTask(with: request) { data, response, error in
            
            guard error == nil else {
                if let err = error as? URLError {
                    // Not connected to the internet, handle it differently
                    completionHandler(.failure(err))
                } else {
                    completionHandler(.failure(SHHTTPError.TransportError.generic(error!)))
                }
                return
            }
            
            bytesReceived = data?.count ?? 0
            if bytesReceived > 0 {
                stopTime = CFAbsoluteTimeGetCurrent()
                let elapsed = stopTime - startTime
                if elapsed > 0 {
                    SHNetwork.shared.setSpeed(bytesPerSecond: Double(bytesReceived) / elapsed)
                }
            }
            
            let httpResponse = response as! HTTPURLResponse
            if httpResponse.statusCode < 200 || httpResponse.statusCode > 299 {
                log.debug("request \(request.url!) received \(httpResponse.statusCode) response")
                if let data = data {
                    let convertedString = String(data: data, encoding: String.Encoding.utf8)
                    log.debug("response body: \(convertedString ?? "")")
                }
            }
            
            switch httpResponse.statusCode {
            case 200..<300:
                break
            case 401:
                completionHandler(.failure(SHHTTPError.ClientError.unauthorized))
                return
            case 402:
                completionHandler(.failure(SHHTTPError.ClientError.paymentRequired))
                return
            case 404:
                completionHandler(.failure(SHHTTPError.ClientError.notFound))
                return
            case 405:
                completionHandler(.failure(SHHTTPError.ClientError.methodNotAllowed))
                return
            case 409:
                completionHandler(.failure(SHHTTPError.ClientError.conflict))
                return
            case 501:
                completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
            case 503:
                completionHandler(.failure(SHHTTPError.ServerError.badGateway))
                return
            case 400..<500:
                var message = "Bad or malformed request"
                if let data = data,
                   let decoded = try? JSONDecoder().decode(GenericFailureResponse.self, from: data),
                   let reason = decoded.reason {
                    message = reason
                }
                completionHandler(.failure(SHHTTPError.ClientError.badRequest(message)))
                return
            default:
                var message = "A server error occurred"
                if let data = data,
                   let decoded = try? JSONDecoder().decode(GenericFailureResponse.self, from: data),
                   let reason = decoded.reason {
                    message = reason
                }
                log.error("\(request.url!.absoluteString) failed with code \(httpResponse.statusCode): \(message)")
                completionHandler(.failure(SHHTTPError.ServerError.generic(message)))
                return
            }
            
            guard let data = data else {
                completionHandler(.failure(SHHTTPError.ServerError.noData))
                return
            }
            
            if type is NoReply.Type {
                completionHandler(.success(NoReply() as! T))
                return
            } else if type is Data.Type {
                completionHandler(.success(data as! T))
                return
            }
            
            do {
                let decoded = try JSONDecoder().decode(type, from: data)
                completionHandler(.success(decoded))
                return
            }
            catch {
                // there's a decoding error, call completion with decoding error
                completionHandler(.failure(error))
                return
            }
            
        }.resume()
    }
    
    func get<T: Decodable>(_ route: String,
                           parameters: [String: String]?,
                           requiresAuthentication: Bool = true,
                           completionHandler: @escaping (Result<T, Error>) -> Void) {
        let url = requestURL(route: route, urlParameters: parameters)
        
        var request = URLRequest(url: url, timeoutInterval: 30)
        request.httpMethod = "GET"
        
        if requiresAuthentication {
            guard let bearerToken = self.requestor.authToken else {
                completionHandler(.failure(SHHTTPError.ClientError.unauthorized))
                return
            }
            request.addValue("Bearer \(bearerToken)", forHTTPHeaderField: "Authorization")
        }
        
        SHServerHTTPAPI.makeRequest(request: request, decodingResponseAs: T.self, completionHandler: completionHandler)
    }
    
    private func route<T: Decodable>(_ route: String,
                                     method: String,
                                     parameters: [String: Any?]?,
                                     requiresAuthentication: Bool,
                                     completionHandler: @escaping (Result<T, Error>) -> Void) {
        let url = requestURL(route: route)
        
        var request = URLRequest(url: url, timeoutInterval: 90)
        request.httpMethod = method
        request.addValue("application/json", forHTTPHeaderField: "Content-Type")
        
        if requiresAuthentication {
            guard let bearerToken = self.requestor.authToken else {
                completionHandler(.failure(SHHTTPError.ClientError.unauthorized))
                return
            }
            request.addValue("Bearer \(bearerToken)", forHTTPHeaderField: "Authorization")
        }
        
        if let parameters = parameters {
            do {
                request.httpBody = try JSONSerialization.data(withJSONObject: parameters, options: .prettyPrinted)
            } catch {
                return completionHandler(.failure(error))
            }
        }
        
        SHServerHTTPAPI.makeRequest(request: request, decodingResponseAs: T.self, completionHandler: completionHandler)
    }
    
    func post<T: Decodable>(_ route: String,
                            parameters: [String: Any?]?,
                            requiresAuthentication: Bool = true,
                            completionHandler: @escaping (Result<T, Error>) -> Void) {
        self.route(
            route,
            method: "POST",
            parameters: parameters,
            requiresAuthentication: requiresAuthentication,
            completionHandler: completionHandler
        )
    }
    
    func delete<T: Decodable>(_ route: String,
                            parameters: [String: Any?]?,
                            requiresAuthentication: Bool = true,
                            completionHandler: @escaping (Result<T, Error>) -> Void) {
        self.route(
            route,
            method: "DELETE",
            parameters: parameters,
            requiresAuthentication: requiresAuthentication,
            completionHandler: completionHandler
        )
    }
    
    func createUser(name: String,
                    completionHandler: @escaping (Result<SHServerUser, Error>) -> ()) {
        let parameters = [
            "identifier": requestor.identifier,
            "publicKey": requestor.publicKeyData.base64EncodedString(),
            "publicSignature": requestor.publicSignatureData.base64EncodedString(),
            "name": name
        ] as [String : Any]
        self.post("users/create", parameters: parameters, requiresAuthentication: false) { (result: Result<SHRemoteUser, Error>) in
            switch result {
            case .success(let user):
                return completionHandler(.success(user))
            case .failure(let error):
                return completionHandler(.failure(error))
            }
        }
    }
    
    func sendCodeToUser(countryCode: Int,
                        phoneNumber: Int,
                        code: String,
                        medium: SendCodeToUserRequestDTO.Medium,
                        completionHandler: @escaping (Result<Void, Error>) -> ()) {
        let parameters = [
            "countryCode": countryCode,
            "phoneNumber": phoneNumber,
            "code": code,
            "medium": medium.rawValue
        ] as [String : Any]
        self.post("users/code/send", parameters: parameters, requiresAuthentication: true) { (result: Result<NoReply, Error>) in
            switch result {
            case .success(_):
                completionHandler(.success(()))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    func updateUser(name: String?,
                    phoneNumber: String? = nil,
                    email: String? = nil,
                    completionHandler: @escaping (Result<SHServerUser, Error>) -> ()) {
        guard email != nil || name != nil || phoneNumber != nil else {
            completionHandler(.failure(SHHTTPError.ClientError.badRequest("Invalid parameters")))
            return
        }
        var parameters = [String : Any]()
        if let email = email {
            parameters["email"] = email
        }
        if let name = name {
            parameters["name"] = name
        }
        if let phoneNumber = phoneNumber {
            parameters["phoneNumber"] = phoneNumber
        }
        self.post("users/update", parameters: parameters, requiresAuthentication: true) { (result: Result<SHRemoteUser, Error>) in
            switch result {
            case .success(let user):
                return completionHandler(.success(user))
            case .failure(let error):
                return completionHandler(.failure(error))
            }
        }
    }
    
    func deleteAccount(completionHandler: @escaping (Result<Void, Error>) -> ()) {
        self.post("users/safe_delete", parameters: nil, requiresAuthentication: true) { (result: Result<NoReply, Error>) in
            switch result {
            case .success(_):
                return completionHandler(.success(()))
            case .failure(let error):
                if case SHHTTPError.ClientError.notFound = error {
                    // If server can't find the user, it was already deleted
                    return completionHandler(.success(()))
                }
                return completionHandler(.failure(error))
            }
        }
    }
    
    func deleteAccount(name: String, password: String, completionHandler: @escaping (Result<Void, Error>) -> ()) {
        self.post("users/delete", parameters: [
            "name": name,
            "password": password
        ], requiresAuthentication: false) { (result: Result<NoReply, Error>) in
            switch result {
            case .success(_):
                return completionHandler(.success(()))
            case .failure(let error):
                if case SHHTTPError.ClientError.notFound = error {
                    // If server can't find the user, it was already deleted
                    return completionHandler(.success(()))
                }
                return completionHandler(.failure(error))
            }
        }
    }
    
    func signIn(clientBuild: Int?, completionHandler: @escaping (Result<SHAuthResponse, Error>) -> ()) {
        var parameters: [String: Any] = [
            "identifier": self.requestor.identifier
        ]
        if let clientBuild = clientBuild {
            parameters["clientBuild"] = clientBuild
        }
        self.post("signin/challenge/start", parameters: parameters, requiresAuthentication: false) {
            (result: Result<SHAuthChallenge, Error>) in
            switch result {
            case .success(let authChallenge):
                
                // Initialize the server's SHRemoteCryptoUser
                // This will fail if the server sends invalid key/signature values
                // Since this is not supposed to happen unless the server is corrupted
                // don't retry
                guard let serverCrypto = try? SHRemoteCryptoUser(
                    publicKeyData: Data(base64Encoded: authChallenge.publicKey)!,
                    publicSignatureData: Data(base64Encoded: authChallenge.publicSignature)!
                ),
                      let authSalt = Data(base64Encoded: authChallenge.protocolSalt)
                else {
                    log.error("[auth] failed to decode challenge parameters")
                    return completionHandler(.failure(SHHTTPError.ServerError.unexpectedResponse("publicKey=\(authChallenge.publicKey) publicSignature=\(authChallenge.publicSignature) salt=\(authChallenge.protocolSalt)")))
                }
                
                let encryptedChallenge = SHShareablePayload(
                    ephemeralPublicKeyData: Data(base64Encoded: authChallenge.ephemeralPublicKey)!,
                    cyphertext: Data(base64Encoded: authChallenge.challenge)!,
                    signature: Data(base64Encoded: authChallenge.ephemeralPublicSignature)!
                )
                
                do {
                    let decryptedChallenge = try SHCypher.decrypt(
                        encryptedChallenge,
                        encryptionKeyData: self.requestor.shUser.privateKeyData,
                        protocolSalt: authSalt,
                        from: serverCrypto.publicSignatureData
                    )
                    let signatureForData = try self.requestor.shUser.signature(for: decryptedChallenge)
                    let digest512 = Data(SHA512.hash(data: decryptedChallenge))
                    let signatureForDigest = try self.requestor.shUser.signature(for: digest512)
                    let parameters = [
                        "userIdentifier": self.requestor.identifier,
                        "signedChallenge": signatureForData.derRepresentation.base64EncodedString(),
                        "digest": digest512.base64EncodedString(),
                        "signedDigest": signatureForDigest.derRepresentation.base64EncodedString()
                    ]
                    self.post("signin/challenge/verify", parameters: parameters, requiresAuthentication: false) {
                        (result: Result<SHAuthResponse, Error>) in
                        if case .failure(let error) = result {
                            log.error("[auth] failed to get verify auth challenge \(error.localizedDescription)")
                        }
                        completionHandler(result)
                    }
                }
                catch {
                    log.error("[auth] failed solve the auth challenge \(error.localizedDescription)")
                    completionHandler(.failure(error))
                }
            case .failure(let err):
                log.error("[auth] failed to get a new auth challenge from the server \(err.localizedDescription)")
                completionHandler(.failure(err))
            }
        }
    }

    func getUsers(withIdentifiers userIdentifiers: [String]?, completionHandler: @escaping (Result<[SHServerUser], Error>) -> ()) {
        let parameters = [
            "userIdentifiers": userIdentifiers ?? []
        ] as [String : Any]
        self.post("users/retrieve", parameters: parameters) { (result: Result<[SHRemoteUser], Error>) in
            switch result {
            case .success(let users):
                return completionHandler(.success(users))
            case .failure(let error):
                return completionHandler(.failure(error))
            }
        }
    }
    
    func searchUsers(query: String, completionHandler: @escaping (Result<[SHServerUser], Error>) -> ()) {
        let parameters = [
            "query": query,
            "page": "1",
            "per": "5"
        ]
        self.get("users/search", parameters: parameters) { (result: Result<SHPaginatedUserSearchResults, Error>) in
            switch result {
            case .success(let searchResult):
                return completionHandler(.success(searchResult.items))
            case .failure(let error):
                return completionHandler(.failure(error))
            }
        }
    }
    
    func registerDevice(_ deviceName: String, token: String, completionHandler: @escaping (Result<Void, Error>) -> ()) {
        let parameters = [
            "deviceId": deviceName,
            "token": token
        ]
        self.post("/users/devices/register", parameters: parameters) { (result: Result<NoReply, Error>) in
            switch result {
            case .success(_):
                completionHandler(.success(()))
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }

    func getAssetDescriptors(forAssetGlobalIdentifiers: [GlobalIdentifier]? = nil,
                             completionHandler: @escaping (Swift.Result<[SHAssetDescriptor], Error>) -> ()) {
        let parameters = forAssetGlobalIdentifiers != nil
        ? ["globalIdentifiers": forAssetGlobalIdentifiers]
        : nil
        self.post("assets/descriptors/retrieve", parameters: parameters) { (result: Result<[SHGenericAssetDescriptor], Error>) in
            switch result {
            case .success(let descriptors):
                completionHandler(.success(descriptors))
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    /// Fetches the assets metadata from the server, then fetches their data from S3
    /// - Parameters:
    ///   - assetIdentifiers: the asset global identifiers
    ///   - versions: optional version filtering. If nil, all versions are retrieved
    ///   - completionHandler: the callback method
    func getAssets(withGlobalIdentifiers assetIdentifiers: [String],
                   versions: [SHAssetQuality]? = nil,
                   completionHandler: @escaping (Swift.Result<[GlobalIdentifier: SHEncryptedAsset], Error>) -> ()) {
        var parameters = [
            "globalIdentifiers": assetIdentifiers,
        ] as [String : Any]
        if let versions = versions {
            parameters["versionNames"] = versions.map { $0.rawValue }
        }
        self.post("assets/retrieve", parameters: parameters) { (result: Result<[SHServerAsset], Error>) in
            switch result {
            case .success(let assets):
                let manifest = ThreadSafeAssetsDict()
                let errors = ThreadSafeS3Errors()
                
                let group = DispatchGroup()
                
                for asset in assets {
                    for version in asset.versions {
                        group.enter()
                        log.info("retrieving asset \(asset.globalIdentifier) version \(version.versionName)")
                        S3Proxy.retrieve(asset, version) { result in
                            switch result {
                            case .success(let encryptedAsset):
                                manifest.add(encryptedAsset)
                            case .failure(let err):
                                errors.set(err, forKey: asset.globalIdentifier + "::" + version.versionName)
                            }
                            group.leave()
                        }
                    }
                }
                
                let allAssetsVersionsCount = assets.reduce(0, { partialResult, asset in partialResult + asset.versions.count })
                let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHDownloadTimeoutInMilliseconds * allAssetsVersionsCount))
                guard dispatchResult == .success else {
                    return completionHandler(.failure(SHHTTPError.TransportError.timedOut))
                }
                
                let errorsDict = errors.toDict()
                guard errorsDict.count == 0 else {
                    return completionHandler(.failure(SHHTTPError.ServerError.generic("Error downloading from S3 asset identifiers \(errorsDict)")))
                }
                completionHandler(.success(manifest.dictionary))
                
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }

    func create(assets: [any SHEncryptedAsset],
                groupId: String,
                filterVersions: [SHAssetQuality]?,
                completionHandler: @escaping (Result<[SHServerAsset], Error>) -> ()) {
        guard assets.count == 1, let asset = assets.first else {
            completionHandler(.failure(SHHTTPError.ClientError.badRequest("Current API only supports creating one asset per request")))
            return
        }
        
        var assetCreationDate: Date
        if asset.creationDate == nil {
            log.warning("No asset creation date. Assuming 1/1/1970")
            assetCreationDate = Date.init(timeIntervalSince1970: 0)
        } else {
            assetCreationDate = asset.creationDate!
        }
        
        var assetVersions = [[String: Any]]()
        for encryptedVersion in asset.encryptedVersions.values {
            guard filterVersions == nil || filterVersions!.contains(encryptedVersion.quality) else {
                continue
            }
            assetVersions.append([
                "versionName": encryptedVersion.quality.rawValue,
                "senderEncryptedSecret": encryptedVersion.encryptedSecret.base64EncodedString(),
                "ephemeralPublicKey": encryptedVersion.publicKeyData.base64EncodedString(),
                "publicSignature": encryptedVersion.publicSignatureData.base64EncodedString()
            ])
        }
        let createDict: [String: Any?] = [
            "globalIdentifier": asset.globalIdentifier,
            "localIdentifier": asset.localIdentifier,
            "creationDate": assetCreationDate.iso8601withFractionalSeconds,
            "groupId": groupId,
            "versions": assetVersions,
            "forceUpdateVersions": true
        ]
        
        self.post("assets/create", parameters: createDict) { (result: Result<SHServerAsset, Error>) in
            switch result {
            case .failure(let error):
                completionHandler(.failure(error))
            case .success(let asset):
                completionHandler(.success([asset]))
            }
        }
    }
    
    func share(asset: SHShareableEncryptedAsset,
               completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        
        if asset.sharedVersions.count == 0 {
            log.warning("no versions specified in sharing. Skipping")
            completionHandler(.success(()))
            return
        }
        
        var versions = [[String: Any?]]()
        for version in asset.sharedVersions {
            versions.append([
                "versionName": version.quality.rawValue,
                "recipientUserIdentifier": version.userPublicIdentifier,
                "recipientEncryptedSecret": version.encryptedSecret.base64EncodedString(),
                "ephemeralPublicKey": version.ephemeralPublicKey.base64EncodedString(),
                "publicSignature": version.publicSignature.base64EncodedString()
            ])
        }
        
        let shareDict: [String: Any?] = [
            "globalAssetIdentifier": asset.globalIdentifier,
            "versionSharingDetails": versions,
            "groupId": asset.groupId
        ]
        
        self.post("assets/share", parameters: shareDict) { (result: Result<NoReply, Error>) in
            switch result {
            case .success(_):
                completionHandler(.success(()))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    func unshare(assetId: GlobalIdentifier, with userPublicIdentifier: String, completionHandler: @escaping (Result<Void, Error>) -> ()) {
        completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
    }
    
    func markAsset(with assetGlobalIdentifier: String,
                   quality: SHAssetQuality,
                   as: SHAssetDescriptorUploadState,
                   completionHandler: @escaping (Result<Void, Error>) -> ()) {
        guard `as` == .completed else {
            completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
            return
        }
        
        self.markAsUploaded(assetGlobalIdentifier,
                            quality: quality,
                            retryCount: 1,
                            completionHandler: completionHandler)
    }
    
    func markAsUploaded(_ assetGlobalIdentifier: String,
                        quality: SHAssetQuality,
                        retryCount: Int,
                        completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        self.post("assets/\(assetGlobalIdentifier)/versions/\(quality.rawValue)/uploaded", parameters: nil)
        { (result: Result<NoReply, Error>) in
            switch result {
            case .success(_):
                completionHandler(.success(()))
            case .failure(let err):
                log.warning("(attempt \(retryCount)/3) failed to mark asset \(assetGlobalIdentifier) version \(quality.rawValue) as uploaded")
                guard retryCount <= 3 else {
                    return completionHandler(.failure(err))
                }
                self.markAsUploaded(assetGlobalIdentifier,
                                    quality: quality,
                                    retryCount: retryCount + 1,
                                    completionHandler: completionHandler)
            }
        }
    }
    
    func upload(serverAsset: SHServerAsset,
                asset: any SHEncryptedAsset,
                filterVersions: [SHAssetQuality]?,
                completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        let writeQueue = DispatchQueue(label: "upload.\(asset.globalIdentifier)", attributes: .concurrent)
        var results = [SHAssetQuality: Swift.Result<Void, Error>]()
        
        for encryptedAssetVersion in asset.encryptedVersions.values {
            guard filterVersions == nil || filterVersions!.contains(encryptedAssetVersion.quality) else {
                continue
            }
            
            log.info("uploading to CDN asset version \(encryptedAssetVersion.quality.rawValue) for asset \(asset.globalIdentifier) (localId=\(asset.localIdentifier ?? ""))")
            
            let serverAssetVersion = serverAsset.versions.first { sav in
                sav.versionName == encryptedAssetVersion.quality.rawValue
            }
            guard let serverAssetVersion = serverAssetVersion else {
                results[encryptedAssetVersion.quality] = .failure(SHHTTPError.ClientError.badRequest("invalid upload payload. Mismatched local and server asset versions. server=\(serverAsset), local=\(asset)"))
                break
            }
            
            guard let url = URL(string: serverAssetVersion.presignedURL) else {
                results[encryptedAssetVersion.quality] = .failure(SHHTTPError.ServerError.unexpectedResponse("presigned URL is invalid"))
                break
            }
            
            S3Proxy.saveInBackground(
                encryptedAssetVersion.encryptedData,
                usingPresignedURL: url,
                sessionIdentifier: [
                    self.requestor.shUser.identifier,
                    serverAsset.globalIdentifier,
                    serverAssetVersion.versionName
                ].joined(separator: "::")
            ) {
                result in
                if case .success(_) = result {
                    self.markAsset(with: asset.globalIdentifier,
                                   quality: encryptedAssetVersion.quality,
                                   as: .completed) { _ in
                    }
                }
            }
            
            results[encryptedAssetVersion.quality] = .success(())
        }
        
        writeQueue.sync {
            for (version, result) in results {
                switch result {
                case .failure(_):
                    log.error("Could not upload asset=\(asset.globalIdentifier) version=\(version.rawValue)")
                    return completionHandler(result)
                default:
                    continue
                }
            }
            completionHandler(.success(()))
        }
    }

    func deleteAssets(withGlobalIdentifiers globalIdentifiers: [String], completionHandler: @escaping (Result<[String], Error>) -> ()) {
        let parameters = [
            "globalIdentifiers": globalIdentifiers
        ] as [String : Any]
        self.post("assets/delete", parameters: parameters) { (result: Result<NoReply, Error>) in
            switch result {
            case .success(_):
                completionHandler(.success(globalIdentifiers))
            case .failure(let error):
                log.error("asset deletion failed. Error: \(error.localizedDescription)")
                completionHandler(.failure(error))
            }
        }
    }

    
    func validateTransaction(
        originalTransactionId: String,
        receipt: String,
        productId: String,
        completionHandler: @escaping (Result<SHReceiptValidationResponse, Error>) -> ()
    ) {
        let parameters = [
            "originalTransactionId": originalTransactionId,
            "receipt": receipt,
            "productId": productId
        ] as [String : Any]
        self.post("purchases/apple/subscription", parameters: parameters) { (result: Result<SHReceiptValidationResponse, Error>) in
            switch result {
            case .success(let response):
                completionHandler(.success(response))
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    func setGroupEncryptionDetails(
        groupId: String,
        recipientsEncryptionDetails: [RecipientEncryptionDetailsDTO],
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        let parameters = [
            "recipients": recipientsEncryptionDetails.map({ encryptionDetails in
                return [
                    "encryptedSecret": encryptionDetails.encryptedSecret,
                    "ephemeralPublicKey": encryptionDetails.ephemeralPublicKey,
                    "secretPublicSignature": encryptionDetails.secretPublicSignature,
                    "userIdentifier": encryptionDetails.userIdentifier
                ]
            }),
            "overwrite": false
        ] as [String: Any]
        
        self.post("groups/\(groupId)/setup", parameters: parameters, requiresAuthentication: true) { (result: Result<NoReply, Error>) in
            switch result {
            case .success(_):
                completionHandler(.success(()))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    func deleteGroup(
        groupId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        self.delete("groups/\(groupId)", parameters: nil, requiresAuthentication: true) { (result: Result<NoReply, Error>) in
            switch result {
            case .success(_):
                completionHandler(.success(()))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    func retrieveGroupUserEncryptionDetails(
        forGroup groupId: String,
        completionHandler: @escaping (Result<[RecipientEncryptionDetailsDTO], Error>) -> ()
    ) {
        self.get("groups/\(groupId)/encryptionDetails", parameters: nil, requiresAuthentication: true) { (result: Result<[RecipientEncryptionDetailsDTO], Error>) in
            switch result {
            case .failure(let error as SHHTTPError.ClientError):
                switch error {
                case .unauthorized:
                    completionHandler(.success([]))
                default:
                    completionHandler(.failure(error))
                }
            case .failure(let error):
                completionHandler(.failure(error))
            case .success(let encryptionDetails):
                completionHandler(.success(encryptionDetails))
            }
        }
    }
    
    func addReactions(
        _ reactions: [ReactionInput],
        toGroupId groupId: String,
        completionHandler: @escaping (Result<[ReactionOutputDTO], Error>) -> ()
    ) {
        guard reactions.count == 1,
              let reaction = reactions.first else {
            completionHandler(.failure(SHHTTPError.ServerError.notImplemented))
            return
        }
        var parameters = [
            "reactionType": reaction.reactionType.rawValue,
        ] as [String: Any]
        
        if let aGid = reaction.inReplyToAssetGlobalIdentifier {
            parameters["inReplyToAssetGlobalIdentifier"] = aGid
        }
        if let iId = reaction.inReplyToInteractionId {
            parameters["inReplyToAssetGlobalIdentifier"] = iId
        }
        
        self.post("interactions/reactions/\(groupId)",
                    parameters: parameters) { (result: Result<ReactionOutputDTO, Error>) in
            switch result {
            case .failure(let error):
                completionHandler(.failure(error))
            case .success(let reactionOutput):
                completionHandler(.success([reactionOutput]))
            }
        }
    }
    
    func removeReactions(
        _ reactions: [ReactionInput],
        fromGroupId groupId: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        guard reactions.count == 1, let reaction = reactions.first else {
            completionHandler(.failure(SHHTTPError.ClientError.badRequest("can't remove more than one reaction at a time")))
            return
        }
        
        var parameters = [
            "reactionType": reaction.reactionType.rawValue,
        ] as [String: Any]

        if let iId = reaction.inReplyToInteractionId {
            parameters["inReplyToInteractionId"] = iId
        }
        if let aGid = reaction.inReplyToAssetGlobalIdentifier {
            parameters["inReplyToAssetGlobalIdentifier"] = aGid
        }
        
        self.delete("interactions/reactions/\(groupId)", parameters: parameters) { (result: Result<NoReply, Error>) in
            switch result {
            case .success(_):
                completionHandler(.success(()))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    func retrieveInteractions(
        inGroup groupId: String,
        per: Int,
        page: Int,
        completionHandler: @escaping (Result<InteractionsGroupDTO, Error>) -> ()
    ) {
        let parameters = [
            "per": per,
            "page": page
        ] as [String: Any]
        
        self.post("interactions/\(groupId)", 
                  parameters: parameters,
                  requiresAuthentication: true,
                  completionHandler: completionHandler)
    }
    
    func addMessages(
        _ messages: [MessageInput],
        toGroupId groupId: String,
        completionHandler: @escaping (Result<[MessageOutputDTO], Error>) -> ()
    ) {
        guard messages.count == 1, let message = messages.first else {
            completionHandler(.failure(SHHTTPError.ClientError.badRequest("can't add more than one message at a time")))
            return
        }
        
        var parameters = [
            "encryptedMessage": message.encryptedMessage,
            "senderPublicSignature": message.senderPublicSignature!,
        ] as [String: Any]
        
        if let aGid = message.inReplyToAssetGlobalIdentifier {
            parameters["inReplyToAssetGlobalIdentifier"] = aGid
        }
        if let iId = message.inReplyToInteractionId {
            parameters["inReplyToAssetGlobalIdentifier"] = iId
        }
        
        self.post("interactions/messages/\(groupId)",
                  parameters: parameters) { (result: Result<MessageOutputDTO, Error>) in
            switch result {
            case .failure(let error):
                completionHandler(.failure(error))
            case .success(let messageOutput):
                completionHandler(.success([messageOutput]))
            }
        }
    }
}
