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
        /// HEURISTICS on network speed
        /// Optimistic approximation of actual network speed calculated on small payloads
        /// On relatively fast connections, the speed calculated on a small payload is far lower
        /// than the peak speed uploading a large file once the connection has been established
        self._bytesPerSecond = bytesPerSecond * 17
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
    
    var productionURLComponents: URLComponents {
        var components = URLComponents()
        components.scheme = "https"
        components.host = "safehill.herokuapp.com"
        components.port = 443
        return components
    }
    
    var localURLComponents: URLComponents {
        var components = URLComponents()
        components.scheme = "http"
        components.host = "127.0.0.1"
        components.port = 8080
        return components
    }
    
    func requestURL(route: String, urlParameters: [String: String]? = nil) -> URL {
#if targetEnvironment(simulator)
        var components = localURLComponents
#else
        var components = productionURLComponents
#endif
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
        
        let configuration = URLSessionConfiguration.default
        configuration.waitsForConnectivity = false
        URLSession(configuration: configuration).dataTask(with: request) { data, response, error in
            
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
    
    func post<T: Decodable>(_ route: String,
                            parameters: [String: Any?]?,
                            requiresAuthentication: Bool = true,
                            completionHandler: @escaping (Result<T, Error>) -> Void) {
        let url = requestURL(route: route)
        
        var request = URLRequest(url: url, timeoutInterval: 90)
        request.httpMethod = "POST"
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
    
    func updateUser(email: String?,
                    name: String?,
                    completionHandler: @escaping (Swift.Result<SHServerUser, Error>) -> ()) {
        guard email != nil || name != nil else {
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
        self.post("users/update", parameters: parameters, requiresAuthentication: false) { (result: Result<SHRemoteUser, Error>) in
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
    
    func signInWithApple(email: String,
                         name: String,
                         authorizationCode: Data,
                         identityToken: Data,
                         completionHandler: @escaping (Result<SHAuthResponse, Error>) -> ()) {
        let parameters = [
            "identifier": requestor.identifier,
            "email": email,
            "name": name,
            "publicKey": requestor.publicKeyData.base64EncodedString(),
            "publicSignature": requestor.publicSignatureData.base64EncodedString(),
            "authorizationCode": authorizationCode.base64EncodedString(),
            "identityToken": authorizationCode.base64EncodedString(),
        ] as [String : Any]
        self.post("signin/apple", parameters: parameters, requiresAuthentication: false, completionHandler: completionHandler)
    }
    
    func signIn(name: String, completionHandler: @escaping (Result<SHAuthResponse, Error>) -> ()) {
        let parameters = [
            "name": name,
            "identifier": self.requestor.identifier
        ]
        self.post("signin/challenge/start", parameters: parameters, requiresAuthentication: false) {
            (result: Result<SHAuthChallenge, Error>) in
            switch result {
            case .success(let authChallenge):
                
                // Initialize the server's SHRemoteCryptoUser
                // This will fail if the server sends invalid key/signature values
                // Since this is not supposed to happen unless the server is corrupted
                // don't retry
                guard let serverCrypto = try? SHRemoteCryptoUser(publicKeyData: Data(base64Encoded: authChallenge.publicKey)!,
                                                                publicSignatureData: Data(base64Encoded: authChallenge.publicSignature)!)
                else {
                    return completionHandler(.failure(SHHTTPError.ServerError.unexpectedResponse("publicKey=\(authChallenge.publicKey) publicSignature=\(authChallenge.publicSignature)")))
                }
                
                let encryptedChallenge = SHShareablePayload(
                    ephemeralPublicKeyData: Data(base64Encoded: authChallenge.ephemeralPublicKey)!,
                    cyphertext: Data(base64Encoded: authChallenge.challenge)!,
                    signature: Data(base64Encoded: authChallenge.ephemeralPublicSignature)!
                )
                
                do {
                    let decryptedChallenge = try SHCypher.decrypt(
                        encryptedChallenge,
                        using: self.requestor.shUser.privateKeyData,
                        from: serverCrypto.publicSignatureData
                    )
                    let signatureForData = try self.requestor.shUser.signature(for: decryptedChallenge)
                    let digest512 = Data(SHA512.hash(data: decryptedChallenge))
                    let signatureForDigest = try self.requestor.shUser.signature(for: digest512)
                    let parameters = [
                        "userIdentifier": self.requestor.identifier,
                        "signedChallenge": signatureForData.rawRepresentation.base64EncodedString(),
                        "digest": digest512.base64EncodedString(),
                        "signedDigest": signatureForDigest.rawRepresentation.base64EncodedString()
                    ]
                    self.post("signin/challenge/verify", parameters: parameters, requiresAuthentication: false, completionHandler: completionHandler)
                }
                catch {
                    completionHandler(.failure(error))
                }
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }

    func getUsers(withIdentifiers userIdentifiers: [String], completionHandler: @escaping (Result<[SHServerUser], Error>) -> ()) {
        let parameters = [
            "userIdentifiers": userIdentifiers
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

    func getAssetDescriptors(completionHandler: @escaping (Swift.Result<[SHAssetDescriptor], Error>) -> ()) {
        self.post("assets/descriptors/retrieve", parameters: nil) { (result: Result<[SHGenericAssetDescriptor], Error>) in
            switch result {
            case .success(let descriptors):
                return completionHandler(.success(descriptors))
            case .failure(let error):
                return completionHandler(.failure(error))
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
                   completionHandler: @escaping (Swift.Result<[String: SHEncryptedAsset], Error>) -> ()) {
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
                var errors = [String: Error]()
                
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
                                errors[asset.globalIdentifier + "::" + version.versionName] = err
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
                
                guard errors.count == 0 else {
                    return completionHandler(.failure(SHHTTPError.ServerError.generic("Error downloading from S3 asset identifiers \(errors.keys)")))
                }
                completionHandler(.success(manifest.dictionary))
                
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }

    func create(assets: [any SHEncryptedAsset],
                groupId: String,
                completionHandler: @escaping (Result<[SHServerAsset], Error>) -> ()) {
        guard assets.count == 1, let asset = assets.first else {
            completionHandler(.failure(SHHTTPError.ClientError.badRequest("Current API currently only supports creating one asset per request")))
            return
        }
        
        let createDict: [String: Any?] = [
            "globalIdentifier": asset.globalIdentifier,
            "localIdentifier": asset.localIdentifier,
            "creationDate": asset.creationDate?.iso8601withFractionalSeconds,
            "groupId": groupId,
            "versions": asset.encryptedVersions.values.map { encryptedVersion in
                [
                    "versionName": encryptedVersion.quality.rawValue,
                    "senderEncryptedSecret": encryptedVersion.encryptedSecret.base64EncodedString(),
                    "ephemeralPublicKey": encryptedVersion.publicKeyData.base64EncodedString(),
                    "publicSignature": encryptedVersion.publicSignatureData.base64EncodedString()
                ]
            }
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
                completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        let writeQueue = DispatchQueue(label: "upload.\(asset.globalIdentifier)",
                                       qos: .background)
        var results = [SHAssetQuality: Swift.Result<Void, Error>]()
        
        let group = DispatchGroup()
        
        for encryptedAssetVersion in asset.encryptedVersions.values {
            group.enter()
            
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
            
            S3Proxy.save(encryptedAssetVersion.encryptedData,
                         usingPresignedURL: url) { result in
                writeQueue.sync {
                    results[encryptedAssetVersion.quality] = result
                }
                
                if case .success(_) = result {
                    self.markAsset(with: asset.globalIdentifier,
                                   quality: encryptedAssetVersion.quality,
                                   as: .completed) { _ in
                        group.leave()
                    }
                } else {
                    group.leave()
                }
            }
        }
        
        let dispatchResult = group.wait(timeout: .now() + .milliseconds(SHUploadTimeoutInMilliseconds * asset.encryptedVersions.count))
        guard dispatchResult == .success else {
            return completionHandler(.failure(SHHTTPError.TransportError.timedOut))
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
}
