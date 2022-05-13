//
//  HTTPAPI.swift
//  
//
//  Created by Gennaro on 06/11/21.
//

import Foundation
import KnowledgeBase

public let SHUploadTimeoutInMilliseconds = 900000 // 15 minutes

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
        var components = URLComponents()
        components.scheme = "http"
        components.host = "localhost"
        components.port = 8080
//        components.scheme = "https"
//        components.host = "safehill.herokuapp.com"
//        components.port = 443
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
        URLSession.shared.dataTask(with: request) { data, response, error in
            guard error == nil else {
                completionHandler(.failure(SHHTTPError.TransportError.generic(error!)))
                return
            }
            
            let httpResponse = response as! HTTPURLResponse
            log.debug("request \(request.url!) received \(httpResponse.statusCode) response")
            
            switch httpResponse.statusCode {
            case 200..<300:
                break
            case 401:
                completionHandler(.failure(SHHTTPError.ClientError.unauthorized))
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
            }
            
            let convertedString = String(data: data, encoding: String.Encoding.utf8)
            log.debug("response: \(convertedString ?? "")")
            
            do {
                let decoded = try JSONDecoder().decode(type, from: data)
                completionHandler(.success(decoded))
            }
            catch {
                // there's a decoding error, call completion with decoding error
                completionHandler(.failure(error))
            }
            
        }.resume()
    }
    
    func get<T: Decodable>(_ route: String,
                           parameters: [String: String]?,
                           requiresAuthentication: Bool = true,
                           completionHandler: @escaping (Result<T, Error>) -> Void) {
        let url = requestURL(route: route, urlParameters: parameters)
        
        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        
        if requiresAuthentication {
            guard let bearerToken = self.requestor.authToken else {
                completionHandler(.failure(SHHTTPError.ClientError.unauthorized))
                return
            }
            request.addValue("Bearer \(bearerToken)", forHTTPHeaderField: "Authorization")
        }
        
        log.info("Making GET Request \(request.httpMethod!) \(request.url!)")
        
        SHServerHTTPAPI.makeRequest(request: request, decodingResponseAs: T.self, completionHandler: completionHandler)
    }
    
    func post<T: Decodable>(_ route: String,
                            parameters: [String: Any?]?,
                            requiresAuthentication: Bool = true,
                            completionHandler: @escaping (Result<T, Error>) -> Void) {
        let url = requestURL(route: route)
        
        var request = URLRequest(url: url)
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
        
        log.info("Making POST Request \(request.httpMethod!) \(request.url!)")
        log.debug("with parameters \(parameters ?? [:])")
        
        SHServerHTTPAPI.makeRequest(request: request, decodingResponseAs: T.self, completionHandler: completionHandler)
    }
    
    func createUser(email: String,
                    name: String,
                    password: String,
                    completionHandler: @escaping (Result<SHServerUser, Error>) -> ()) {
        let parameters = [
            "identifier": requestor.identifier,
            "publicKey": requestor.publicKeyData.base64EncodedString(),
            "publicSignature": requestor.publicSignatureData.base64EncodedString(),
            "email": email,
            "name": name,
            "password": password,
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
                    password: String?,
                    completionHandler: @escaping (Swift.Result<SHServerUser, Error>) -> ()) {
        guard email != nil || name != nil || password != nil else {
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
        if let password = password {
            parameters["password"] = password
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
    
    func deleteAccount(email: String, password: String, completionHandler: @escaping (Result<Void, Error>) -> ()) {
        self.post("users/delete", parameters: [
            "email": email,
            "password": password
        ], requiresAuthentication: false) { (result: Result<NoReply, Error>) in
            switch result {
            case .success(_):
                return completionHandler(.success(()))
            case .failure(let error):
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
    
    func signIn(email: String?, password: String, completionHandler: @escaping (Swift.Result<SHAuthResponse, Error>) -> ()) {
        let parameters = [
            "identifier": self.requestor.identifier,
            "email": email,
            "password": password
        ] as [String : Any?]
        self.post("signin", parameters: parameters, requiresAuthentication: false, completionHandler: completionHandler)
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
        let parameters = [
            "globalIdentifiers": assetIdentifiers,
        ] as [String : Any]
        self.post("assets/retrieve", parameters: parameters) { (result: Result<[SHServerAsset], Error>) in
            switch result {
            case .success(let assets):
                var dictionary = [String: SHEncryptedAsset]()
                
                let dispatch = KBTimedDispatch()
                
                for asset in assets {
                    for version in asset.versions {
                        
                        // Version filtering
                        if let versions = versions, !versions.map({$0.rawValue}).contains(version.versionName) {
                            continue
                        }
                        
                        dispatch.group.enter()
                        S3Proxy.retrieve(asset, version) { result in
                            switch result {
                            case .success(let encryptedAsset):
                                dictionary[encryptedAsset.globalIdentifier] = encryptedAsset
                                dispatch.group.leave()
                            case .failure(let error):
                                dispatch.interrupt(error)
                            }
                        }
                    }
                }
                
                do {
                    try dispatch.wait()
                    completionHandler(.success(dictionary))
                } catch {
                    completionHandler(.failure(error))
                }
                
            case .failure(let error):
                return completionHandler(.failure(error))
            }
        }
    }

    func create(asset: SHEncryptedAsset,
                completionHandler: @escaping (Result<SHServerAsset, Error>) -> ()) {
        let createDict: [String: Any?] = [
            "globalIdentifier": asset.globalIdentifier,
            "localIdentifier": asset.localIdentifier,
            "creationDate": asset.creationDate?.iso8601withFractionalSeconds,
            "versions": asset.encryptedVersions.map { encryptedVersion in
                [
                    "versionName": encryptedVersion.quality.rawValue,
                    "senderEncryptedSecret": encryptedVersion.encryptedSecret.base64EncodedString(),
                    "publicKey": encryptedVersion.publicKeyData.base64EncodedString(),
                    "publicSignature": encryptedVersion.publicSignatureData.base64EncodedString(),
                ]
            }
        ]
        
        self.post("assets/create", parameters: createDict, completionHandler: completionHandler)
    }
    
    func share(asset: SHShareableEncryptedAsset,
               completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        
        var versions = [[String: Any?]]()
        for version in asset.sharedVersions {
            versions.append([
                "versionName": version.quality.rawValue,
                "userEncryptedSecret": version.encryptedSecret.base64EncodedString(),
            ])
        }
        
        let shareDict: [String: Any?] = [
            "globalAssetIdentifier": asset.globalIdentifier,
            "versionSharingDetails": versions
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
    
    func upload(serverAsset: SHServerAsset,
                asset: SHEncryptedAsset,
                completionHandler: @escaping (Swift.Result<Void, Error>) -> ()) {
        
        var results = [SHAssetQuality: Swift.Result<Void, Error>]()
        
        let dispatch = KBTimedDispatch(timeoutInMilliseconds: SHUploadTimeoutInMilliseconds)
        
        for encryptedAssetVersion in asset.encryptedVersions {
            dispatch.group.enter()
            
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
                         usingPresignedURL: url,
                         completionHandler: completionHandler)
        }
        
        try! dispatch.notify {
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

}
