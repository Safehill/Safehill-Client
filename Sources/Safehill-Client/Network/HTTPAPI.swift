//
//  HTTPAPI.swift
//  
//
//  Created by Gennaro on 06/11/21.
//

import Foundation
import KnowledgeBase

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
            }
            
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
        
        log.info("Making HTTP Request \(request.httpMethod!) \(request.url!)")
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
    
    func deleteAccount(completionHandler: @escaping (Result<Void, Error>) -> ()) {
        self.post("users/delete", parameters: nil, requiresAuthentication: false) { (result: Result<NoReply, Error>) in
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
    
    func signIn(password: String, completionHandler: @escaping (Swift.Result<SHAuthResponse, Error>) -> ()) {
        let parameters = [
            "identifier": self.requestor.identifier,
            "password": password
        ] as [String : Any]
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
    
    func getAssets(withGlobalIdentifiers assetIdentifiers: [String],
                   quality: SHAssetQuality,
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
                        if version.versionName != quality.rawValue {
                            continue
                        }
                        S3Proxy.retrieve(asset, version) { result in
                            switch result {
                            case .success(let encryptedAsset):
                                dictionary[encryptedAsset.globalIdentifier] = encryptedAsset
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

    func storeAsset(lowResAsset: SHEncryptedAsset, hiResAsset: SHEncryptedAsset, completionHandler: @escaping (Result<Void, Error>) -> ()) {
        
        let createDict: [String: Any?] = [
            "globalIdentifier": lowResAsset.globalIdentifier,
            "localIdentifier": lowResAsset.localIdentifier,
            "creationDate": lowResAsset.creationDate?.iso8601withFractionalSeconds,
            "versions": [
                [
                    "versionName": SHAssetQuality.lowResolution.rawValue,
                    "encryptedSecret": lowResAsset.encryptedSecret.base64EncodedString(),
                    "publicKey": lowResAsset.publicKeyData.base64EncodedString(),
                    "publicSignature": lowResAsset.publicSignatureData.base64EncodedString(),
                ],
                [
                    "versionName": SHAssetQuality.hiResolution.rawValue,
                    "encryptedSecret": hiResAsset.encryptedSecret.base64EncodedString(),
                    "publicKey": hiResAsset.publicKeyData.base64EncodedString(),
                    "publicSignature": hiResAsset.publicSignatureData.base64EncodedString()
                ]
            ]
        ]
        
        self.post("assets/create", parameters: createDict) { (result: Result<SHServerAsset, Error>) in
            switch result {
            case .success(let serverAsset):
                let dispatch = KBTimedDispatch(timeoutInMilliseconds: 120000)
                
                log.info("server asset \(lowResAsset.globalIdentifier) created. uploading versions to S3")
                
                for version in serverAsset.versions {
                    let asset: SHEncryptedAsset
                    if version.versionName == SHAssetQuality.lowResolution.rawValue {
                        asset = lowResAsset
                    } else if version.versionName == SHAssetQuality.hiResolution.rawValue {
                        asset = hiResAsset
                    } else {
                        self.deleteAssets(withGlobalIdentifiers: [lowResAsset.globalIdentifier]) { _ in
                            completionHandler(.failure(SHHTTPError.ServerError.unexpectedResponse("version names don't match")))
                        }
                        return
                    }
                    
                    guard let url = URL(string: version.presignedURL) else {
                        completionHandler(.failure(SHHTTPError.ServerError.unexpectedResponse("presigned URL is invalid")))
                        return
                    }
                    
                    dispatch.group.enter()
                    S3Proxy.save(asset.encryptedData, usingPresignedURL: url) { result in
                        switch result {
                        case .success(_):
                            dispatch.group.leave()
                        case .failure(let error):
                            log.error("error uploading asset= \(asset.globalIdentifier) version=\(version.versionName). deleting asset from server")
                            self.deleteAssets(withGlobalIdentifiers: [lowResAsset.globalIdentifier]) { _ in
                                dispatch.interrupt(error)
                            }
                        }
                    }
                }
                
                do {
                    try dispatch.wait()
                    completionHandler(.success(()))
                }
                catch {
                    completionHandler(.failure(error))
                }
                
            case .failure(let error):
                return completionHandler(.failure(error))
            }
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
