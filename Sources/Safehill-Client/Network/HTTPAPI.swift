//
//  HTTPAPI.swift
//  
//
//  Created by Gennaro on 06/11/21.
//

import Foundation

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

struct GenericSuccessResponse: Decodable {
    let status: String
}

struct GenericFailureResponse: Decodable {
    let error: Bool
    let reason: String?
}

struct DeleteConfirmationSuccessResponse: Decodable {
    let status: String
    let deletedAssetGlobalIdentifiers: [String]
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
    
    func makeRequest<T: Decodable>(request: URLRequest,
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
            case 400..<500:
                var message = "Bad or malformed request"
                if let data = data,
                   let decoded = try? JSONDecoder().decode(GenericFailureResponse.self, from: data),
                   let reason = decoded.reason {
                    message = reason
                }
                return completionHandler(.failure(SHHTTPError.ClientError.badRequest(message)))
            default:
                var message = "A server error occurred"
                if let data = data,
                   let decoded = try? JSONDecoder().decode(GenericFailureResponse.self, from: data),
                   let reason = decoded.reason {
                    message = reason
                }
                return completionHandler(.failure(SHHTTPError.ServerError.generic(message)))
            }
            
            guard let data = data else {
                completionHandler(.failure(SHHTTPError.ServerError.noData))
                return
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
                            parameters: [String: Any]?,
                            requiresAuthentication: Bool = true,
                            completionHandler: @escaping (Result<T, Error>) -> Void) {
        
        let url = requestURL(route: route)
        
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.addValue("application/json", forHTTPHeaderField: "Content-Type")
        
        if requiresAuthentication {
            guard let bearerToken = self.requestor.authToken else {
                completionHandler(.failure(SHHTTPError.ClientError.unauthenticated))
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
        
        print("Making HTTP Request \(request.httpMethod!) \(request.url!)") // with parameters \(parameters ?? [:])")
        
        self.makeRequest(request: request, decodingResponseAs: T.self, completionHandler: completionHandler)
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
            "identifiers": userIdentifiers
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
    
    func getAssets(withIdentifiers assetIdentifiers: [String],
                   quality: String,
                   completionHandler: @escaping (Swift.Result<[String: SHEncryptedAsset], Error>) -> ()) {
        let parameters = [
            "identifiers": assetIdentifiers,
            "quality": quality
        ] as [String : Any]
        self.post("assets/retrieve", parameters: parameters) { (result: Result<[SHGenericEncryptedAsset], Error>) in
            switch result {
            case .success(let assets):
                var dictionary = [String: SHEncryptedAsset]()
                for asset in assets {
                    dictionary[asset.globalIdentifier] = asset
                }
                return completionHandler(.success(dictionary))
            case .failure(let error):
                return completionHandler(.failure(error))
            }
        }
    }

    func getLowResAssets(withGlobalIdentifiers assetIdentifiers: [String], completionHandler: @escaping (Swift.Result<[String: SHEncryptedAsset], Error>) -> ()) {
        getAssets(withIdentifiers: assetIdentifiers,
                  quality: "low",
                  completionHandler: completionHandler)
    }
    
    func getHiResAssets(withGlobalIdentifiers assetIdentifiers: [String], completionHandler: @escaping (Swift.Result<[String: SHEncryptedAsset], Error>) -> ()) {
        getAssets(withIdentifiers: assetIdentifiers,
                  quality: "hi",
                  completionHandler: completionHandler)
    }

    func storeAsset(lowResAsset: SHEncryptedAsset, hiResAsset: SHEncryptedAsset, completionHandler: @escaping (Result<Void, Error>) -> ()) {

        let lowResDict: [String: Any?] = [
            "assetIdentifier": lowResAsset.globalIdentifier,
            "applePhotosAssetIdentifier": lowResAsset.localIdentifier,
            "encryptedData": lowResAsset.encryptedData.base64EncodedString(),
            "encryptedSecret": lowResAsset.encryptedSecret.base64EncodedString(),
            "publicKey": lowResAsset.publicKeyData.base64EncodedString(),
            "publicSignature": lowResAsset.publicSignatureData.base64EncodedString(),
            "creationDate": lowResAsset.creationDate?.iso8601withFractionalSeconds
        ]
        let hiResDict: [String: Any?] = [
            "assetIdentifier": hiResAsset.globalIdentifier,
            "applePhotosAssetIdentifier": hiResAsset.localIdentifier,
            "encryptedData": hiResAsset.encryptedData.base64EncodedString(),
            "encryptedSecret": hiResAsset.encryptedSecret.base64EncodedString(),
            "publicKey": hiResAsset.publicKeyData.base64EncodedString(),
            "publicSignature": hiResAsset.publicSignatureData.base64EncodedString(),
            "creationDate": hiResAsset.creationDate?.iso8601withFractionalSeconds
        ]
        
        self.post("assets/create", parameters: ["low": lowResDict, "hi": hiResDict]) { (result: Result<GenericSuccessResponse, Error>) in
            switch result {
            case .success(_):
                return completionHandler(.success(()))
            case .failure(let error):
                if case .notImplemented = error as? SHHTTPError.ServerError {
                    // TODO: Remove this once the API is implemented
                    print("Mocking success create even though there was an error")
                    DispatchQueue.global(qos: .userInitiated).asyncAfter(deadline: .now() + .seconds(2)) {
                        completionHandler(.success(()))
                    }
                    return
                }
                return completionHandler(.failure(error))
            }
        }
    }

    func deleteAssets(withGlobalIdentifiers globalIdentifiers: [String], completionHandler: @escaping (Result<[String], Error>) -> ()) {
        let parameters = [
            "identifiers": globalIdentifiers
        ] as [String : Any]
        self.post("assets/delete", parameters: parameters) { (result: Result<DeleteConfirmationSuccessResponse, Error>) in
            switch result {
            case .success(let deleteConfirmation):
                return completionHandler(.success(deleteConfirmation.deletedAssetGlobalIdentifiers))
            case .failure(let error):
                return completionHandler(.failure(error))
            }
        }
    }

}
