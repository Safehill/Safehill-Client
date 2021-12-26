//
//  S3Proxy.swift
//  
//
//  Created by Gennaro on 21/12/21.
//

import Foundation

public enum S3HTTPMethod: String {
    case GET = "GET"
    case PUT = "PUT"
}

struct S3Proxy {
    let presignedURL: String
    
    static func save(_ data: Data,
                     usingPresignedURL presignedURL: URL,
                     completionHandler: @escaping (Result<Void, Error>) -> ()) {
        var request = URLRequest(url: presignedURL)
        request.httpMethod = "PUT"
        request.httpBody = data
        
        log.info("S3Proxy request \(request.httpMethod!) \(request.url!)")
        
        SHServerHTTPAPI.makeRequest(request: request,
                                    decodingResponseAs: GenericSuccessResponse.self) { result in
            switch result {
            case .success(_):
                completionHandler(.success(()))
            case .failure(let err):
                completionHandler(.failure(err))
            }
        }
    }
    
    static func retrieve(_ asset: SHServerAsset,
                         _ version: SHServerAssetVersion,
                         completionHandler: @escaping (Result<SHEncryptedAsset, Error>) -> ()) {
        guard let url = URL(string: version.presignedURL) else {
            completionHandler(.failure(SHHTTPError.ServerError.unexpectedResponse("presigned URL is invalid")))
            return
        }
        
        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        
        log.info("S3Proxy request \(request.httpMethod!) \(request.url!)")
        
        SHServerHTTPAPI.makeRequest(request: request, decodingResponseAs: Data.self) { result in
            switch result {
            case .success(let data):
                let encryptedAsset = SHGenericEncryptedAsset(globalIdentifier: asset.globalIdentifier,
                                                             localIdentifier: asset.localIdentifier,
                                                             encryptedData: data,
                                                             encryptedSecret: version.encryptedSecret,
                                                             publicKeyData: version.publicKeyData,
                                                             publicSignatureData: version.publicSignatureData,
                                                             creationDate: asset.creationDate)
                completionHandler(.success(encryptedAsset))
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
}
