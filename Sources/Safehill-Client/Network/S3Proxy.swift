import Foundation

public enum S3HTTPMethod: String {
    case GET = "GET"
    case PUT = "PUT"
}

struct S3Proxy {
    let presignedURL: String
    
    static func save(_ data: Data,
                     usingPresignedURL presignedURL: URL,
                     headers: [String: String]? = nil,
                     completionHandler: @escaping (Result<Void, Error>) -> ()) {
        var request = URLRequest(url: presignedURL)
        request.httpMethod = "PUT"
        request.httpBody = data
        for (headerField, headerValue) in headers ?? [:] {
            request.addValue(headerValue, forHTTPHeaderField: headerField)
        }
        
        log.info("storing asset to S3 using request \(request.httpMethod!) \(request.url!) with headers \(String(describing: request.allHTTPHeaderFields))")
        
        let bcf = ByteCountFormatter()
        bcf.allowedUnits = [.useMB] // optional: restricts the units to MB only
        bcf.countStyle = .file
        let inMegabytes = bcf.string(fromByteCount: Int64(data.count))
        log.debug("Uploading \(data.count) bytes (\(inMegabytes))")
        
        SHServerHTTPAPI.makeRequest(request: request,
                                    decodingResponseAs: NoReply.self) { result in
            switch result {
            case .success(_):
                log.info("successfully uploaded to \(presignedURL)")
                completionHandler(.success(()))
            case .failure(let err):
                log.error("error uploading to \(presignedURL): \(err.localizedDescription)")
                completionHandler(.failure(err))
            }
        }
    }
    
    static func retrieve(_ asset: SHServerAsset,
                         _ version: SHServerAssetVersion,
                         completionHandler: @escaping (Result<SHEncryptedAsset, Error>) -> ()) {
        guard let quality = SHAssetQuality(rawValue: version.versionName) else {
            completionHandler(.failure(SHHTTPError.ClientError.badRequest("invalid versionName=\(version.versionName)")))
            return
        }
        
        guard let url = URL(string: version.presignedURL) else {
            completionHandler(.failure(SHHTTPError.ServerError.unexpectedResponse("presigned URL is invalid")))
            return
        }
        
        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        
        log.info("retrieving asset \(asset.globalIdentifier) version \(version.versionName) from S3 using request \(request.httpMethod!) \(request.url!)")
        
        SHServerHTTPAPI.makeRequest(request: request, decodingResponseAs: Data.self) { result in
            switch result {
            case .success(let data):
                let encryptedAsset = SHGenericEncryptedAsset(
                    globalIdentifier: asset.globalIdentifier,
                    localIdentifier: asset.localIdentifier,
                    creationDate: asset.creationDate,
                    groupId: asset.groupId,
                    encryptedVersions: [
                        SHGenericEncryptedAssetVersion(
                            quality: quality,
                            encryptedData: data,
                            encryptedSecret: version.encryptedSecret,
                            publicKeyData: version.publicKeyData,
                            publicSignatureData: version.publicSignatureData
                        )
                    ]
                )
                completionHandler(.success(encryptedAsset))
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
}
