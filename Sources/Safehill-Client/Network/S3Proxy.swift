import Foundation
import Safehill_Crypto

public enum S3HTTPMethod: String {
    case GET = "GET"
    case PUT = "PUT"
}


struct S3Proxy {
    let presignedURL: String
    
    static let S3URLSession = URLSession(configuration: CDNServerDefaultURLSessionConfiguration)
    
    static var tempFolderURL: URL? {
        let folder = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent("uploads")
        
        do {
            try FileManager.default.createDirectory(at: folder, withIntermediateDirectories: true)
            return folder
        } catch {
            fatalError("failed to create temporary directory for uploads")
        }
    }

    // - MARK: Uploading
    
    private static func backgroundUpload(data: Data,
                                         urlRequest: URLRequest,
                                         sessionIdentifier: String,
                                         completionHandler: @escaping (Result<Void, Error>) -> Void) {
        guard let tempFolderURL = tempFolderURL else {
            completionHandler(.failure(SHBackgroundOperationError.fatalError("failed to create directory")))
            return
        }
        
        let sessionDelegate = SHSessionDelegate.sharedInstance
        
        sessionDelegate.addCompletionHandler(
            handler: completionHandler,
            identifier: sessionIdentifier
        )
        
        let configuration = CDNServerDefaultBackgroundURLSessionConfiguration(with: sessionIdentifier)
        let backgroundSession = URLSession(configuration: configuration,
                                           delegate: sessionDelegate,
                                           delegateQueue: OperationQueue.main)
        
        let fileName = SHHash.stringDigest(for: sessionIdentifier.data(using: .utf8)!)
        let fileURL = tempFolderURL.appendingPathComponent(fileName)
        let filePath = fileURL.path
        
        try? FileManager.default.removeItem(at: fileURL)
        if FileManager.default.createFile(atPath: filePath, contents: data, attributes: nil) {
            let task = backgroundSession.uploadTask(with: urlRequest, fromFile: fileURL)
            task.resume()
        } else {
            completionHandler(.failure(SHBackgroundOperationError.fatalError("failed to create file")))
            fatalError("failed to create file to upload")
        }
    }
    
    internal static func urlRequest(
        _ data: Data?,
        usingPresignedURL presignedURL: URL,
        headers: [String: String]? = nil
    ) -> URLRequest {
        var request = URLRequest(url: presignedURL)
        request.httpMethod = "PUT"
        request.httpBody = data
        for (headerField, headerValue) in headers ?? [:] {
            request.addValue(headerValue, forHTTPHeaderField: headerField)
        }
        return request
    }
    
    static func saveInBackground(
        _ data: Data,
        usingPresignedURL presignedURL: URL,
        headers: [String: String]? = nil,
        sessionIdentifier: String,
        completionHandler: @escaping (Result<Void, Error>) -> ()
    ) {
        /// 
        /// When in the background the request data is served from file
        /// See `backgroundUpload(data:urlRequest:sessionIdentifier:completionHandler:)`
        ///
        let request = urlRequest(nil, usingPresignedURL: presignedURL)
        
        log.info("storing asset to S3 using request \(request.httpMethod!) \(request.url!) with headers \(String(describing: request.allHTTPHeaderFields))")
        
        let bcf = ByteCountFormatter()
        bcf.allowedUnits = [.useMB] // optional: restricts the units to MB only
        bcf.countStyle = .file
        let inMegabytes = bcf.string(fromByteCount: Int64(data.count))
        log.debug("[file-size] uploading \(data.count) bytes (\(inMegabytes))")
        
        self.backgroundUpload(
            data: data,
            urlRequest: request,
            sessionIdentifier: sessionIdentifier,
            completionHandler: completionHandler
        )
    }
    
    static func save(_ data: Data,
                     usingPresignedURL presignedURL: URL,
                     headers: [String: String]? = nil,
                     completionHandler: @escaping (Result<Void, Error>) -> ()) {
        let request = urlRequest(data, usingPresignedURL: presignedURL)
        
        log.info("storing asset to S3 using request \(request.httpMethod!) \(request.url!) with headers \(String(describing: request.allHTTPHeaderFields))")
        
        let bcf = ByteCountFormatter()
        bcf.allowedUnits = [.useMB] // optional: restricts the units to MB only
        bcf.countStyle = .file
        let inMegabytes = bcf.string(fromByteCount: Int64(data.count))
        log.debug("[file-size] uploading \(data.count) bytes (\(inMegabytes))")

        RemoteServer.makeRequest(
            request: request,
            usingSession: Self.S3URLSession,
            decodingResponseAs: NoReply.self
        ) { result in
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
    
    static func saveMultipartImageData(
        _ data: Data,
        usingPresignedURL presignedURL: URL,
        headers: [String: String]? = nil,
        completionHandler: @escaping (Result<Void, Error>) -> ())
    {
        var request = URLRequest(url: presignedURL)
        request.httpMethod = "PUT"
        request.httpBody = data
        for (headerField, headerValue) in headers ?? [:] {
            request.addValue(headerValue, forHTTPHeaderField: headerField)
        }
        
        let boundary = UUID().uuidString
        request.setValue("multipart/form-data; boundary=\(boundary)", forHTTPHeaderField: "Content-Type")
        
        var data = Data()
        data.append("\r\n--\(boundary)\r\n".data(using: .utf8)!)
        data.append("Content-Disposition: form-data;\r\n".data(using: .utf8)!)
        data.append("Content-Type: image/png\r\n\r\n".data(using: .utf8)!)
        data.append(data)
        data.append("\r\n--\(boundary)--\r\n".data(using: .utf8)!)
        
        log.info("storing asset to S3 using request \(request.httpMethod!) \(request.url!) with headers \(String(describing: request.allHTTPHeaderFields))")
        
        let bcf = ByteCountFormatter()
        bcf.allowedUnits = [.useMB] // optional: restricts the units to MB only
        bcf.countStyle = .file
        let inMegabytes = bcf.string(fromByteCount: Int64(data.count))
        log.debug("[file-size] uploading \(data.count) bytes (\(inMegabytes))")
        
        RemoteServer.makeRequest(
            request: request,
            usingSession: Self.S3URLSession,
            decodingResponseAs: NoReply.self
        ) { result in
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
    
    // - MARK: Downloading
    
    private func backgroundDownload(urlRequest: URLRequest, sessionIdentifier: String) {
        let configuration = URLSessionConfiguration.background(withIdentifier: sessionIdentifier)
        
        let backgroundSession = URLSession(configuration: configuration,
                                           delegate: SHSessionDelegate.sharedInstance,
                                           delegateQueue: OperationQueue.main)
        
        let task = backgroundSession.downloadTask(with: urlRequest)
        task.resume()
    }
    
    static func retrieve(_ asset: SHServerAsset,
                         _ version: SHServerAssetVersion,
                         completionHandler: @escaping (Result<any SHEncryptedAsset, Error>) -> ()) {
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
        
        RemoteServer.makeRequest(
            request: request,
            usingSession: Self.S3URLSession,
            decodingResponseAs: Data.self
        ) { result in
            switch result {
            case .success(let data):
                let versionsDict = [
                    quality: SHGenericEncryptedAssetVersion(
                        quality: quality,
                        encryptedData: data,
                        encryptedSecret: version.encryptedSecret,
                        publicKeyData: version.publicKeyData,
                        publicSignatureData: version.publicSignatureData,
                        verificationSignatureData: version.serverPublicSignatureData ?? version.senderPublicSignatureData
                    )
                ]
                let encryptedAsset = SHGenericEncryptedAsset(
                    globalIdentifier: asset.globalIdentifier,
                    localIdentifier: asset.localIdentifier,
                    creationDate: asset.creationDate,
                    encryptedVersions: versionsDict
                )
                completionHandler(.success(encryptedAsset))
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
}
