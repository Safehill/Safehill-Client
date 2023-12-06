import Foundation
import Safehill_Crypto


public enum S3HTTPMethod: String {
    case GET = "GET"
    case PUT = "PUT"
}

public class SHSessionDelegate: NSObject, URLSessionDelegate, URLSessionTaskDelegate {
    
    public static let sharedInstance = SHSessionDelegate()
    
    private let writeQueueAccessQueue = DispatchQueue(label: "SHSessionDelegate.write", attributes: .concurrent)
    private var handlerQueue = [String: [(Result<Void, Error>) -> Void]]()
    
    public func urlSession(_ session: URLSession,
                           taskIsWaitingForConnectivity task: URLSessionTask) {
        log.error("[BACKGROUND-URLSESSION] \(session.configuration.identifier ?? "") task is waiting for connectivity")
    }
    
    public func urlSession(_ session: URLSession,
                           didReceive challenge: URLAuthenticationChallenge,
                           completionHandler: @escaping (URLSession.AuthChallengeDisposition, URLCredential?) -> Void) {
        completionHandler(.useCredential, URLCredential(trust: challenge.protectionSpace.serverTrust!))
    }
    
    public func urlSession(_ session: URLSession,
                           task: URLSessionTask,
                           didCompleteWithError error: Error?) {
        log.error("[BACKGROUND-URLSESSION] \(session.configuration.identifier ?? "") did complete with error: \(error?.localizedDescription ?? "nil")")
        guard let identifier = session.configuration.identifier,
              identifier.isEmpty == false,
              let handlers = handlerQueue[identifier]
        else {
            return
        }
        
        /// Remove previously created file for upload once upload is done
        if let tempFolderURL = S3Proxy.tempFolderURL {
            let fileURL = tempFolderURL.appendingPathComponent(identifier)
            let filePath = fileURL.path
            try? FileManager.default.removeItem(atPath: filePath)
        }
        
        writeQueueAccessQueue.sync(flags: .barrier) {
            self.handlerQueue.removeValue(forKey: identifier)
        }
        
        for handler in handlers {
            if error == nil {
                handler(.success(()))
            } else {
                handler(.failure(error!))
            }
        }
    }
    
    public func urlSession(_ session: URLSession,
                           task: URLSessionTask,
                           didSendBodyData bytesSent: Int64,
                           totalBytesSent: Int64,
                           totalBytesExpectedToSend: Int64) {
        log.debug("[BACKGROUND-URLSESSION] \(session.configuration.identifier ?? "") did send \(bytesSent) of \(totalBytesExpectedToSend) bytes")
    }
    
    public func urlSession(_ session: URLSession, didBecomeInvalidWithError error: Error?) {
        log.error("[BACKGROUND-URLSESSION] \(session.configuration.identifier ?? "") url session became invalid: \(error?.localizedDescription ?? "")")
    }
    
    public func urlSessionDidFinishEvents(forBackgroundURLSession session: URLSession) {
        log.info("[BACKGROUND-URLSESSION] \(session.configuration.identifier ?? "") background session \(session) finished events")
        
        guard let identifier = session.configuration.identifier,
              identifier.isEmpty == false,
              let handlers = handlerQueue[identifier]
        else {
            return
        }
        
        writeQueueAccessQueue.sync(flags: .barrier) {
            self.handlerQueue.removeValue(forKey: identifier)
        }
        
        for handler in handlers {
            handler(.success(()))
        }
    }
    
    public func addCompletionHandler(handler: @escaping (Result<Void, Error>) -> Void, identifier: String) {
        writeQueueAccessQueue.sync(flags: .barrier) {
            if self.handlerQueue[identifier] == nil {
                self.handlerQueue[identifier] = [handler]
            } else {
                self.handlerQueue[identifier]!.append(handler)
            }
        }
    }
}

extension SHSessionDelegate: URLSessionDownloadDelegate {
    public func urlSession(_ session: URLSession, downloadTask: URLSessionDownloadTask, didFinishDownloadingTo location: URL) {
        log.info("[BACKGROUND-URL-SESSION] \(session.configuration.identifier ?? "") has finished the download task \(downloadTask) of URL \(location).")
    }
    
    public func urlSession(_ session: URLSession, downloadTask: URLSessionDownloadTask, didWriteData bytesWritten: Int64, totalBytesWritten: Int64, totalBytesExpectedToWrite: Int64) {
        log.info("[BACKGROUND-URL-SESSION] \(session.configuration.identifier ?? "") download task \(downloadTask) wrote an additional \(bytesWritten) bytes (total \(totalBytesWritten) bytes) out of an expected \(totalBytesExpectedToWrite) bytes.")
    }
}


struct S3Proxy {
    let presignedURL: String
    
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
        
        let configuration = URLSessionConfiguration.background(withIdentifier: sessionIdentifier)
        configuration.allowsCellularAccess = true // defaults to true
        configuration.waitsForConnectivity = true // default to false
        
        let sessionDelegate = SHSessionDelegate.sharedInstance
        
        sessionDelegate.addCompletionHandler(
            handler: completionHandler,
            identifier: sessionIdentifier
        )
        
        let backgroundSession = URLSession(configuration: configuration,
                                           delegate: sessionDelegate,
                                           delegateQueue: OperationQueue.main)
        
        let fileName = SHHash.stringDigest(for: sessionIdentifier.data(using: .utf8)!)
        let fileURL = tempFolderURL.appendingPathComponent(fileName)
        let filePath = fileURL.path
        
        try? FileManager.default.removeItem(atPath: filePath)
        if FileManager.default.createFile(atPath: filePath, contents: data, attributes: nil) {
            let task = backgroundSession.uploadTask(with: urlRequest, fromFile: fileURL)
            task.resume()
        } else {
            completionHandler(.failure(SHBackgroundOperationError.fatalError("failed to create file")))
            fatalError("failed to create file to upload")
        }
    }
    
    private static func urlRequest(
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
        log.debug("Uploading \(data.count) bytes (\(inMegabytes))")
        
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
        
        SHServerHTTPAPI.makeRequest(request: request, decodingResponseAs: Data.self) { result in
            switch result {
            case .success(let data):
                let versionsDict = [
                    quality: SHGenericEncryptedAssetVersion(
                        quality: quality,
                        encryptedData: data,
                        encryptedSecret: version.encryptedSecret,
                        publicKeyData: version.publicKeyData,
                        publicSignatureData: version.publicSignatureData
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
