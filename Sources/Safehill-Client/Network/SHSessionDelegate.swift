import Foundation

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
        if error == nil {
            log.info("[BACKGROUND-URLSESSION] \(session.configuration.identifier ?? "") completed successfully")
        } else {
            log.error("[BACKGROUND-URLSESSION] \(session.configuration.identifier ?? "") completed with error: \(error!.localizedDescription)")
        }
        guard let identifier = session.configuration.identifier,
              identifier.isEmpty == false,
              let handlers = handlerQueue[identifier]
        else {
            return
        }
        
        /// Remove previously created file for upload once upload is done
        if let tempFolderURL = S3Proxy.tempFolderURL {
            let fileURL = tempFolderURL.appendingPathComponent(identifier)
            try? FileManager.default.removeItem(at: fileURL)
        }
        
        let _ = writeQueueAccessQueue.sync(flags: .barrier) {
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
        
        let _ = writeQueueAccessQueue.sync(flags: .barrier) {
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
