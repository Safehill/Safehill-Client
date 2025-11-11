import Foundation

public enum WebSocketConnectionError: Error {
    case connectionError
    case transportError
    case encodingError
    case decodingError
    case disconnected
    case closed
    case invalidURL
}

public actor WebSocketAPI {
    
    private let urlComponents: URLComponents
    private var webSocketTask: URLSessionWebSocketTask?
    
    let webSocketURLSession: URLSession
    
    init() {
        self.urlComponents = SafehillServerURLComponentsForWebsockets
        self.webSocketURLSession = URLSession(
            configuration: SafehillServerDefaultURLSessionConfiguration,
            delegate: WebSocketAPIDelegate(),
            delegateQueue: nil
        )
    }
    
    init(url: String) throws {
        guard let components = URLComponents(string: url) else {
            throw WebSocketConnectionError.transportError
        }
        self.urlComponents = components
        self.webSocketURLSession = URLSession(
            configuration: SafehillServerDefaultURLSessionConfiguration,
            delegate: WebSocketAPIDelegate(),
            delegateQueue: nil
        )
    }
    
    deinit {
        // Make sure to cancel the WebSocketTask (if not already canceled or completed)
        webSocketTask?.cancel(with: .goingAway, reason: nil)
    }
    
    public func connect(
        to endpoint: String,
        as authedUser: SHAuthenticatedLocalUser,
        from deviceId: String,
        keepAliveIntervalInSeconds: TimeInterval? = nil
    ) throws {
        guard self.webSocketTask == nil else {
            log.warning("[ws] connect() called but webSocketTask already exists (state=\(self.webSocketTask?.state.rawValue ?? -1)). Ignoring duplicate connection attempt for deviceId=\(deviceId)")
            return
        }

        var urlComponents = self.urlComponents
        let param = URLQueryItem(name: "deviceId", value: deviceId)
        urlComponents.queryItems = [param]
        guard let url = urlComponents.url else {
            throw WebSocketConnectionError.invalidURL
        }

        var request = URLRequest(url: url.appendingPathComponent(endpoint))
        request.addValue("Bearer \(authedUser.authToken)", forHTTPHeaderField: "Authorization")

        log.info("[ws] connect() starting new connection: endpoint=\(endpoint), deviceId=\(deviceId), url=\(url.absoluteString)")

        self.webSocketTask = self.webSocketURLSession.webSocketTask(with: request)
        self.webSocketTask!.resume()

        if let keepAliveIntervalInSeconds {
            Task { [weak self] in
                try await Task.sleep(nanoseconds: UInt64(keepAliveIntervalInSeconds) * 1_000_000_000)
                await self?.keepAlive(keepAliveIntervalInSeconds)
            }
        }
    }
    
    public func disconnect() {
        if let task = self.webSocketTask {
            log.info("[ws] disconnect() called: current state=\(task.state.rawValue), closeCode=\(task.closeCode.rawValue)")
            task.cancel(with: .normalClosure, reason: nil)
            self.webSocketTask = nil
        } else {
            log.debug("[ws] disconnect() called but webSocketTask is already nil")
        }
    }
}

extension WebSocketAPI {
    
    private func keepAlive(_ keepAliveIntervalInSeconds: TimeInterval) {
        log.debug("[ws] sending ping for keepAlive")
        self.webSocketTask?.sendPing { [weak self] error in
            guard let self = self else { return }
            
            if let error {
                log.error("[ws] failed to send ping for keepAlive: \(error)")
            } else {
                Task { [weak self] in
                    try await Task.sleep(nanoseconds: UInt64(keepAliveIntervalInSeconds) * 1_000_000_000)
                    await self?.keepAlive(keepAliveIntervalInSeconds)
                }
            }
        }
    }
    
    public func send(_ wsMessage: WebSocketMessage) async throws {
        guard let webSocketTask = self.webSocketTask else {
            throw WebSocketConnectionError.closed
        }
        
        let serialized: String
        
        do {
            let jsonData = try JSONEncoder().encode(wsMessage)
            serialized = String(data: jsonData, encoding: .utf8)!
        } catch {
            log.error("[ws] error sending message: \(error)")
            throw WebSocketConnectionError.encodingError
        }
        
        do {
            let message = URLSessionWebSocketTask.Message.string(serialized)
            try await webSocketTask.send(message)
            log.debug("[ws] message sent successfully: \(serialized)")
        } catch {
            switch webSocketTask.closeCode {
            case .invalid:
                throw WebSocketConnectionError.connectionError
            case .goingAway:
                throw WebSocketConnectionError.disconnected
            case .normalClosure:
                throw WebSocketConnectionError.closed
            default:
                throw WebSocketConnectionError.transportError
            }
        }
    }
    
    public func receive() -> AsyncThrowingStream<WebSocketMessage, Error> {
        AsyncThrowingStream { [weak self] in
            guard let self else {
                // Self is gone, return nil to end the stream
                return nil
            }
            
            let message = try await self.receiveOneMessage()
            
            // End the stream (by returning nil) if the calling Task was canceled
            return Task.isCancelled ? nil : message
        }
    }
    
    private func receiveOneMessage() async throws -> WebSocketMessage {
        guard let webSocketTask = self.webSocketTask else {
            throw WebSocketConnectionError.closed
        }
        
        do {
            let message = try await webSocketTask.receive()
            switch message {
            case .string(let text):
                let parsed = try self.parseEncodedMessage(text)
                return parsed
            case .data(_):
                log.debug("[ws] received data message")
                throw WebSocketConnectionError.decodingError
            @unknown default:
                log.error("[ws] received unknown message type")
                throw WebSocketConnectionError.decodingError
            }
        } catch let error as WebSocketConnectionError {
            throw error
        } catch {
            switch webSocketTask.closeCode {
            case .invalid:
                throw WebSocketConnectionError.connectionError
            case .goingAway:
                throw WebSocketConnectionError.disconnected
            case .normalClosure:
                throw WebSocketConnectionError.closed
            default:
                throw WebSocketConnectionError.transportError
            }
        }
    }
    
    private func parseEncodedMessage(_ encodedMessage: String) throws -> WebSocketMessage {
        let jsonData = encodedMessage.data(using: .utf8)!
        do {
            return try JSONDecoder().decode(WebSocketMessage.self, from: jsonData)
        } catch {
            log.error("[ws] unable to parse WebSocketMessage \(encodedMessage): \(error.localizedDescription)")
            throw WebSocketConnectionError.decodingError
        }
    }
}

class WebSocketAPIDelegate: NSObject, URLSessionWebSocketDelegate {

    func urlSession(_ session: URLSession, webSocketTask: URLSessionWebSocketTask, didOpenWithProtocol protocol: String?) {
        log.info("[ws] task OPEN: protocol=\(`protocol` ?? "none"), state=\(webSocketTask.state.rawValue), taskId=\(webSocketTask.taskIdentifier)")
    }

    func urlSession(_ session: URLSession, webSocketTask: URLSessionWebSocketTask, didCloseWith closeCode: URLSessionWebSocketTask.CloseCode, reason: Data?) {
        let reasonString = reason.flatMap { String(data: $0, encoding: .utf8) } ?? "none"
        log.info("[ws] task CLOSED: code=\(closeCode.rawValue), reason=\(reasonString), state=\(webSocketTask.state.rawValue), taskId=\(webSocketTask.taskIdentifier)")
    }
}
