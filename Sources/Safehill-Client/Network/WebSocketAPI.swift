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
    private var keepAliveTimer: Timer? = nil
    
    static let webSocketURLSession = URLSession(configuration: SafehillServerDefaultURLSessionConfiguration)
    
    init() {
        self.urlComponents = SafehillServerURLComponentsForWebsockets
    }
    
    init(url: String) throws {
        guard let components = URLComponents(string: url) else {
            throw WebSocketConnectionError.transportError
        }
        self.urlComponents = components
    }
    
    deinit {
        // Make sure to cancel the WebSocketTask (if not already canceled or completed)
        webSocketTask?.cancel(with: .goingAway, reason: nil)
    }
    
    public func connect(
        to endpoint: String,
        as authedUser: SHAuthenticatedLocalUser,
        from deviceId: String,
        keepAliveIntervalInSeconds: TimeInterval
    ) throws {
        guard webSocketTask == nil else {
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
        
        self.webSocketTask = WebSocketAPI.webSocketURLSession.webSocketTask(with: request)
        self.webSocketTask!.resume()
        
        keepAliveTimer = Timer.scheduledTimer(withTimeInterval: keepAliveIntervalInSeconds, repeats: true) { [weak self] _ in
            do { try self?.sendKeepAliveMessage() }
            catch { log.error("failed to send ping for keepAlive") }
        }
        keepAliveTimer?.fire()
        RunLoop.current.run()
    }
    
    public func disconnect() {
        keepAliveTimer?.invalidate()
        self.webSocketTask?.cancel(with: .normalClosure, reason: nil)
    }
}

extension WebSocketAPI {
    
    private func sendKeepAliveMessage() throws {
        guard let webSocketTask = self.webSocketTask else {
            throw WebSocketConnectionError.closed
        }
        
        Task {
            let keepAliveMessage = URLSessionWebSocketTask.Message.string("ping")
            try await webSocketTask.send(keepAliveMessage)
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
