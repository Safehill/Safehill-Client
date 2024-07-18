import Foundation

public protocol WebSocketDelegate {
    func didConnect()
    func didDisconnect(error: Error?)
}
