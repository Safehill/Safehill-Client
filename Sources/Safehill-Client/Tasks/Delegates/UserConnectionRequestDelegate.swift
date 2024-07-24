import Foundation

public protocol SHUserConnectionRequestDelegate {
    
    /// Let the delegates know there's a connection request,
    /// namely an unauthorized user trying to share content with this user
    /// - Parameters
    ///   - user: the `SHServerUser` requesting to connect
    func didReceiveAuthorizationRequest(from user: any SHServerUser)
}
