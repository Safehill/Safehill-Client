import Foundation

public struct UserAuthorizationStatusDTO: Codable {
    public let pending: [SHRemoteUser]
    public let blocked: [SHRemoteUser]
}
