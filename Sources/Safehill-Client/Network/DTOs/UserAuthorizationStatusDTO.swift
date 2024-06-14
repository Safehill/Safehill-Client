import Foundation

public struct UserAuthorizationStatusDTO: Codable {
    let pending: [UserDTO]
    let blocked: [UserDTO]
}
