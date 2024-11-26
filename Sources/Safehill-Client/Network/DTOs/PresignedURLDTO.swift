import Foundation

struct PresignedURLDTO: Codable {
    let url: URL
    let expiresInMinutes: Int64
}
