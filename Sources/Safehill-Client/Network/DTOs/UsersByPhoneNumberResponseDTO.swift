import Foundation

public struct UsersByPhoneNumberResponseDTO: Codable {
    
    let result: [String: SHServerUser]
    
    enum CodingKeys: String, CodingKey {
        case result
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        result = try container.decode([String: SHRemoteUser].self, forKey: .result)
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        
        let serializedDict = Dictionary(
            uniqueKeysWithValues: result.map({ return ($0.key, $0.value as! SHRemoteUser) })
        )                    
        try container.encode(serializedDict, forKey: .result)
    }
}
