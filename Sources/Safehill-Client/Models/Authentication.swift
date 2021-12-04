//
//  File.swift
//  
//
//  Created by Gennaro on 02/12/21.
//

import Foundation

public typealias BearerToken = String

public struct SHAuthResponse: Codable {
    public let user: SHRemoteUser
    public let bearerToken: BearerToken
    
    enum CodingKeys: String, CodingKey {
        case user
        case bearerToken
    }
    
    public init(user: SHRemoteUser, bearerToken: BearerToken) {
        self.user = user
        self.bearerToken = bearerToken
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        user = try container.decode(SHRemoteUser.self, forKey: .user)
        bearerToken = try container.decode(String.self, forKey: .bearerToken)
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(user, forKey: .user)
        try container.encode(bearerToken, forKey: .bearerToken)
    }
}
