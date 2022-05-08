//
//  PaginatedSearchResult.swift
//  
//
//  Created by Gennaro on 06/05/22.
//

import Foundation

public struct SHPageMetadata : Decodable {
    public let page: Int

    /// Max items per page.
    public let per: Int

    /// Total number of items available.
    public let total: Int
    
    enum CodingKeys: String, CodingKey {
        case page
        case per
        case total
    }
}

public struct SHPaginatedSearchResult<T: Decodable> : Decodable {
    
    // The result
    let items: [T]
    
    // The pagination details
    let metadata: SHPageMetadata
}

typealias SHPaginatedUserSearchResults = SHPaginatedSearchResult<SHRemoteUser>
