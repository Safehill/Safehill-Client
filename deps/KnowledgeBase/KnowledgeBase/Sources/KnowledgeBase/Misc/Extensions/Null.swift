//
//  Null.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/18/21.
//

import Foundation

internal func NSNullToNil(_ v: Any) -> Any? {
    if v is NSNull {
        return nil
    }
    return v
}

internal func nilToNSNull(_ v: Any?) -> Any {
    if v == nil {
        return NSNull()
    }
    return v!
}
