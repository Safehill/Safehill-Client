//
//  Dictionary.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/18/21.
//

import Foundation

extension Dictionary {
    
    mutating func append(_ key: Key, value: Value) {
        guard key is String else { return }
        
        if let previousValue = self[key] , self[key] != nil {
            if var array = previousValue as? Array<Any> {
                array.append(value)
            } else {
                var list = [Any]()
                list.append(previousValue)
                list.append(value)
                self[key] = list as? Value
            }
        } else {
            self[key] = value
        }
    }
}

