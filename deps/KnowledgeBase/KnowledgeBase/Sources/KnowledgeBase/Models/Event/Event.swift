//
//  File.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/14/21.
//

import Foundation

@objc public class KBEvent: NSObject {
    
    let identifier: String
    let startDate: Date
    let endDate: Date
    let metadata: [String: Any?]
    
    init(identifier: String, start: Date, end: Date, metadata: [String: Any?]? = nil) {
        self.identifier = identifier
        self.startDate = start
        self.endDate = end
        if let m = metadata {
            self.metadata = m
        } else {
            self.metadata = [:]
        }
    }
    
    public override var hash: Int {
        var hasher = Hasher()
        hasher.combine(identifier)
        hasher.combine(startDate)
        hasher.combine(endDate)
        return hasher.finalize()
    }
    
    public override func isEqual(_ object: Any?) -> Bool {
        if let other = object as? KBEvent {
            return
                self.identifier == other.identifier &&
                self.startDate == other.startDate &&
                self.endDate == other.endDate
        } else {
            return false
        }
    }
    
    public override var description: String {
        return "\(identifier)[start=\(startDate),end=\(endDate)]"
    }
    
    public override var debugDescription: String {
        return "\(identifier)[start=\(startDate),end=\(endDate),metadata\(metadata)]"
    }
}
