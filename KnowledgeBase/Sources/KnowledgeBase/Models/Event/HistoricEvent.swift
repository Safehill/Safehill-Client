//
//  HistoricEvent.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/11/21.
//

import Foundation

let IdentifierKey = "id"
let FirstSeenKey = "firstSeen"
let LastSeenKey = "lastSeen"
let FrequencyKey = "frequency"
let LastDurationKey = "lastDuration"
let TotalDurationKey = "totalDuration"
let MetadataKey = "metadata"

@objc(KBHistoricEvent)
public class KBHistoricEvent : NSObject, NSCopying, NSSecureCoding {
    
    @objc public let identifier: String
    internal var _firstSeen: Date
    internal var _lastSeen: Date
    internal var _frequency: Int
    internal var _lastDuration: TimeInterval
    internal var _totalDuration: TimeInterval
    internal var _metadata: KBJSONObject
    
    @objc public var firstSeen: Date {
        return self._firstSeen
    }
    @objc public var lastSeen: Date {
        return self._lastSeen
    }
    @objc public var frequency: Int {
        return self._frequency
    }
    @objc public var lastDuration: TimeInterval {
        return self._lastDuration
    }
    @objc public var totalDuration: TimeInterval {
        return self._totalDuration
    }
    @objc public var metadata: KBJSONObject {
        return self._metadata
    }
    
    internal init(identifier: String,
                  firstSeen: Date,
                  lastSeen: Date,
                  frequency: Int,
                  lastDuration: TimeInterval,
                  totalDuration: TimeInterval,
                  metadata: KBJSONObject) {
        self.identifier = identifier
        self._firstSeen = firstSeen
        self._lastSeen = lastSeen
        self._frequency = frequency
        self._lastDuration = lastDuration
        self._totalDuration = totalDuration
        self._metadata = metadata
    }
    
    public func encode(with aCoder: NSCoder) {
        aCoder.encode(self.identifier, forKey: IdentifierKey)
        aCoder.encode(self._firstSeen, forKey: FirstSeenKey)
        aCoder.encode(self._lastSeen, forKey: LastSeenKey)
        aCoder.encode(self._frequency, forKey: FrequencyKey)
        aCoder.encode(self._lastDuration, forKey: LastDurationKey)
        aCoder.encode(self._totalDuration, forKey: TotalDurationKey)
        aCoder.encode(self._metadata, forKey: MetadataKey)
    }
    
    public required convenience init?(coder aDecoder: NSCoder) {
        let id = aDecoder.decodeObject(of: NSString.self, forKey: IdentifierKey)
        let fs = aDecoder.decodeObject(of: NSDate.self, forKey: FirstSeenKey)
        let ls = aDecoder.decodeObject(of: NSDate.self, forKey: LastSeenKey)
        let f = aDecoder.decodeInteger(forKey: FrequencyKey)
        let ld = aDecoder.decodeDouble(forKey: LastDurationKey)
        let td = aDecoder.decodeDouble(forKey: TotalDurationKey)
        let m = aDecoder.decodeObject(of: NSDictionary.self, forKey: MetadataKey)
        
        guard let identifier = id as String? else {
            log.error("unexpected value for identifier when decoding KBHistoricEvent object")
            return nil
        }
        guard let firstSeen = fs as Date? else {
            log.error("unexpected value for firstSeen when decoding KBHistoricEvent object")
            return nil
        }
        guard let lastSeen = ls as Date? else {
            log.error("unexpected value for lastSeen when decoding KBHistoricEvent object")
            return nil
        }
        guard let frequency = f as Int? else {
            log.error("unexpected value for frequency when decoding KBHistoricEvent object")
            return nil
        }
        guard let lastDuration = ld as TimeInterval? else {
            log.error("unexpected value for lastDuration when decoding KBHistoricEvent object")
            return nil
        }
        guard let totalDuration = td as TimeInterval? else {
            log.error("unexpected value for totalDuration when decoding KBHistoricEvent object")
            return nil
        }
        
        var metadata = KBJSONObject()
        if let m = m as? KBJSONObject {
            metadata = m
        }
        
        self.init(identifier: identifier,
                  firstSeen: firstSeen,
                  lastSeen: lastSeen,
                  frequency: frequency,
                  lastDuration: lastDuration,
                  totalDuration: totalDuration,
                  metadata: metadata)
    }
    
    public static var supportsSecureCoding: Bool {
        return true
    }
    
    @objc public required init(_ event: KBHistoricEvent) {
        self.identifier = event.identifier
        self._firstSeen = event.firstSeen
        self._lastSeen = event.lastSeen
        self._frequency = event.frequency
        self._lastDuration = event.lastDuration
        self._totalDuration = event.totalDuration
        self._metadata = event.metadata
    }
    
    @objc public convenience init?(from string: String, withIdentifier identifier: String) {
        if let data = string.data(using: .utf8) {
            do {
                let object = try JSONSerialization.jsonObject(with: data, options: JSONSerialization.ReadingOptions()) as? [String: Any]
                
                if let object = object,
                    let fs = object[FirstSeenKey] as? String,
                    let ls = object[LastSeenKey] as? String,
                    let frequency = object[FrequencyKey] as? Int {
                    
                    if let firstSeen = fs.toDate(KBDefaultDateFormat),
                        let lastSeen = ls.toDate(KBDefaultDateFormat) {
                        self.init(identifier: identifier, firstSeen: firstSeen, lastSeen: lastSeen, frequency: frequency, lastDuration: 0, totalDuration: 0, metadata: [:])
                        return
                    }
                }
            } catch {
                log.error("Couldn't initialize KBHistoricEvent from string %@", string)
            }
        }
        return nil
    }
    
    public func copy(with zone: NSZone? = nil) -> Any {
        return type(of: self).init(self)
    }

}
