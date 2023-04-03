//
//  File.swift
//  
//
//  Created by Gennaro Frazzingaro on 6/25/21.
//

import Foundation

extension String {

    /************************************************
     * NOTE (Swift): Avoid Array allocation (var-args) for performance reasons
     */


    /**
     Checks if self begins with the string argument, or the concatenation of strings argument

     :param: args a variable number of arguments of type String.
     If more than one the string has to begin with their concatenation

     :returns: true if self begins with the string argument
     */
    /*
    func beginsWith(_ args: String...) -> Bool {
        var last: Index = self.startIndex
        for arg: String in args {
            let range = Range(start: last, end: self.endIndex)
            if let r = self.rangeOfString(arg, range: range)
                where r.startIndex == self.startIndex {
                last = r.endIndex
            } else {
                return false
            }
        }
        return true
    }
    ************************************************/

    func beginsWith(_ s1: String) -> Bool {
        return self.range(of: s1)?.lowerBound == self.startIndex;
    }

    func beginsWith(_ s1: String, _ s2: String) -> Bool {
        guard let r1 = self.range(of: s1)
            , r1.lowerBound == self.startIndex else { return false }
        guard let r2 = self.range(of: s2, range: (r1.upperBound ..< self.endIndex))
            , r2.lowerBound == r1.upperBound else { return false }
        return true;
    }

    func beginsWith(_ s1: String, _ s2: String, _ s3: String) -> Bool {
        guard let r1 = self.range(of: s1)
            , r1.lowerBound == self.startIndex else { return false }
        guard let r2 = self.range(of: s2, range: (r1.upperBound ..< self.endIndex))
            , r2.lowerBound == r1.upperBound else { return false }
        guard let r3 = self.range(of: s3, range: (r2.upperBound ..< self.endIndex))
            , r3.lowerBound == r2.upperBound else { return false }
        return true;
    }

    func beginsWith(_ s1: String, _ s2: String, _ s3: String, _ s4: String) -> Bool {
        guard let r1 = self.range(of: s1)
            , r1.lowerBound == self.startIndex else { return false }
        guard let r2 = self.range(of: s2, range: (r1.upperBound ..< self.endIndex))
            , r2.lowerBound == r1.upperBound else { return false }
        guard let r3 = self.range(of: s3, range: (r2.upperBound ..< self.endIndex))
            , r3.lowerBound == r2.upperBound else { return false }
        guard let r4 = self.range(of: s3, range: (r3.upperBound ..< self.endIndex))
            , r4.lowerBound == r3.upperBound else { return false }
        return true;
    }

    /************************************************
     * NOTE (Swift): Avoid Array allocation (var-args) for performance reasons
     */
    /**
     Checks if self ends with the string argument, or the concatenation of strings argument

     - parameter args: a variable number of arguments of type String.
     If more than one the string has to end with their concatenation

     - returns: true if self ends with the string argument
     */
    /*
    func endsWith(_ args: String...) -> Bool {
        var last: Index = self.endIndex
        for (var i = args.count-1 ; i >= 0 ; i--) {
            let arg = args[i]
            let range = Range(start: self.startIndex, end: last)
            if let r = self.rangeOfString(arg, range: range)
                where r.endIndex == self.endIndex {
                last = r.startIndex
            } else {
                return false
            }
        }
        return true
    }
    ************************************************/

    func endsWith(_ s1: String) -> Bool {
        return self.range(of: s1)?.upperBound == self.endIndex;
    }

    func contains (_ str: String) -> Bool {
        return self.range(of: str) != nil
    }

    /**
     * NOTE (Swift): Avoid Array allocation (var-args) for performance reasons
     */

    func combine(_ s1: String, _ s2: String, start: Bool = false, end: Bool = false) -> String {
        var result = ""
        let count = start && end ? 3 : start || end ? 2 : 1
        result.reserveCapacity(s1.utf16.count + s2.utf16.count + (self.utf16.count * count))
        if start { result.append(self) }
        result.append(s1)
        result.append(self)
        result.append(s2)
        if end { result.append(self) }
        return result
    }

    func combine(_ s1: String, _ s2: String, _ s3: String, start: Bool = false, end: Bool = false) -> String {
        var result = ""
        let count = start && end ? 4 : start || end ? 3 : 2
        result.reserveCapacity(s1.utf16.count + s2.utf16.count + s3.utf16.count + (self.utf16.count * count))
        if start { result.append(self) }
        result.append(s1)
        result.append(self)
        result.append(s2)
        result.append(self)
        result.append(s3)
        if end { result.append(self) }
        return result
    }

    func combine(_ s1: String, _ s2: String, _ s3: String, _ s4: String, start: Bool = false, end: Bool = false) -> String {
        var result = ""
        let count = start && end ? 5 : start || end ? 4 : 3
        result.reserveCapacity(s1.utf16.count + s2.utf16.count + s3.utf16.count + s4.utf16.count + (self.utf16.count * count))
        if start { result.append(self) }
        result.append(s1)
        result.append(self)
        result.append(s2)
        result.append(self)
        result.append(s3)
        result.append(self)
        result.append(s4)
        if end { result.append(self) }
        return result
    }
}


extension String {
    func toDate(_ format:String = "MM/dd/yyyy") -> Date? {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(abbreviation: "GMT")
        formatter.dateFormat = format

        return formatter.date(from: self)
    }
}


extension String {
    public func isHexaKey() -> Bool {
        let minLenght = "\(KBHexastore.SPO.rawValue)\(KBHexastore.JOINER)".count
        if self.count < minLenght {
            return false
        }
        var prefixMatches = false
        for hexaPrefix in KBHexastore.allValues {
            if self.hasPrefix(hexaPrefix.rawValue + KBHexastore.JOINER) {
                prefixMatches = true
            }
        }
        if prefixMatches {
            return self.components(separatedBy: KBHexastore.JOINER).count == 4
        }
        return false
    }
}
