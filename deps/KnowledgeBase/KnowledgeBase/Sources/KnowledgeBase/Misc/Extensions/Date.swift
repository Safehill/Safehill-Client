//
//  File.swift
//  
//
//  Created by Gennaro Frazzingaro on 6/28/21.
//

import Foundation

let KBDefaultDateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"

extension Date {
    func toString(_ format: String) -> String? {
        let formatter:DateFormatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(abbreviation: "GMT")
        formatter.dateFormat = format
        return formatter.string(from: self)
    }
    
    func dateTimeComponents() -> DateComponents {
        let components : Set<Calendar.Component> = [.hour,
                                                    .minute,
                                                    .second,
                                                    .day,
                                                    .month,
                                                    .year]
        return Calendar.current.dateComponents(components, from:self);
    }
    
    func isSameAs(_ date: Date, upTo: Calendar.Component) -> Bool {
        let thisComponents = self.dateTimeComponents()
        let thatComponents = date.dateTimeComponents()
        
        if thisComponents.year != thatComponents.year { return false }
        if upTo == .year { return true }
        
        if thisComponents.month != thatComponents.month { return false }
        if upTo == .month { return true }
        
        if thisComponents.day != thatComponents.day { return false }
        if upTo == .day { return true }
        
        if thisComponents.hour != thatComponents.hour { return false }
        if upTo == .hour { return true }
        
        if thisComponents.minute != thatComponents.minute { return false }
        if upTo == .minute { return true }
        
        if thisComponents.second != thatComponents.second { return false }
        if upTo == .second { return true }
        
        if ![.year, .month, .day, .hour, .minute, .second].contains(upTo) {
            log.error("Calendar.Component not supported for Date comparison")
            return false
        }

        return true
    }
}
