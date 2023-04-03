//
//  File.swift
//  
//
//  Created by Gennaro Frazzingaro on 7/14/21.
//

import Foundation

public struct KBCanonicalName {

    // Date Time
    public static var hour: String { return "hour" }
    public static var minute: String { return "minute" }
    public static var day: String { return "day" }
    public static var month: String { return "month" }
    public static var year: String { return "year" }

    // Calendar
    public static var attendee: String { return "attendee" }

    // Location
    public static var latitude: String { return "latitude" }
    public static var longitude: String { return "longitude" }

    // Contacts
    public static var givenName: String { return "givenName" }
    public static var familyName: String { return "familyName" }
    public static var nickName: String { return "nickName" }
    public static var organizationName: String { return "organizationName" }
}
