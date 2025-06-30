import Foundation

public enum GroupPermission: Int {
    case confidential = 0
    case shared = 1
    
    public static let defaultValue: GroupPermission = .shared
    
    var label: String {
        switch self {
        case .confidential:
            "Confidential"
        case .shared:
            "Regular"
        }
    }
}
