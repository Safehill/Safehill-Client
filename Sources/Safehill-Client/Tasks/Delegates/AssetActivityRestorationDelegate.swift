import Foundation

public protocol SHAssetActivityRestorationDelegate {
    
    /// Provide the descriptors for items uploaded by this user, but not shared
    /// - Parameter from: the descriptors
    func restoreUploadHistoryItems(from: [String: [(SHUploadHistoryItem, Date)]])
    
    /// Provide the descriptors shared by this user with other users
    /// - Parameter from: the descriptors
    func restoreShareHistoryItems(from: [String: [(SHShareHistoryItem, Date)]])
}
