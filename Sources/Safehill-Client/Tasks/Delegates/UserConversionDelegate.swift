import Foundation

public protocol SHUserConversionDelegate {
    
    /// Let the delegate know that the users and invitees in the specifiied threads need to be refreshed
    /// as some invited user was converted to a user in such threads
    ///
    /// - Parameter threadIds: the ids of the thread where at least one user was converted
    func didConvertUserInThreads(with threadIds: [String])
    
    /// Let the delegate know that the users and invitees in the specifiied threads need to be refreshed
    /// as some invited user was converted to a user in such threads
    ///
    /// - Parameter threadIds: the ids of the thread where at least one user was converted
    func didRequestUserConversion(assetIdsByGroupId: [String: [GlobalIdentifier]], threadIds: [String])
}
