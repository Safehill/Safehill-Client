import Foundation

public enum AssetActivityState {
    case notStarted, someInProgress, allCompletedSomeFailed, allCompletedSuccessfully
    
    var label: String {
        switch self {
        case .notStarted:
            "not started"
        case .someInProgress:
            "some in progress"
        case .allCompletedSomeFailed:
            "all completed some failed"
        case .allCompletedSuccessfully:
            "all completed"
        }
    }
    
    func isInProgress() -> Bool {
        return [.notStarted, .someInProgress].contains(self)
    }
}
