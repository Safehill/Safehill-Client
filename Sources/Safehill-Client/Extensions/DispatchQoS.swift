import Foundation

extension DispatchQoS.QoSClass {
    func toTaskPriority() -> TaskPriority {
        let taskPriority: TaskPriority
        switch self {
        case .background:
            taskPriority = .background
        case .utility:
            taskPriority = .utility
        case .default, .unspecified:
            taskPriority = .medium
        case .userInitiated:
            taskPriority = .userInitiated
        case .userInteractive:
            taskPriority = .high
        @unknown default:
            taskPriority = .medium
        }
        return taskPriority
    }
}
