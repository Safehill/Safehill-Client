import Foundation

public enum SHAssetDescriptorUploadState: String {
    
    /// `.notStarted` is the default state on remote server when the asset is created,
    ///  but the upload of any version hasn't completed.
    ///  It shouldn't exist on local server
    case notStarted = "not_started"
    
    /// This state means two different things on local and remote.
    /// It's the default state on local when an asset is created (because `.notStarted` doesn't make sense).
    case partial = "partial"
    
    case completed = "completed"
    case failed = "failed"
}
