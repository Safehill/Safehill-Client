import Foundation

public enum SHAssetUploadState: String {
    
    /// Not started is  default state on remote server when the asset is created,
    /// but the upload of any version hasn't completed yet
    case notStarted = "not_started"
    
    /// This state does not exist on remote server but it's the default on local server.
    /// Locally, the presence of an asset means the presence of its data,
    /// so this is the equivalent of `.notStarted` on server descriptor.
    case started = "started"
    
    case partial = "partial"
    case completed = "completed"
    case failed = "failed"
}
