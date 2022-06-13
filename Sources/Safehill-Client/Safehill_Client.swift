import Photos.PHImageManager
import os

internal let log = Logger(subsystem: "com.gf.safehill", category: "SafehillClient")

public let kSHLowResPictureSize = CGSize(width: 240.0, height: 240.0)

public enum SHBackgroundOperationError : Error {
    case unexpectedData(Any?)
    case fatalError(String)
    case timedOut
}
