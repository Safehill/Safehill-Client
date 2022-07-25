import Photos.PHImageManager
import os

internal let log = Logger(subsystem: "com.gf.safehill", category: "SafehillClient")

public let kSHLowResPictureSize = CGSize(width: 480.0, height: 480.0)
public let kSHHiResPictureSize = CGSize(width: 2048.0, height: 2048.0)
public let kSHFullResPictureSize: CGSize? = nil

public enum SHBackgroundOperationError : Error {
    case unexpectedData(Any?)
    case fatalError(String)
    case timedOut
}
