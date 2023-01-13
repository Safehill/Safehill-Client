import Photos.PHImageManager
import os

internal let log = Logger(subsystem: "com.gf.safehill", category: "SafehillClient")

public let kSHLowResPictureSize = CGSize(width: 480.0, height: 480.0)
public let kSHMidResPictureSize = CGSize(width: 960.0, height: 960.0)
public let kSHHiResPictureSize = CGSize(width: 2400.0, height: 2400.0)
public let kSHFullResPictureSize: CGSize? = nil


/* Set this to true to randomly simulate failures */
let kSHSimulateBackgroundOperationFailures = false


public enum SHBackgroundOperationError : Error, CustomStringConvertible {
    case unexpectedData(Any?)
    case fatalError(String)
    case timedOut
    case globalIdentifierDisagreement(String)
    case missingAssetInLocalServer(String)
    
    public var description: String {
        switch self {
        case .missingAssetInLocalServer(let globalIdentifier):
            return "Missing \(globalIdentifier) in local server assets"
        case .fatalError(let errorString):
            return "Fatal error: \(errorString)"
        case .timedOut:
            return "The operation timed out"
        case .globalIdentifierDisagreement(let localIdentifier):
            return "The global identifier for local id \(localIdentifier) doesn't match the one previously computed"
        case .unexpectedData(let data):
            return "unexpected data: \(String(describing: data))"
        }
    }
}
