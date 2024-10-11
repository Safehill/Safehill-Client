import Photos.PHImageManager
import os

internal let log = Logger(subsystem: "com.gf.safehill", category: "SafehillClient")

public let LowResPictureSize = CGSize(width: 480.0, height: 480.0)
let kSHMidResPictureSize = CGSize(width: 1440.0, height: 1440.0)
let kSHHiResPictureSize = CGSize(width: 4800.0, height: 4800.0)
let kSHFullResPictureSize: CGSize? = nil

// TODO: This should change to `kSHFullResPictureSize` for premium accounts
let kSHMaxPictureSize = (quality: SHAssetQuality.hiResolution, size: kSHHiResPictureSize)

public func SHSizeForQuality(quality: SHAssetQuality) -> CGSize {
    switch quality {
    case .lowResolution:
        return LowResPictureSize
    case .midResolution:
        return kSHMidResPictureSize
    case .hiResolution:
        return kSHHiResPictureSize
//    case .fullResolution:
//        return kSHFullResPictureSize
    }
}


/* Set these to a value greater than 0 to randomly simulate failures */
enum ErrorSimulator {
    static let percentageUploadFailures: UInt32 = 0
    static let percentageShareFailures: UInt32 = 0
}


public enum SHBackgroundOperationError : Error, CustomStringConvertible, LocalizedError {
    case unexpectedData(Any?)
    case fatalError(String)
    case timedOut
    case globalIdentifierDisagreement(GlobalIdentifier, GlobalIdentifier)
    case missingAssetInLocalServer(String)
    case missingUnauthorizedDownloadIndexForUserId(String)
    case missingE2EEDetailsForGroup(String)
    case missingE2EEDetailsForThread(String)
    case alreadyProcessed
    
    public var description: String {
        switch self {
        case .missingE2EEDetailsForGroup(let groupId):
            return "no encryption details available for group \(groupId)"
        case .missingE2EEDetailsForThread(let threadId):
            return "no encryption details available for thread \(threadId)"
        case .missingUnauthorizedDownloadIndexForUserId(let userId):
            return "no unauthorized downloads indexed for user \(userId)"
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
        case .alreadyProcessed:
            return "A request with the same identifier is enqueued for processing already"
        }
    }
    
    public var errorDescription: String? {
        return description
    }
}
