import CocoaImageHashing
import Photos
import Safehill_Crypto

public typealias PerceptualHash = String
public typealias PerceptualHashDistance = Int64

public class SHHashingController {
    
    static let HashingProvider = OSImageHashingProviderId.pHash
    
    public enum Error: Swift.Error, CustomStringConvertible, LocalizedError {
        case noImageData
        case invalidHash
        
        public var description: String {
            switch self {
            case .noImageData:
                return "No data to calculate the hash"
            case .invalidHash:
                return "The providede hash is invalid"
            }
        }
    }
    
    static func globalIdentifier(
        for photoAsset: SHApplePhotoAsset
    ) async -> GlobalIdentifier {
        let exifData = await photoAsset.getExifData()
        
        /// Global identifier is generated based on a combination of:
        /// - media type and subtype (metadata)
        /// - width and height (metadata)
        /// - datetime it was taken (metadata)
        /// - location (metadata)
        /// - EXIF data, like aperture, shutterspeed, and iso (metadata)
        ///
        /// If any metadata such as datetime, location and EXIF data of the photo is missing
        /// a UUID generated each time the identifier is generated is added for randomness.
        /// If all the metadata values are present, then photos are equal when
        /// - taken at a specific time (with milliseconds)
        /// - in a specific location (with exact coordinates)
        /// - with the specific settings (width, height, media type, EXIF data)
        ///
        
        var seed = [
            "\(photoAsset.phAsset.mediaType.rawValue)",
            "\(photoAsset.phAsset.mediaSubtypes.rawValue)",
            "\(photoAsset.phAsset.pixelWidth)",
            "\(photoAsset.phAsset.pixelHeight)"
        ]
        
        if photoAsset.phAsset.creationDate == nil || photoAsset.phAsset.location == nil || exifData == nil {
            seed.append(UUID().uuidString)
        } else {
            seed.append(photoAsset.phAsset.creationDate!.iso8601withFractionalSeconds)
            seed.append("\(photoAsset.phAsset.location!.coordinate.latitude)")
            seed.append("\(photoAsset.phAsset.location!.coordinate.longitude)")
            seed.append("\(exifData!.aperture)+\(exifData!.shutterSpeed)+\(exifData!.iso)")
        }
        
        let globalIdentifier = SHHash.stringDigest(for: seed.joined(separator: ":").data(using: .utf8)!)
        return globalIdentifier
    }
    
    static func perceptualHash(forImageData imageData: Data) throws -> PerceptualHash {
        guard imageData.count > 0 else {
            throw Error.noImageData
        }
        let osHash = OSImageHashing.sharedInstance().hashImageData(
            imageData,
            with: HashingProvider
        )
        if osHash == OSHashTypeError {
            throw Error.noImageData
        }
        return "\(osHash)"
    }
    
    static func calculateDistanceBetween(
        lhsPerceptualHash: PerceptualHash,
        rhsPerceptualHash: PerceptualHash
    ) throws -> OSHashDistanceType {
        guard let lhs = OSHashType(lhsPerceptualHash),
              let rhs = OSHashType(rhsPerceptualHash)
        else {
            throw SHHashingController.Error.invalidHash
        }
        
        return Int64(OSImageHashing.sharedInstance().hashDistance(
            lhs,
            to: rhs,
            with: HashingProvider
        ))
    }
}
