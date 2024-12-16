import Photos
import Safehill_Crypto
import opencv2

public typealias PerceptualHash = String
extension Mat {

    static func from(hash: PerceptualHash) throws -> Mat {
        // Convert the hex string to bytes
        let bytes = stride(from: 0, to: hash.count, by: 2).compactMap { i -> UInt8? in
            let start = hash.index(hash.startIndex, offsetBy: i)
            let end = hash.index(start, offsetBy: 2, limitedBy: hash.endIndex) ?? hash.endIndex
            return UInt8(hash[start..<end], radix: 16)
        }
        
        // Create a Mat from the bytes
        let mat = Mat(rows: 1, cols: Int32(bytes.count), type: CvType.CV_8UC1)
        try mat.put(row: 0, col: 0, data: bytes)
        
        return mat
    }
    
    func hash() -> PerceptualHash {
        let bufferPointer = UnsafeBufferPointer(start: self.dataPointer(), count: self.total())
        return bufferPointer.reduce("") { result, byte in
            result + String(format: "%02x", byte)
        }
    }

}

public class SHHashingController {
    
    
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
    
    public static func globalIdentifier(
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
    
    public static func perceptualHash(for imageData: Data) throws -> PerceptualHash {
        guard let image = NSUIImage.from(data: imageData) else {
            throw Error.noImageData
        }
        
        return try perceptualHash(for: image)
    }
    
    public static func perceptualHash(for image: NSUIImage) throws -> PerceptualHash {
        let src = convertImageToMat(image)
        
        let phashAlgorithm = PHash.create()
        let outputMat = Mat()
        phashAlgorithm.compute(inputArr: src, outputArr: outputMat)
        
        return outputMat.hash()
    }
    
    public static func compare(_ hash1: PerceptualHash, _ hash2: PerceptualHash) throws -> Double {
        
        let phashAlgorithm = PHash.create()
        return phashAlgorithm.compare(hashOne: try Mat.from(hash: hash1), hashTwo: try Mat.from(hash: hash2))
    }
    
    private static func convertImageToMat(_ image: NSUIImage) -> Mat {
        let cgImage: CGImage
        let width: Int
        let height: Int
        
#if os(iOS)
        guard let cgImg = image.platformImage.cgImage else { fatalError("Unable to get CGImage") }
        cgImage = cgImg
        width = image.size.width
        height = image.size.height
#elseif os(macOS)
        guard let cgImg = image.platformImage.cgImage(forProposedRect: nil, context: nil, hints: nil) else { fatalError("Unable to get CGImage") }
        cgImage = cgImg
        width = Int(image.platformImage.size.width)
        height = Int(image.platformImage.size.height)
#endif
        
        let colorSpace = CGColorSpaceCreateDeviceRGB()
        let bitmapInfo = CGImageAlphaInfo.premultipliedLast.rawValue | CGBitmapInfo.byteOrder32Big.rawValue
        let bytesPerRow = cgImage.bytesPerRow
        
        guard let context = CGContext(data: nil,
                                      width: width,
                                      height: height,
                                      bitsPerComponent: 8,
                                      bytesPerRow: bytesPerRow,
                                      space: colorSpace,
                                      bitmapInfo: bitmapInfo) else {
            fatalError("Unable to create CGContext")
        }
        
        context.draw(cgImage, in: CGRect(x: 0, y: 0, width: CGFloat(width), height: CGFloat(height)))
        
        guard let data = context.data else {
            fatalError("Unable to get context data")
        }
        
        let dataSize = height * bytesPerRow
        let buffer = data.bindMemory(to: UInt8.self, capacity: dataSize)
        let nsData = Data(bytes: buffer, count: dataSize)
        
        let mat = Mat(rows: Int32(height), cols: Int32(width), type: CvType.CV_8UC4, data: nsData)
        return mat
    }
}
