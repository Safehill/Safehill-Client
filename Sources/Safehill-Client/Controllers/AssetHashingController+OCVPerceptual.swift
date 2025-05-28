import opencv2

public extension SHHashingController {

    internal static func perceptualHash(for imageData: Data) throws -> PerceptualHash {
        guard let image = NSUIImage.from(data: imageData) else {
            throw Error.noImageData
        }
        
        return try perceptualHash(for: image)
    }
    
    internal static func perceptualHash(for image: NSUIImage) throws -> PerceptualHash {
        let src = convertImageToMat(image)
        
        let phashAlgorithm = PHash.create()
        let outputMat = Mat()
        phashAlgorithm.compute(inputArr: src, outputArr: outputMat)
        
        return outputMat.hash()
    }
    
    internal static func compare(_ hash1: PerceptualHash, _ hash2: PerceptualHash) throws -> Double {
        let phashAlgorithm = PHash.create()
        return phashAlgorithm.compare(hashOne: try Mat.from(hash: hash1), hashTwo: try Mat.from(hash: hash2))
    }
    
    private static func convertImageToMat(_ image: NSUIImage) -> Mat {
        let cgImage: CGImage
        let width: Int
        let height: Int
        
#if os(iOS)
        guard let cgImg = image.platformImage.cgImage else {
            fatalError("Unable to get CGImage")
        }
#elseif os(macOS)
        guard let cgImg = image.platformImage.cgImage(forProposedRect: nil, context: nil, hints: nil) else {
            fatalError("Unable to get CGImage")
        }
#endif
        
        cgImage = cgImg
        width = Int(image.platformImage.size.width)
        height = Int(image.platformImage.size.height)
        
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
