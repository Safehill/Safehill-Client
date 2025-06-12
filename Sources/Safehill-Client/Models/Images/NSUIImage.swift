import CoreVideo
import CoreML

#if os(iOS)
import UIKit
#elseif os(macOS)
import AppKit
#endif


public enum NSUIImage {
#if os(iOS)
    case uiKit(UIImage)
    
    public var platformImage: UIImage {
        guard case .uiKit(let uiImage) = self else {
            fatalError("platform inconsistency")
        }
        return uiImage
    }
#elseif os(macOS)
    case appKit(NSImage)
    
    public var platformImage: NSImage {
        guard case .appKit(let nsImage) = self else {
            fatalError("platform inconsistency")
        }
        return nsImage
    }
#endif
    
    public static func from(data: Data) -> NSUIImage? {
#if os(iOS)
        guard let image = UIImage(data: data) else {
            return nil
        }
        return NSUIImage.uiKit(image)
#else
        guard let image = NSImage(data: data) else {
            return nil
        }
        return NSUIImage.appKit(image)
#endif
    }
    
    func data() throws -> Data {
#if os(iOS)
        let data = self.platformImage.pngData()
#else
        let data = self.platformImage.png
#endif
        if let data {
            if let dataWithoutMetadata = Self.stripMetadata(from: data) {
                return dataWithoutMetadata
            } else {
                return data
            }
        } else {
            throw SHBackgroundOperationError.unexpectedData(self.platformImage)
        }
    }
    
    private static func stripMetadata(from imageData: Data) -> Data? {
        guard let imageSource = CGImageSourceCreateWithData(imageData as CFData, nil),
              let uti = CGImageSourceGetType(imageSource) else {
            return nil
        }
        
        let options: [NSString: Any] = [
            kCGImageDestinationLossyCompressionQuality: 1.0
        ]
        
        let outputData = NSMutableData()
        guard let imageDestination = CGImageDestinationCreateWithData(outputData, uti, 1, nil) else {
            return nil
        }
        
        CGImageDestinationAddImageFromSource(imageDestination, imageSource, 0, options as CFDictionary)
        if CGImageDestinationFinalize(imageDestination) {
            return outputData as Data
        }
        
        return nil
    }
}

extension NSUIImage {
    
    func toNormalizedNCHWArray() throws -> MLMultiArray {
        guard let resized = self.resized(to: CGSize(width: 224, height: 224)),
              let rgbData = resized.rgbData() else {
            throw NSError(domain: "TinyCLIP", code: 10, userInfo: [NSLocalizedDescriptionKey: "Failed to preprocess image"])
        }

        let array = try MLMultiArray(shape: [1, 3, 224, 224], dataType: .float32)
        let ptr = UnsafeMutablePointer<Float32>(OpaquePointer(array.dataPointer))

        // CLIP normalization constants
        let mean: [Float] = [0.485, 0.456, 0.406]
        let std: [Float]  = [0.229, 0.224, 0.225]

        ///
        /// TinyCLIP and similar CLIP models using TorchVision-style preprocessing expect:
        /// Shape: [1, 3, 224, 224] (NCHW)
        /// Channel order: RGB
        /// Value range: float32 in [0,1], then normalized using the CLIP normalization constants
        ///
        
        for y in 0..<224 {
            for x in 0..<224 {
                let pixelIndex = (y * 224 + x) * 3

                let r = Float(rgbData[pixelIndex]) / 255.0
                let g = Float(rgbData[pixelIndex + 1]) / 255.0
                let b = Float(rgbData[pixelIndex + 2]) / 255.0

                let rNorm = (r - mean[0]) / std[0]
                let gNorm = (g - mean[1]) / std[1]
                let bNorm = (b - mean[2]) / std[2]

                let baseOffset = y * 224 + x
                ptr[0 * 224 * 224 + baseOffset] = rNorm
                ptr[1 * 224 * 224 + baseOffset] = gNorm
                ptr[2 * 224 * 224 + baseOffset] = bNorm
            }
        }

        return array
    }

    private func resized(to size: CGSize) -> NSUIImage? {
#if os(iOS)
        UIGraphicsBeginImageContextWithOptions(size, false, 1.0)
        self.platformImage.draw(in: CGRect(origin: .zero, size: size))
        let result = UIGraphicsGetImageFromCurrentImageContext()
        UIGraphicsEndImageContext()
        return result == nil ? nil : NSUIImage.uiKit(result!)
#elseif os(macOS)
        let newImage = NSImage(size: size)
        newImage.lockFocus()
        self.platformImage.draw(
            in: NSRect(origin: .zero, size: size),
            from: .zero,
            operation: .copy,
            fraction: 1.0
        )
        newImage.unlockFocus()
        return NSUIImage.appKit(newImage)
#endif
    }

    private func rgbData() -> [UInt8]? {
#if os(iOS)
        guard let cgImage = self.platformImage.cgImage else { return nil }
#elseif os(macOS)
        guard let cgImage = self.platformImage.cgImage(forProposedRect: nil, context: nil, hints: nil) else { return nil }
#endif
        let width = cgImage.width
        let height = cgImage.height
        let bytesPerPixel = 3
        let bytesPerRow = bytesPerPixel * width
        var rawData = [UInt8](repeating: 0, count: height * width * bytesPerPixel)
        let colorSpace = CGColorSpaceCreateDeviceRGB()
        let context = CGContext(data: &rawData,
                                width: width,
                                height: height,
                                bitsPerComponent: 8,
                                bytesPerRow: bytesPerRow,
                                space: colorSpace,
                                bitmapInfo: CGImageAlphaInfo.noneSkipLast.rawValue | CGBitmapInfo.byteOrder32Big.rawValue)
        context?.draw(cgImage, in: CGRect(origin: .zero, size: CGSize(width: width, height: height)))
        return rawData
    }
}

