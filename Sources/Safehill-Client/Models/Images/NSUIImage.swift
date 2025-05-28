import CoreVideo

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
    
    internal func toCVPixelBuffer(width: Int = 224, height: Int = 224) -> CVPixelBuffer? {
        var pixelBuffer: CVPixelBuffer?
        
        let attrs: [CFString: Any] = [
            kCVPixelBufferCGImageCompatibilityKey: true,
            kCVPixelBufferCGBitmapContextCompatibilityKey: true
        ]
        
        let status = CVPixelBufferCreate(
            kCFAllocatorDefault,
            width,
            height,
            kCVPixelFormatType_32ARGB,
            attrs as CFDictionary,
            &pixelBuffer
        )
        
        guard status == kCVReturnSuccess, let buffer = pixelBuffer else {
            return nil
        }
        
        CVPixelBufferLockBaseAddress(buffer, [])
        
        let colorSpace = CGColorSpaceCreateDeviceRGB()
        guard let context = CGContext(
            data: CVPixelBufferGetBaseAddress(buffer),
            width: width,
            height: height,
            bitsPerComponent: 8,
            bytesPerRow: CVPixelBufferGetBytesPerRow(buffer),
            space: colorSpace,
            bitmapInfo: CGImageAlphaInfo.noneSkipFirst.rawValue
        ) else {
            CVPixelBufferUnlockBaseAddress(buffer, [])
            return nil
        }
        
        context.clear(CGRect(x: 0, y: 0, width: width, height: height))
        
        let image = self.platformImage
        
#if os(iOS)
        guard let cgImage = image.cgImage else {
            CVPixelBufferUnlockBaseAddress(buffer, [])
            return nil
        }
        context.draw(cgImage, in: CGRect(x: 0, y: 0, width: width, height: height))
        
#elseif os(macOS)
        let newImage = NSImage(size: NSSize(width: width, height: height))
        newImage.lockFocus()
        image.draw(
            in: NSRect(x: 0, y: 0, width: width, height: height),
            from: .zero,
            operation: .copy,
            fraction: 1.0
        )
        newImage.unlockFocus()
        
        guard let cgImage = newImage.cgImage(forProposedRect: nil, context: nil, hints: nil) else {
            CVPixelBufferUnlockBaseAddress(buffer, [])
            return nil
        }
        
        context.draw(cgImage, in: CGRect(x: 0, y: 0, width: width, height: height))
#endif
        
        CVPixelBufferUnlockBaseAddress(buffer, [])
        
        return buffer
    }
}

