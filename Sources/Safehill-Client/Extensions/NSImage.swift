#if os(macOS)
import AppKit

public extension NSImage {
    func resized(to newSize: NSSize) -> NSImage? {
        if let bitmapRep = NSBitmapImageRep(
            bitmapDataPlanes: nil, pixelsWide: Int(newSize.width), pixelsHigh: Int(newSize.height),
            bitsPerSample: 8, samplesPerPixel: 4, hasAlpha: true, isPlanar: false,
            colorSpaceName: .calibratedRGB, bytesPerRow: 0, bitsPerPixel: 0
        ) {
            let sizeKeepingRatio: CGSize
            if self.size.width > self.size.height {
                ///
                /// **Landscape**
                /// `self.size.width : newSize.width = self.size.height : x`
                /// `x = newSize.width * self.size.height / self.size.width`
                ///
                let ratio = self.size.height / self.size.width
                sizeKeepingRatio = CGSize(width: newSize.width, height: newSize.width * ratio)
            } else {
                ///
                /// **Portrait**
                /// `self.size.width : x = self.size.height : newSize.height
                /// `x = newSize.height * self.size.width / self.size.height
                ///
                let ratio = self.size.width / self.size.height
                sizeKeepingRatio = CGSize(width: newSize.height * ratio, height: newSize.height)
            }
            
            bitmapRep.size = sizeKeepingRatio
            NSGraphicsContext.saveGraphicsState()
            NSGraphicsContext.current = NSGraphicsContext(bitmapImageRep: bitmapRep)
            draw(in: NSRect(x: 0, y: 0, width: sizeKeepingRatio.width, height: sizeKeepingRatio.height), from: .zero, operation: .copy, fraction: 1.0)
            NSGraphicsContext.restoreGraphicsState()

            let resizedImage = NSImage(size: sizeKeepingRatio)
            resizedImage.addRepresentation(bitmapRep)
            return resizedImage
        }

        return nil
    }
}
#endif
