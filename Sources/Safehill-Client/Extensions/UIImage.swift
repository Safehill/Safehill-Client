#if os(iOS)
import UIKit

public extension UIImage {
    func resized(to newSize: CGSize) -> UIImage? {
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
        
        guard sizeKeepingRatio.width > 0, sizeKeepingRatio.height > 0 else {
            log.error("Invalid size when resizing image to \(String(describing: newSize)): \(String(describing: sizeKeepingRatio))")
            return nil
        }
        
        // Use UIGraphicsImageRenderer for efficient image rendering
        let renderer = UIGraphicsImageRenderer(size: sizeKeepingRatio)
        let resizedImage = renderer.image { _ in
            self.draw(in: CGRect(origin: .zero, size: sizeKeepingRatio))
        }
        
        // Ensure the resized image is valid
        guard let finalImage = resizedImage.cgImage else {
            log.error("Failed to create resized image")
            return nil
        }
        
        return UIImage(cgImage: finalImage, scale: self.scale, orientation: self.imageOrientation)
    }
}
#endif
