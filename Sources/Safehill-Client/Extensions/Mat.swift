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
