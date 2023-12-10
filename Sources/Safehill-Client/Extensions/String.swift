import Foundation

public extension String {
    func isValidEmail() -> Bool {
        let regex = try! NSRegularExpression(pattern: "^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$", options: .caseInsensitive)
        return regex.firstMatch(in: self, options: [], range: NSRange(location: 0, length: count)) != nil
    }
    
    func isValidName() -> Bool {
        guard self.count > 3 else {
            return false
        }
        let regex = try! NSRegularExpression(pattern: "^[\\p{Letter}\\s\\-.']+$", options: .caseInsensitive)
        return regex.firstMatch(in: self, options: [], range: NSRange(location: 0, length: count)) != nil
    }
    
    func isValidPassword() -> Bool {
        return self.count > 8
    }
    
    var isNumeric: Bool {
        guard !self.isEmpty else { return false }
        return CharacterSet(charactersIn: self).isSubset(of: CharacterSet.decimalDigits)
    }
}
