import XCTest
@testable import Safehill_Client
@testable import Safehill_Crypto
import CryptoKit
import KnowledgeBase

final class Safehill_SerializationTests: XCTestCase {
    
    func testAuthChallenge() throws {
        let jsonString = """
{
    "challenge": "VAqQd03uyB4bq5FpiQeKL10tPginBvMNcA5T4tH09i3Cy+fH4Vo4RPYvewg=",
    "ephemeralPublicKey": "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEGbDFYjDBZ9ibPDISCqQn1wECxewtmm5eMGFqnGUrZ3BPsNCDO2HJ0O4nx0zED9b3bhvxC8IPS2X+hJeuKtw==",
    "ephemeralPublicSignature": "MEQCIEvauD2o82pllQceyqs7ex51/W82SLb8V8Md1jWSUU9WAiAvUkZDTx4xhfPetxnOf3X9OLr0AoISe3nqrF4BYEE+6g==",
    "publicKey": "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEOQHp1hTpZcmFVI+J/OskCpMtSwd0osxeYJpSHRPy1zk8WyF9TqPpRPMXHNSCzOSPKqa3hCuFevnItOS3WGQ==",
    "publicSignature": "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEUJjWbMzuHtzhLzu9Mbh0S9fxKeTYYVaAzz1JJs3jR2A7tbir6H31Ub/oNhZg0v6u78sQ4UGuofgo59VphPA==",
    "protocolSalt": "zgyVe5mrVAnkVib8HRoA35TlL6LDuKiUA0nKOliNS7U=",
    "iv": null
}
"""
        let jsonData = Data(jsonString.utf8)
        
        let decoder = JSONDecoder()
        let authChallenge = try decoder.decode(SHAuthChallenge.self, from: jsonData)
        XCTAssertNil(authChallenge.iv)
        XCTAssertNotNil(authChallenge.protocolSalt)
        XCTAssertNotNil(authChallenge.challenge)
        XCTAssertNotNil(authChallenge.publicKey)
        XCTAssertNotNil(authChallenge.publicSignature)
        XCTAssertNotNil(authChallenge.ephemeralPublicKey)
        XCTAssertNotNil(authChallenge.ephemeralPublicSignature)
    }
    
    func testAuthChallengeNoIV() throws {
        let jsonString = """
{
    "challenge": "VAqQd03uyB4bq5FpiQeKL10tPginBvMNcA5T4tH09i3Cy+fH4Vo4RPYvewg=",
    "ephemeralPublicKey": "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEGbDFYjDBZ9ibPDISCqQn1wECxewtmm5eMGFqnGUrZ3BPsNCDO2HJ0O4nx0zED9b3bhvxC8IPS2X+hJeuKtw==",
    "ephemeralPublicSignature": "MEQCIEvauD2o82pllQceyqs7ex51/W82SLb8V8Md1jWSUU9WAiAvUkZDTx4xhfPetxnOf3X9OLr0AoISe3nqrF4BYEE+6g==",
    "publicKey": "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEOQHp1hTpZcmFVI+J/OskCpMtSwd0osxeYJpSHRPy1zk8WyF9TqPpRPMXHNSCzOSPKqa3hCuFevnItOS3WGQ==",
    "publicSignature": "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEUJjWbMzuHtzhLzu9Mbh0S9fxKeTYYVaAzz1JJs3jR2A7tbir6H31Ub/oNhZg0v6u78sQ4UGuofgo59VphPA==",
    "protocolSalt": "zgyVe5mrVAnkVib8HRoA35TlL6LDuKiUA0nKOliNS7U=",
}
"""
        let jsonData = Data(jsonString.utf8)
        
        let decoder = JSONDecoder()
        let authChallenge = try decoder.decode(SHAuthChallenge.self, from: jsonData)
        XCTAssertNil(authChallenge.iv)
        XCTAssertNotNil(authChallenge.protocolSalt)
        XCTAssertNotNil(authChallenge.challenge)
        XCTAssertNotNil(authChallenge.publicKey)
        XCTAssertNotNil(authChallenge.publicSignature)
        XCTAssertNotNil(authChallenge.ephemeralPublicKey)
        XCTAssertNotNil(authChallenge.ephemeralPublicSignature)
    }
    
    func testAuthChallengeIncomplete() throws {
        let jsonString = """
{
    "challenge": "VAqQd03uyB4bq5FpiQeKL10tPginBvMNcA5T4tH09i3Cy+fH4Vo4RPYvewg=",
    "ephemeralPublicKey": "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEGbDFYjDBZ9ibPDISCqQn1wECxewtmm5eMGFqnGUrZ3BPsNCDO2HJ0O4nx0zED9b3bhvxC8IPS2X+hJeuKtw==",
    "ephemeralPublicSignature": "MEQCIEvauD2o82pllQceyqs7ex51/W82SLb8V8Md1jWSUU9WAiAvUkZDTx4xhfPetxnOf3X9OLr0AoISe3nqrF4BYEE+6g==",
    "publicKey": "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEOQHp1hTpZcmFVI+J/OskCpMtSwd0osxeYJpSHRPy1zk8WyF9TqPpRPMXHNSCzOSPKqa3hCuFevnItOS3WGQ==",
    "publicSignature": "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEUJjWbMzuHtzhLzu9Mbh0S9fxKeTYYVaAzz1JJs3jR2A7tbir6H31Ub/oNhZg0v6u78sQ4UGuofgo59VphPA=="
}
"""
        let jsonData = Data(jsonString.utf8)
        
        let decoder = JSONDecoder()
        do {
            let _ = try decoder.decode(SHAuthChallenge.self, from: jsonData)
            XCTFail()
        } catch {
            // Supposed to fail
        }
    }
    
    func testAuthResponse() throws {
        let jsonString = """
{
    "user": {
        "identifier" : "c4aa3013df93f6a35006a5f0311d0c89a4cdb45326235fdbc062a79379896a134db307442d9894996d7e065d72afe3f22bb349b1ae4dcf00c53396e84389113b",
        "name" : "Elizabeth",
        "email" : null,
        "phoneNumber" : null,
        "publicKey" : "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEZ04EPL69SUi7vZG+jRRRSDqHIxGniGFiNq1NiJmdZgFnZYgb0Kmr6UpyN9U7bu9ca4jpZVwntBgmQ4qwrujwfA==",
        "publicSignature" : "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEhFaNz48U9sA0BxhoZMfCBodeqNbS8usVj3JVkBLuAwNYwmnw7PrigxAGMLZdGUhtrz9Zg40Cau7LIOwqLpeL2w=="
    },
    "bearerToken" : "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJwdWJsaWNJZGVudGlmaWVyIjoiYzRhYTMwMTNkZjkzZjZhMzUwMDZhNWYwMzExZDBjODlhNGNkYjQ1MzI2MjM1ZmRiYzA2MmE3OTM3OTg5NmExMzRkYjMwNzQ0MmQ5ODk0OTk2ZDdlMDY1ZDcyYWZlM2YyMmJiMzQ5YjFhZTRkY2YwMGM1MzM5NmU4NDM4OTExM2IiLCJpZCI6IkQ1QzJCQTI5LUI3NTQtNEM1Qy1CODZFLTk1QjQ4MTQ3QjVBQiIsImV4cCI6MTY5NDA0NDI5OS4wNDAyMTY5LCJyb2xlIjoic3RhbmRhcmQifQ.SMlNrfOT3xHUyk310sw7Ls57jFbjhKUwcERw_wuImt6L6k5EUbUwauqcSSFbBdcx2raUUquOYoJ2axXsyEMtezXSr0kXzSmN79fMVd_ywOGXuRlulEOGc6wkA0YoDfsu_EGCHyFNd7SKsU2LngtyQOSdGB2Vctbwy2QyF4lpJI1ds06mL9BDsys1P5ka-7Gf9xrkWzZASuGgxr_qzjNLyBo0vL4Ge5yDBAKPVraWX9mMA0m3Uuhf8P0TpSfk2FKoig7LDpsLs4Aofs7pw9y6mcza-hdUZGZOhPY3v6UX6wyeNXXF7uQicAqaM2J-FlvIK_e960CFw_Lo_vpqnZK_bOhWP6UNcOXQRlM49rZ7xV_VNYDjs7mlrtLEYyRqe6tCE7R6M_YEl73NP81Ux8KGi4ZGK7ZzsQYnjeRcslEsWirOGgLQ4Z2vzYDMYGChsooymPeEt4pU6Y2K0JkhZIBLW1js9tpQt2hsEz7cjam08nymbKx4WwNZVSEh32JOx4Qg7zFlmhepv47gesSbv32IAU3sx0hYbcF_5j6CE9UZ1khUUZX_GoCOMCs56KbMNCnPSKORq1_8ttbwaZNsaU37QNHrlSVj91kca74T7J60rt2bAhtXW0ZO380aROeVRB-MR4o1B12s6Ba4ZGB5_7ONienkUxPUfXE4_icqlpXRqew",
    "encryptionProtocolSalt" : "0PT/RKOwUpk8dxYU/pJ3Vx/zespMkey8yMMgFp4ov2E=",
    "metadata" : null
}
"""
        let jsonData = Data(jsonString.utf8)
        
        let decoder = JSONDecoder()
        let authResponse = try decoder.decode(SHAuthResponse.self, from: jsonData)
        XCTAssertNil(authResponse.metadata)
        XCTAssertNotNil(authResponse.encryptionProtocolSalt)
    }
    
    func testAuthResponseNoMetadata() throws {
        let jsonString = """
{
    "user": {
        "identifier" : "c4aa3013df93f6a35006a5f0311d0c89a4cdb45326235fdbc062a79379896a134db307442d9894996d7e065d72afe3f22bb349b1ae4dcf00c53396e84389113b",
        "name" : "Elizabeth",
        "publicKey" : "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEZ04EPL69SUi7vZG+jRRRSDqHIxGniGFiNq1NiJmdZgFnZYgb0Kmr6UpyN9U7bu9ca4jpZVwntBgmQ4qwrujwfA==",
        "publicSignature" : "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEhFaNz48U9sA0BxhoZMfCBodeqNbS8usVj3JVkBLuAwNYwmnw7PrigxAGMLZdGUhtrz9Zg40Cau7LIOwqLpeL2w=="
    },
    "bearerToken" : "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJwdWJsaWNJZGVudGlmaWVyIjoiYzRhYTMwMTNkZjkzZjZhMzUwMDZhNWYwMzExZDBjODlhNGNkYjQ1MzI2MjM1ZmRiYzA2MmE3OTM3OTg5NmExMzRkYjMwNzQ0MmQ5ODk0OTk2ZDdlMDY1ZDcyYWZlM2YyMmJiMzQ5YjFhZTRkY2YwMGM1MzM5NmU4NDM4OTExM2IiLCJpZCI6IkQ1QzJCQTI5LUI3NTQtNEM1Qy1CODZFLTk1QjQ4MTQ3QjVBQiIsImV4cCI6MTY5NDA0NDI5OS4wNDAyMTY5LCJyb2xlIjoic3RhbmRhcmQifQ.SMlNrfOT3xHUyk310sw7Ls57jFbjhKUwcERw_wuImt6L6k5EUbUwauqcSSFbBdcx2raUUquOYoJ2axXsyEMtezXSr0kXzSmN79fMVd_ywOGXuRlulEOGc6wkA0YoDfsu_EGCHyFNd7SKsU2LngtyQOSdGB2Vctbwy2QyF4lpJI1ds06mL9BDsys1P5ka-7Gf9xrkWzZASuGgxr_qzjNLyBo0vL4Ge5yDBAKPVraWX9mMA0m3Uuhf8P0TpSfk2FKoig7LDpsLs4Aofs7pw9y6mcza-hdUZGZOhPY3v6UX6wyeNXXF7uQicAqaM2J-FlvIK_e960CFw_Lo_vpqnZK_bOhWP6UNcOXQRlM49rZ7xV_VNYDjs7mlrtLEYyRqe6tCE7R6M_YEl73NP81Ux8KGi4ZGK7ZzsQYnjeRcslEsWirOGgLQ4Z2vzYDMYGChsooymPeEt4pU6Y2K0JkhZIBLW1js9tpQt2hsEz7cjam08nymbKx4WwNZVSEh32JOx4Qg7zFlmhepv47gesSbv32IAU3sx0hYbcF_5j6CE9UZ1khUUZX_GoCOMCs56KbMNCnPSKORq1_8ttbwaZNsaU37QNHrlSVj91kca74T7J60rt2bAhtXW0ZO380aROeVRB-MR4o1B12s6Ba4ZGB5_7ONienkUxPUfXE4_icqlpXRqew",
    "encryptionProtocolSalt" : "0PT/RKOwUpk8dxYU/pJ3Vx/zespMkey8yMMgFp4ov2E=",
}
"""
        let jsonData = Data(jsonString.utf8)
        
        let decoder = JSONDecoder()
        let authResponse = try decoder.decode(SHAuthResponse.self, from: jsonData)
        XCTAssertNil(authResponse.metadata)
        XCTAssertNotNil(authResponse.encryptionProtocolSalt)
    }
}
