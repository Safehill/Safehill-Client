import XCTest
@testable import Safehill_Client
@testable import Safehill_Crypto
import CryptoKit
import KnowledgeBase
import Contacts

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
    
    func testSerializeFetchRequest() throws {
        let sender = SHLocalUser.create(keychainPrefix: "com.gf.safehill.client.testUser")
        
        let asset1 = SHUploadableAsset(
            localIdentifier: "localIdentifier",
            globalIdentifier: "globalIdentifier",
            creationDate: nil,
            data: [:]
        )
        
        let queueItems = [
            SHGenericShareableGroupableQueueItem(
                asset: asset1,
                versions: [.lowResolution, .hiResolution],
                groupId: "groupId",
                groupTitle: nil,
                eventOriginator: sender,
                sharedWith: [],
                invitedUsers: [],
                asPhotoMessageInThreadId: nil,
                permissions: 0,
                isBackground: false
            ),
            SHGenericShareableGroupableQueueItem(
                asset: asset1,
                versions: [.hiResolution],
                groupId: "groupId",
                groupTitle: "blah",
                eventOriginator: sender,
                sharedWith: [sender],
                invitedUsers: ["phoneNumber1", "phoneNumber2"],
                asPhotoMessageInThreadId: "threadId",
                permissions: 0,
                isBackground: true
            ),
        ]
        
        for queueItem in queueItems {
            let data = try NSKeyedArchiver.archivedData(withRootObject: queueItem, requiringSecureCoding: true)
            
            let unarchiver: NSKeyedUnarchiver = try NSKeyedUnarchiver(forReadingFrom: data)
            let deserialized = unarchiver.decodeObject(of: SHGenericShareableGroupableQueueItem.self, forKey: NSKeyedArchiveRootObjectKey)
            
            guard let deserialized = deserialized else {
                XCTFail()
                return
            }
            XCTAssertEqual(queueItem.asset.localIdentifier, deserialized.asset.localIdentifier)
            XCTAssertEqual(queueItem.asset.globalIdentifier, deserialized.asset.globalIdentifier)
            XCTAssertEqual(queueItem.groupId, deserialized.groupId)
            XCTAssert(queueItem.versions.count == deserialized.versions.count)
            XCTAssert(queueItem.versions.sorted(by: { $0.rawValue >= $1.rawValue }).elementsEqual(deserialized.versions.sorted(by: { $0.rawValue >= $1.rawValue })))
            XCTAssertEqual(queueItem.eventOriginator.identifier, deserialized.eventOriginator.identifier)
            XCTAssert(queueItem.sharedWith.count == deserialized.sharedWith.count)
            XCTAssert(queueItem.sharedWith.map({$0.identifier}).sorted().elementsEqual(deserialized.sharedWith.map({$0.identifier}).sorted()))
            XCTAssertEqual(Set(queueItem.invitedUsers), Set(deserialized.invitedUsers))
            XCTAssertEqual(queueItem.asPhotoMessageInThreadId, deserialized.asPhotoMessageInThreadId)
            XCTAssertEqual(queueItem.isBackground, deserialized.isBackground)
        }
    }
    
    func testSerializeSHAddressBookContact() throws {
        
        let expectation = expectation(description: "afterClearingCaches")
        
        let contact = CNMutableContact()
        contact.givenName = "Jimmy"
        contact.familyName = "Claus"
        contact.phoneNumbers = [
            CNLabeledValue<CNPhoneNumber>(label: nil, value: CNPhoneNumber(stringValue: "(408) 555-5270")),
            CNLabeledValue<CNPhoneNumber>(label: nil, value: CNPhoneNumber(stringValue: "+1 (408) 555-5270")),
            CNLabeledValue<CNPhoneNumber>(label: nil, value: CNPhoneNumber(stringValue: "335 8765433")),
            CNLabeledValue<CNPhoneNumber>(label: nil, value: CNPhoneNumber(stringValue: "+39 3358765433")),
        ]
        
        let abContact = SHAddressBookContact.fromCNContact(contact: contact)
        
        SHPhoneNumberParser.sharedInstance.invalidateCaches { result in
            guard case .success(_) = result else {
                XCTFail()
                return
            }
            
            for _ in 1...4 {
                do {
                    let data = try NSKeyedArchiver.archivedData(withRootObject: abContact, requiringSecureCoding: true)
                    
                    let unarchiver: NSKeyedUnarchiver = try NSKeyedUnarchiver(forReadingFrom: data)
                    let deserialized = unarchiver.decodeObject(of: SHAddressBookContact.self, forKey: NSKeyedArchiveRootObjectKey)
                    
                    guard let deserialized = deserialized else {
                        XCTFail()
                        return
                    }
                    
                    XCTAssertEqual(abContact.id, deserialized.id)
                    XCTAssertEqual(abContact.fullName(), deserialized.fullName())
                    
                    let originalPPNs = abContact.formattedPhoneNumbers
                    let deserializedPPNs = deserialized.formattedPhoneNumbers
                    
                    XCTAssertEqual(originalPPNs.count, deserializedPPNs.count)
                    
                    for (index, number) in originalPPNs.enumerated() {
                        let deserializedNumber = deserializedPPNs[index]
                        XCTAssertEqual(number.label, deserializedNumber.label)
                        XCTAssertEqual(number.e164FormattedNumber, deserializedNumber.e164FormattedNumber)
                    }
                    
                } catch {
                    XCTFail()
                }
            }
            
            expectation.fulfill()
        }
        
        wait(for: [expectation], timeout: 2)
    }
    
    func testSerializeSHAddressBookContactList() throws {
        let contact1 = CNMutableContact()
        contact1.givenName = "Jimmy"
        contact1.familyName = "Claus"
        contact1.phoneNumbers = [
            CNLabeledValue<CNPhoneNumber>(label: nil, value: CNPhoneNumber(stringValue: "(408) 555-5270")),
            CNLabeledValue<CNPhoneNumber>(label: "primary", value: CNPhoneNumber(stringValue: "335 8765433")),
            CNLabeledValue<CNPhoneNumber>(label: nil, value: CNPhoneNumber(stringValue: "+39 3358765433")),
        ]
        
        let abContact1 = SHAddressBookContact.fromCNContact(contact: contact1)
        
        let contact2 = CNMutableContact()
        contact2.givenName = "Santa"
        contact2.phoneNumbers = [
            CNLabeledValue<CNPhoneNumber>(label: "phone", value: CNPhoneNumber(stringValue: "+39 3358765433")),
        ]
        
        let abContact2 = SHAddressBookContact.fromCNContact(contact: contact2)
        
        let abContactList = [abContact1, abContact2]
        let data = try NSKeyedArchiver.archivedData(withRootObject: abContactList, requiringSecureCoding: true)
        
        let deserializedList = try NSKeyedUnarchiver.unarchivedObject(
            ofClasses: [NSArray.self, SHAddressBookContact.self],
            from: data
        )
        
        guard let deserializedList = deserializedList as? [SHAddressBookContact] else {
            XCTFail()
            return
        }
        
        for (index, deserialized) in deserializedList.enumerated() {
            let abContact = abContactList[index]
            
            XCTAssertEqual(abContact.id, deserialized.id)
            XCTAssertEqual(abContact.fullName(), deserialized.fullName())
            
            let originalPPNs = abContact.formattedPhoneNumbers
            let deserializedPPNs = deserialized.formattedPhoneNumbers
            
            XCTAssertEqual(originalPPNs.count, deserializedPPNs.count)
            
            for (index, number) in originalPPNs.enumerated() {
                let deserializedNumber = deserializedPPNs[index]
                XCTAssertEqual(number.label, deserializedNumber.label)
                XCTAssertEqual(number.e164FormattedNumber, deserializedNumber.e164FormattedNumber)
            }
        }
    }
    
    func testDeserializeUser() throws {
        let privateKeyData = "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgnq973UVuwg+YiEkup+KLoaZDPN0ifPrBJVl8ow9aL8qhRANCAASl98xGDTjzbQzITeZ9BwtrlmZB5lTi+BFOzci9k/CtUz1hjahld0UTBnNTwVkut1hi91o1nFZGd4Z/bf9GP3hz"
        let privateSignatureData = "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgbcjnbcVTpw8XXKQAz1Cb5Jle+0lfAP6zWSCiQ1/ONsShRANCAASZ94CvKtq6IXvNQ5QOpnXLLBjq+djvshZIjwsKu+HkEgDjttnXOr6+XjFHctR8SJKrk0nqFWSaxZ6FnotWtbOX"
        
        let privKey = try P256.KeyAgreement.PrivateKey(derRepresentation: Data(base64Encoded: privateKeyData)!)
        let privSig = try P256.Signing.PrivateKey(derRepresentation: Data(base64Encoded: privateSignatureData)!)
        let shUser1 = SHLocalCryptoUser(key: privKey, signature: privSig)
        
        let string = """
{"keychainPrefix":"com.gf.safehill","shUser":{"privateKeyData":"MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgnq973UVuwg+YiEkup+KLoaZDPN0ifPrBJVl8ow9aL8qhRANCAASl98xGDTjzbQzITeZ9BwtrlmZB5lTi+BFOzci9k/CtUz1hjahld0UTBnNTwVkut1hi91o1nFZGd4Z/bf9GP3hz","privateSignatureData":"MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgbcjnbcVTpw8XXKQAz1Cb5Jle+0lfAP6zWSCiQ1/ONsShRANCAASZ94CvKtq6IXvNQ5QOpnXLLBjq+djvshZIjwsKu+HkEgDjttnXOr6+XjFHctR8SJKrk0nqFWSaxZ6FnotWtbOX"}}
"""
        
        let data = string.data(using: .utf8)!
        let shUser2 = try JSONDecoder().decode(SHLocalUser.self, from: data)
        
        XCTAssertEqual(shUser1.identifier, shUser2.identifier)
    }
    
    func testSerializeAuthenticatedUser() throws {
        let keychainPrefix = "com.gf.safehill.client.test"
        let user = SHLocalUser.create(keychainPrefix: keychainPrefix)
        
        let authedUser = SHAuthenticatedLocalUser(
            localUser: user,
            name: "testUser",
            phoneNumber: nil,
            encryptionProtocolSalt: kTestStaticProtocolSalt,
            authToken: "token"
        )
        
        let jsonForAuthedUser = try authedUser.toLocalUser().shareableLocalUser()
        let jsonForUnauthedUser = try user.shareableLocalUser()
        
        XCTAssertEqual(String(data: jsonForAuthedUser, encoding: .utf8)!,
                       String(data: jsonForUnauthedUser, encoding: .utf8)!)
        
        let expectedJsonString = """
{"keychainPrefix":"\(keychainPrefix)","shUser":{"privateKeyData":"\(user.shUser.privateKeyData.base64EncodedString())","privateSignatureData":"\(user.shUser.privateSignatureData.base64EncodedString())"}}
"""
        let expectedJsonData = expectedJsonString.data(using: .utf8)!
        
        let jsonDecoder = JSONDecoder()
        
        let expectedUser = try jsonDecoder.decode(SHLocalUser.self, from: expectedJsonData)
        
        let serializedUnauthedUser = try jsonDecoder.decode(SHLocalUser.self, from: jsonForUnauthedUser)
        let serializedAuthedUser = try jsonDecoder.decode(SHLocalUser.self, from: jsonForAuthedUser)
        
        XCTAssertEqual(serializedAuthedUser.identifier, serializedUnauthedUser.identifier)
        XCTAssertEqual(serializedAuthedUser.publicKeyData.base64EncodedString(), serializedUnauthedUser.publicKeyData.base64EncodedString())
        XCTAssertEqual(serializedAuthedUser.publicSignatureData.base64EncodedString(), serializedUnauthedUser.publicSignatureData.base64EncodedString())
        
        XCTAssertEqual(serializedAuthedUser.identifier, expectedUser.identifier)
        XCTAssertEqual(serializedAuthedUser.publicKeyData.base64EncodedString(), expectedUser.publicKeyData.base64EncodedString())
        XCTAssertEqual(serializedAuthedUser.publicSignatureData.base64EncodedString(), expectedUser.publicSignatureData.base64EncodedString())
    }
    
    func testDeserializeStatic() throws {
        let serialized = """
{"keychainPrefix":"com.gf.safehill","shUser":{"privateKeyData":"MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgtA51VKyxCkzCK97wwKWZ1+ltGe/pv50tQSJ7E/UndMuhRANCAATLabwjjg1l8xDA9qACj2Z3Y7n9qK6DMpJ8GFPeppAexDcUyC3OhcNtOTbbEj5rQSokgDoUw9vdh5+HUU3dK7i1","privateSignatureData":"MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg/UaTD+mekl+1U0y8HkW/846FoBA7Gmjo/F+996WF56+hRANCAASKfGLlI/zbGrW1LVdkVphC76YXWYVTjVE2tsJ8yVXtYn+QJ5wwdSvZr+cUv57LATeKcOhjk1yIxPqVkHACfU+r"}}
"""
        let jsonDecoder = JSONDecoder()
        
        let deserializedUser = try jsonDecoder.decode(SHLocalUser.self, from: serialized.data(using: .utf8)!)
        
        print(deserializedUser.name)
        print(deserializedUser.identifier)
    }
    
    func testEncodingDTO() throws {
        let parameters = ConversationThreadMembersUpdateDTO(
            recipientsToAdd: [
                RecipientEncryptionDetailsDTO(
                    recipientUserIdentifier: "uid",
                    ephemeralPublicKey: "epk",
                    encryptedSecret: "es",
                    secretPublicSignature: "sps",
                    senderPublicSignature: "spsig"
                )
            ],
            membersPublicIdentifierToRemove: ["uid2"],
            phoneNumbersToAdd: ["1"],
            phoneNumbersToRemove: ["2"]
        )
        
        let encoder = JSONEncoder()
        let data = try encoder.encode(parameters)
        
        let decoder = JSONDecoder()
        let serialized = try decoder.decode(ConversationThreadMembersUpdateDTO.self, from: data)
        
        XCTAssertEqual(serialized.recipientsToAdd.first?.recipientUserIdentifier, parameters.recipientsToAdd.first?.recipientUserIdentifier)
        XCTAssertEqual(serialized.recipientsToAdd.first?.ephemeralPublicKey, parameters.recipientsToAdd.first?.ephemeralPublicKey)
        XCTAssertEqual(serialized.recipientsToAdd.first?.encryptedSecret, parameters.recipientsToAdd.first?.encryptedSecret)
        XCTAssertEqual(serialized.recipientsToAdd.first?.secretPublicSignature, parameters.recipientsToAdd.first?.secretPublicSignature)
        XCTAssertEqual(serialized.recipientsToAdd.first?.senderPublicSignature, parameters.recipientsToAdd.first?.senderPublicSignature)

        XCTAssertEqual(Set(serialized.membersPublicIdentifierToRemove), Set(parameters.membersPublicIdentifierToRemove))
        XCTAssertEqual(Set(serialized.phoneNumbersToAdd), Set(parameters.phoneNumbersToAdd))
        XCTAssertEqual(Set(serialized.phoneNumbersToRemove), Set(parameters.phoneNumbersToRemove))
    }
    
    func testSerializePhone() throws {
        let parsed = SHPhoneNumberParser.sharedInstance.parse(["+14154949152"])
        if let first = parsed.first, let phoneNumber = first {
            print(phoneNumber.e164FormattedNumber)
            let hashedPhoneNumber = phoneNumber.hashedPhoneNumber
            print(hashedPhoneNumber)
        }
        print("nothing")
        print("DONE")
    }
}
