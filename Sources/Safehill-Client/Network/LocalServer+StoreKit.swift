import Foundation


let SHIAPVerifyReceiptURLSandbox = "https://sandbox.itunes.apple.com/verifyReceipt"
let SHIAPVerifyReceiptURLProd = "https://buy.itunes.apple.com/verifyReceipt"


public enum SHStoreKitHandlerError: Error, LocalizedError {
    case paymentsNotAllowed
    case paymentWasCancelled
    case transactionInProgress
    case transactionFailed
    case receiptValidationError(String)
    case receiptValidationFailedWithStatus(Int)
    
    public var errorDescription: String? {
        switch self {
        case .receiptValidationError(let message):
            return "validation failed: \(message)"
        case .receiptValidationFailedWithStatus(let status):
            return "validation failed with status \(status)"
        case .transactionFailed:
            return "transaction failed"
        case .transactionInProgress:
            return "transaction in progress"
        case .paymentWasCancelled:
            return "payment was cancelled"
        case .paymentsNotAllowed:
            return "payments not allowed"
        }
    }
}

public enum SHIAPProduct {
    case free, basic, premium, pro
    
    var identifier: String? {
        switch self {
        case .free:
            nil
        case .basic:
            "safehill.subscription.basic"
        case .premium:
            "safehill.subscription.premium"
        case .pro:
            "safehill.subscription.professional"
        }
    }
}

public let SHIAPSharedSecret = "92267f6120324e16b1cf9b52c5ea26e7"

public let SHIAPProductLimits = [
    SHIAPProduct.free: 50,
    SHIAPProduct.basic: 1000,
    SHIAPProduct.premium: 25000,
    SHIAPProduct.pro: 1000000
]


// MARK: - IAP API DTOs

extension String {
    func gmtDate() -> Date? {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd HH:mm:ss VV"
        return formatter.date(from: self)
    }
}

struct ValidateReceiptResponse: Codable {
    let status: Int
    let environment: String?
    let latestReceiptInfo: [LatestReceiptInfo]
    let pendingRenewalInfo: [PendingRenewalInfo]
    
    enum CodingKeys: String, CodingKey {
        case status
        case environment
        case latestReceiptInfo = "latest_receipt_info"
        case pendingRenewalInfo = "pending_renewal_info"
    }
    
    init(status: Int,
         environment: String?,
         latestReceiptInfo: [LatestReceiptInfo],
         pendingRenewalInfo: [PendingRenewalInfo]) {
        self.status = status
        self.environment = environment
        self.latestReceiptInfo = latestReceiptInfo
        self.pendingRenewalInfo = pendingRenewalInfo
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        status = try container.decode(Int.self, forKey: .status)
        environment = try? container.decode(String.self, forKey: .environment)
        guard status != 21007 else {
            latestReceiptInfo = []
            pendingRenewalInfo = []
            return
        }
        do {
            let _latestReceiptInfo = try container.decode([LatestReceiptInfo]?.self, forKey: .latestReceiptInfo)
            self.latestReceiptInfo = _latestReceiptInfo ?? []
        } catch {
            log.warning("unable to parse latestReceiptInfo: \(error)")
            self.latestReceiptInfo = []
        }
        do {
            let _pendingRenewalInfo = try container.decode([PendingRenewalInfo]?.self, forKey: .pendingRenewalInfo)
            self.pendingRenewalInfo = _pendingRenewalInfo ?? []
        } catch {
            log.warning("unable to parse pendingRenewalInfo: \(error)")
            self.pendingRenewalInfo = []
        }
    }
}

// See https://developer.apple.com/documentation/appstorereceipts/responsebody/latest_receipt_info
struct LatestReceiptInfo: Codable {
    let quantity: Int
    let productId: String
    let transactionId: String
    let originalTransactionId: String
    let purchaseDate: Date?
    let originalPurchaseDate: Date?
    let expiresDate: Date?
    let cancellationDate: Date?
    let isInIntroOfferPeriod: Bool
    let subscriptionGroupIdentifier: String
    
    enum CodingKeys: String, CodingKey {
        case quantity
        case productId = "product_id"
        case transactionId = "transaction_id"
        case originalTransactionId = "original_transaction_id"
        case purchaseDate = "purchase_date"
        case originalPurchaseDate = "original_purchase_date"
        case expiresDate = "expires_date"
        case cancellationDate = "cancellation_date"
        case isInIntroOfferPeriod = "is_in_intro_offer_period"
        case subscriptionGroupIdentifier = "subscription_group_identifier"
    }
    
    init(quantity: Int,
         productId: String,
         transactionId: String,
         originalTransactionId: String,
         purchaseDate: Date?,
         originalPurchaseDate: Date?,
         expiresDate: Date?,
         cancellationDate: Date?,
         isInIntroOfferPeriod: Bool,
         subscriptionGroupIdentifier: String) {
        self.quantity = quantity
        self.productId = productId
        self.transactionId = transactionId
        self.originalTransactionId = originalTransactionId
        self.purchaseDate = purchaseDate
        self.originalPurchaseDate = originalPurchaseDate
        self.expiresDate = expiresDate
        self.cancellationDate = cancellationDate
        self.isInIntroOfferPeriod = isInIntroOfferPeriod
        self.subscriptionGroupIdentifier = subscriptionGroupIdentifier
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        quantity = Int(try container.decode(String.self, forKey: .quantity)) ?? 0
        productId = try container.decode(String.self, forKey: .productId)
        transactionId = try container.decode(String.self, forKey: .transactionId)
        originalTransactionId = try container.decode(String.self, forKey: .originalTransactionId)
        
        purchaseDate = try container.decode(String.self, forKey: .purchaseDate).gmtDate()
        originalPurchaseDate = try container.decode(String.self, forKey: .originalPurchaseDate).gmtDate()
        expiresDate = try container.decode(String.self, forKey: .expiresDate).gmtDate()
        cancellationDate = try? container.decode(String.self, forKey: .cancellationDate).gmtDate()
        
        isInIntroOfferPeriod = Bool(try container.decode(String.self, forKey: .isInIntroOfferPeriod)) ?? false
        subscriptionGroupIdentifier = try container.decode(String.self, forKey: .subscriptionGroupIdentifier)
    }
}

// See https://developer.apple.com/documentation/appstorereceipts/responsebody/pending_renewal_info
struct PendingRenewalInfo: Codable {
    let autoRenewProductId: String
    let originalTransactionId: String
    let autoRenewStatus: Int
    let isInBillingRetryPeriod: Bool
    
    enum CodingKeys: String, CodingKey {
        case autoRenewProductId = "auto_renew_product_id"
        case originalTransactionId = "original_transaction_id"
        case autoRenewStatus = "auto_renew_status"
        case isInBillingRetryPeriod = "is_in_billing_retry_period"
    }
    
    init(autoRenewProductId: String,
         originalTransactionId: String,
         autoRenewStatus: Int,
         isInBillingRetryPeriod: Bool) {
        self.autoRenewProductId = autoRenewProductId
        self.originalTransactionId = originalTransactionId
        self.autoRenewStatus = autoRenewStatus
        self.isInBillingRetryPeriod = isInBillingRetryPeriod
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        autoRenewProductId = try container.decode(String.self, forKey: .autoRenewProductId)
        originalTransactionId = try container.decode(String.self, forKey: .originalTransactionId)
        autoRenewStatus = Int(try container.decode(String.self, forKey: .autoRenewStatus)) ?? 1
        do {
            isInBillingRetryPeriod = Bool(try container.decode(String.self, forKey: .isInBillingRetryPeriod)) ?? false
        } catch {
            isInBillingRetryPeriod = false
        }
    }
}

// MARK: - LocalServer Extension

extension LocalServer {
    
    /// Validate the current receipt. Try the PROD endpoint first, and the SANDBOX if that fails with status 21007
    /// This approach ensures we don’t have to switch between URLs while your application is tested, reviewed by App Review, or live in the App Store.
    /// Reference: https://developer.apple.com/documentation/storekit/in-app_purchase/original_api_for_in-app_purchase/validating_receipts_with_the_app_store
    func validateTransaction(
        originalTransactionId: String,
        receipt: String,
        productId: String,
        completionHandler: @escaping (Result<SHReceiptValidationResponse, Error>) -> ()
    ) {
        self.validate(receipt: receipt, against: SHIAPVerifyReceiptURLProd) { result in
            switch result {
            case .success(let response):
                switch response.0 {
                case 0:
                    completionHandler(.success(response.1))
                case 21007, 21002:
                    self.validate(receipt: receipt, against: SHIAPVerifyReceiptURLSandbox) {
                        result in
                        switch result {
                        case .success(let response):
                            if response.0 == 0 {
                                completionHandler(.success(response.1))
                            } else {
                                completionHandler(.failure(SHStoreKitHandlerError.receiptValidationFailedWithStatus(response.0)))
                            }
                        case .failure(let error):
                            completionHandler(.failure(error))
                        }
                    }
                default:
                    completionHandler(.failure(SHStoreKitHandlerError.receiptValidationFailedWithStatus(response.0)))
                }
            case .failure(let error):
                completionHandler(.failure(error))
            }
        }
    }
    
    private func validate(receipt: String, against url: String, completionHandler: @escaping (Swift.Result<(Int, SHReceiptValidationResponse), Error>) -> Void) {
        let jsonDict: [String: AnyObject] = [
            "receipt-data" : receipt as AnyObject,
            "exclude-old-transactions": true as AnyObject,
            "password" : SHIAPSharedSecret as AnyObject
        ]
        
        do {
            let requestData = try JSONSerialization.data(withJSONObject: jsonDict, options: JSONSerialization.WritingOptions.prettyPrinted)
            let storeURL = URL(string: url)!
            var storeRequest = URLRequest(url: storeURL)
            storeRequest.httpMethod = "POST"
            storeRequest.httpBody = requestData
            
            let session = URLSession.shared
            let task = session.dataTask(with: storeRequest, completionHandler: { (data, response, error) in
                guard let data = data else {
                    completionHandler(.failure(SHStoreKitHandlerError.receiptValidationError("failed to parse response")))
                    return
                }
                
                do {
                    let validateReceiptResponse = try JSONDecoder().decode(ValidateReceiptResponse.self, from: data)
                    let response = SHReceiptValidationResponse(
                        productId: validateReceiptResponse.latestReceiptInfo.last?.productId,
                        autoRenewStatus: validateReceiptResponse.pendingRenewalInfo.last?.autoRenewStatus,
                        originalTransactionId: validateReceiptResponse.latestReceiptInfo.last?.originalTransactionId,
                        isInBillingRetryPeriod: validateReceiptResponse.pendingRenewalInfo.last?.isInBillingRetryPeriod,
                        notificationType: -1,
                        expiration: validateReceiptResponse.latestReceiptInfo.last?.expiresDate
                    )
                    
                    completionHandler(.success((validateReceiptResponse.status, response)))
                } catch let parseError {
                    log.error("[receipt validation] error parsing response with error: \(parseError.localizedDescription)")
                    completionHandler(.failure(SHStoreKitHandlerError.receiptValidationError("failed to parse response")))
                }
            })
            task.resume()
        } catch let parseError {
            log.error("error parsing request \(jsonDict) with error: \(parseError.localizedDescription)")
            completionHandler(.failure(SHStoreKitHandlerError.receiptValidationError("failed to parse request")))
        }
    }
}
