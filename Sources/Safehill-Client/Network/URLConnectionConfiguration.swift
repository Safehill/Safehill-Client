import Foundation


public let SHDefaultNetworkTimeoutInMilliseconds = 30000 // 30 seconds
public let SHUploadTimeoutInMilliseconds = 300000 // 5 minutes
public let SHDownloadTimeoutInMilliseconds = 300000 // 5 minutes



public let SafehillServerURLComponents: URLComponents = {
    var components = URLComponents()
    
#if targetEnvironment(simulator)
    components.scheme = "http"
    components.host = "127.0.0.1"
    components.port = 8080
#elseif DEBUG
    components.scheme = "https"
    components.host = "safehill-stage-1-ec0cd53b3592.herokuapp.com"
    components.port = 443
#else
    components.scheme = "https"
    components.host = "app.safehill.io"
    components.port = 443
#endif
    
    return components
}()

public let SafehillServerURLComponentsForWebsockets: URLComponents = {
    var components = URLComponents()
    
#if targetEnvironment(simulator)
    components.scheme = "ws"
    components.host = "127.0.0.1"
    components.port = 8080
#elseif DEBUG
    components.scheme = "wss"
    components.host = "safehill-stage-1-ec0cd53b3592.herokuapp.com"
    components.port = 443
#else
    components.scheme = "wss"
    components.host = "app.safehill.io"
    components.port = 443
#endif
    
    return components
}()


internal var SafehillServerDefaultURLSessionConfiguration: URLSessionConfiguration {
    let configuration = URLSessionConfiguration.default

    /// Fail immediately if there is no connectivity. Re-attempts are managed in the app
    configuration.waitsForConnectivity = false

    /// How long (in seconds) a task should wait for additional data between packets
    configuration.timeoutIntervalForRequest = Double(SHDefaultNetworkTimeoutInMilliseconds / 1000)
    /// How long (in seconds) to wait for the entire request/response to complete
    configuration.timeoutIntervalForResource = Double(SHDefaultNetworkTimeoutInMilliseconds * 2 / 1000)

    /// API responses should not be cached
    configuration.requestCachePolicy = .reloadIgnoringLocalCacheData
    configuration.urlCache = nil

#if DEBUG
    /// Limit concurrent connections to avoid overwhelming ngrok if using that
    configuration.httpMaximumConnectionsPerHost = 2
#else
    /// Allow more concurrent connections in production
    configuration.httpMaximumConnectionsPerHost = 6
#endif

    /// Defaults to `false` to only allow WiFi/Ethernet. Set it to `true`
    configuration.allowsCellularAccess = true
    /// Set to `false` to prevent your app from using network interfaces that the system considers expensive. Set it to `true`
    configuration.allowsExpensiveNetworkAccess = true
    /// Indicates whether connections may use the network when the user has specified Low Data Mode.
    configuration.allowsConstrainedNetworkAccess = true

#if !DEBUG
    /// Use Wi-Fi and cellular simultaneously for better connectivity
    /// Disabled in DEBUG because multipath doesn't work with localhost/LocalStack
    if #available(iOS 11.0, *) {
        configuration.multipathServiceType = .handover
    }
#endif

    return configuration
}

internal var CDNServerDefaultURLSessionConfiguration: URLSessionConfiguration {
    let configuration = URLSessionConfiguration.default

#if DEBUG
    /// In DEBUG mode with ngrok, fail faster if no connectivity
    configuration.waitsForConnectivity = false
#else
    /// In production, wait for connectivity to become available for S3 transfers
    configuration.waitsForConnectivity = true
#endif

    /// How long (in seconds) a task should wait for additional data between packets
    configuration.timeoutIntervalForRequest = Double(SHDownloadTimeoutInMilliseconds / 1000)
    /// How long (in seconds) to wait for the entire download to complete
    configuration.timeoutIntervalForResource = Double(SHDownloadTimeoutInMilliseconds * 2 / 1000)

#if !DEBUG
    /// S3 downloads can be cached to improve performance (disabled in DEBUG for LocalStack)
    configuration.requestCachePolicy = .returnCacheDataElseLoad
    configuration.urlCache = URLCache(
        memoryCapacity: 50 * 1024 * 1024,  // 50 MB memory cache
        diskCapacity: 200 * 1024 * 1024,   // 200 MB disk cache
        diskPath: "safehill_s3_cache"
    )
#else
    /// In DEBUG, use default cache behavior for LocalStack compatibility
    configuration.requestCachePolicy = .useProtocolCachePolicy
#endif

#if DEBUG
    /// Very conservative limits for ngrok - only 1 connection at a time
    configuration.httpMaximumConnectionsPerHost = 1
#else
    /// In production, S3 can handle more parallel downloads
    configuration.httpMaximumConnectionsPerHost = 8
#endif

    /// Defaults to `false` to only allow WiFi/Ethernet. Set it to `true`
    configuration.allowsCellularAccess = true
    /// Set to `false` to prevent your app from using network interfaces that the system considers expensive. Set it to `true`
    configuration.allowsExpensiveNetworkAccess = true
    /// Indicates whether connections may use the network when the user has specified Low Data Mode.
    configuration.allowsConstrainedNetworkAccess = true

#if !DEBUG
    /// Use Wi-Fi and cellular simultaneously for better connectivity
    /// Disabled in DEBUG because multipath doesn't work with localhost/LocalStack
    if #available(iOS 11.0, *) {
        configuration.multipathServiceType = .handover
    }
#endif

    return configuration
}

internal func CDNServerDefaultBackgroundURLSessionConfiguration(with sessionIdentifier: String) -> URLSessionConfiguration {
    let configuration = URLSessionConfiguration.background(withIdentifier: sessionIdentifier)

    configuration.sharedContainerIdentifier = "group.com.gf.safehill.snoog"

    /// The session should wait for connectivity to become available, instead of fail immediately
    configuration.waitsForConnectivity = true

    /// How long (in seconds) a task should wait for additional data
    configuration.timeoutIntervalForRequest = Double(SHUploadTimeoutInMilliseconds / 1000)
    /// How long (in seconds) to wait for a complete resource to transfer before giving up
    configuration.timeoutIntervalForResource = 60 * 60 * 24 // 24 hours

    /// Background transfers are not time-critical, let the system schedule them
    configuration.isDiscretionary = false  // Set to true if uploads can be deferred

#if DEBUG
    /// Limit concurrent connections even for background to avoid overwhelming ngrok
    configuration.httpMaximumConnectionsPerHost = 1
#else
    /// In production, allow more concurrent background uploads
    configuration.httpMaximumConnectionsPerHost = 4
#endif

    /// Defaults to `false` to only allow WiFi/Ethernet. Set it to `true`
    configuration.allowsCellularAccess = true
    /// Set to `false` to prevent your app from using network interfaces that the system considers expensive. Set it to `true`
    configuration.allowsExpensiveNetworkAccess = true
    /// Indicates whether connections may use the network when the user has specified Low Data Mode.
    configuration.allowsConstrainedNetworkAccess = true

#if !DEBUG
    /// Use Wi-Fi and cellular simultaneously for better connectivity
    /// Disabled in DEBUG because multipath doesn't work with localhost/LocalStack
    if #available(iOS 11.0, *) {
        configuration.multipathServiceType = .handover
    }
#endif

    return configuration
}
