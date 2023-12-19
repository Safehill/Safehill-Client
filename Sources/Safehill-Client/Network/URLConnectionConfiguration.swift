import Foundation

public let SafehillServerURLComponents: URLComponents = {
    var components = URLComponents()
    
#if targetEnvironment(simulator)
    components.scheme = "http"
    components.host = "127.0.0.1"
    components.port = 8080
    /// If using ngrok locally, comment the lines above, uncomment the ones below and change the hostname
//    components.scheme = "https"
//    components.host = "bc5f-2600-6c4e-2200-3f73-7964-edc9-ddd9-b376.ngrok-free.app"
//    components.port = 443
#else
    components.scheme = "https"
    components.host = "app.safehill.io"
    components.port = 443
#endif
    
    return components
}()


internal var SafehillServerDefaultURLSessionConfiguration: URLSessionConfiguration {
    let configuration = URLSessionConfiguration.default
    
    /// The session should wait for connectivity to become available, instead of fail immediately
    configuration.waitsForConnectivity = true
    
    /// How long (in seconds) a task should wait for additional data
    configuration.timeoutIntervalForRequest = 60 // 1 minute
    /// How long (in seconds) to wait for a complete resource to transfer before giving up
    configuration.timeoutIntervalForResource = 60 * 60 // 1 hour
    
    /// Defaults to `false` to only allow WiFi/Ethernet. Set it to `true`
    configuration.allowsCellularAccess = true
    /// Set to `false` to prevent your app from using network interfaces that the system considers expensive. Set it to `true`
    configuration.allowsExpensiveNetworkAccess = true
    /// Indicates whether connections may use the network when the user has specified Low Data Mode.
    configuration.allowsConstrainedNetworkAccess = true
    
    return configuration
}

internal func CDNServerDefaultBackgroundURLSessionConfiguration(with sessionIdentifier: String) -> URLSessionConfiguration {
    let configuration = URLSessionConfiguration.background(withIdentifier: sessionIdentifier)
    
    /// The session should wait for connectivity to become available, instead of fail immediately
    configuration.waitsForConnectivity = true
    
    /// How long (in seconds) a task should wait for additional data
    configuration.timeoutIntervalForRequest = 60 // 1 minute
    /// How long (in seconds) to wait for a complete resource to transfer before giving up
    configuration.timeoutIntervalForResource = 60 * 60 * 24 // 24 hours
    
    /// Defaults to `false` to only allow WiFi/Ethernet. Set it to `true`
    configuration.allowsCellularAccess = true
    /// Set to `false` to prevent your app from using network interfaces that the system considers expensive. Set it to `true`
    configuration.allowsExpensiveNetworkAccess = true
    /// Indicates whether connections may use the network when the user has specified Low Data Mode.
    configuration.allowsConstrainedNetworkAccess = true
    
    return configuration
}