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
    components.host = "7f6d-2601-645-c580-55eb-00-1002.ngrok-free.app"
//    components.host = "safehill-stage-1-ec0cd53b3592.herokuapp.com"
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
    components.host = "7f6d-2601-645-c580-55eb-00-1002.ngrok-free.app"
//    components.host = "safehill-stage-1-ec0cd53b3592.herokuapp.com"
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
    
    /// Defaults to `false` to only allow WiFi/Ethernet. Set it to `true`
    configuration.allowsCellularAccess = true
    /// Set to `false` to prevent your app from using network interfaces that the system considers expensive. Set it to `true`
    configuration.allowsExpensiveNetworkAccess = true
    /// Indicates whether connections may use the network when the user has specified Low Data Mode.
    configuration.allowsConstrainedNetworkAccess = true
    
    return configuration
}

internal var CDNServerDefaultURLSessionConfiguration: URLSessionConfiguration {
    let configuration = URLSessionConfiguration.default
    
    /// Fail immediately if there is no connectivity. Re-attempts are managed in the app
    configuration.waitsForConnectivity = true
    
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
    configuration.timeoutIntervalForRequest = Double(SHUploadTimeoutInMilliseconds / 1000)
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
