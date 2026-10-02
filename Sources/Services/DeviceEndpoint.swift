//
//  DeviceEndpoint.swift
//  Minimuxer
//
//  Original Rust Implementation by @jkcoxson
//  Swift Port created by Magesh K on 02/03/26.
//  Copyright © 2026 SideStore. All rights reserved.
//

import Foundation
internal import DeviceGatewayAPI

actor DeviceEndpoint {

    let deviceProvider: DeviceProvider
    var gateway: any DeviceGatewayAPI {
        deviceProvider.gateway
    }

    private var ipAddr: String? = nil

    init(deviceProvider: DeviceProvider) {
        self.deviceProvider = deviceProvider
    }

    func ip() throws -> String {
        guard let ip = ipAddr else { throw MinimuxerInternalError.deviceEndpointNotInitialized }
        return ip
    }

    // ⬇️ CHANGED: return Bool = true nếu giá trị thay đổi.
    @discardableResult
    func update(_ newIP: String) -> Bool {
        let changed = (ipAddr != newIP)
        ipAddr = newIP
        self.gateway.setDeviceEndpointIp(newIP)
        verboseLog("[minimuxer] device endpoint updated -> \(newIP) (changed: \(changed))")
        return changed
    }

    // ⬇️ CHANGED: return Bool = true nếu trước đó có giá trị.
    @discardableResult
    func clear() -> Bool {
        let wasSet = (ipAddr != nil)
        ipAddr = nil
        self.gateway.setDeviceEndpointIp(nil)
        verboseLog("[minimuxer] device endpoint cleared -> nil (wasSet: \(wasSet))")
        return wasSet
    }

    var isInitialized: Bool {
        ipAddr != nil
    }
}
