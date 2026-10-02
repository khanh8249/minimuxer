//
//  HeartbeatService.swift
//  Minimuxer
//
//  Original Rust Implementation by @jkcoxson
//  Swift Port created by Magesh K on 02/03/26.
//

import Foundation
internal import MinimuxerCommon
internal import DeviceGatewayAPI

final internal class HeartbeatService {
    let deviceProvider: DeviceProvider
    var gateway: any DeviceGatewayAPI {
        deviceProvider.gateway
    }
    let proxyServer: UsbmuxdProxyServer
    let endpoint: DeviceEndpoint

    private let sleepNs: UInt64 = MinimuxerConstants.heartbeatInterval * 1_000_000

    init(deviceProvider: DeviceProvider, proxyServer: UsbmuxdProxyServer, endpoint: DeviceEndpoint) {
        self.deviceProvider = deviceProvider
        self.proxyServer = proxyServer
        self.endpoint = endpoint
    }

    private actor MutableState {
        var running = false
        var taskActive = false

        func tryStart() -> Bool {
            if taskActive {
                running = true
                return false
            }
            running = true
            taskActive = true
            return true
        }

        func stop() {
            running = false
        }

        func terminate() {
            taskActive = false
            running = false
        }
    }

    private let state = MutableState()
    private var lastErrorDescription: String?

    //  CHANGED: property ghi bi heartbeat task, c bi MinimuxerImpl.
    // Ch c 1 writer (heartbeat task), reader ch poll bool  chp nhn c.
    var lastBeatSuccessful = false

    //  NEW: task handle  cancel sch khi stop().
    private var heartbeatTask: Task<Void, Never>?

    /// Trng thi task  dng cho isReady() gate (optional).
    var isRunning: Bool {
        get async {
            await state.with { $0.running }
        }
    }

    // MARK: - Lifecycle

    /// Start the heartbeat loop. Ignored if a task is already active.
    func start() async {
        guard await state.tryStart() else {
            return
        }

        verboseLog("[minimuxer] Starting heartbeat task...")
        let task = Task.detached { [weak self] in
            guard let self = self else { return }
            verboseLog("[minimuxer] heartbeat-task: started")

            await self.heartbeatLoop()

            await self.state.terminate()
            self.lastBeatSuccessful = false
            verboseLog("[minimuxer] heartbeat-task: stopped")
        }
        self.heartbeatTask = task
    }

    /// Signal the heartbeat task to stop. Will exit on next iteration.
    /// Also cancels the task handle for faster shutdown.
    func stop() async {
        await state.stop()
        lastBeatSuccessful = false
        heartbeatTask?.cancel()
        heartbeatTask = nil
        verboseLog("[minimuxer] HeartbeatService stop requested")
    }

    // MARK: - Internals

    private func logIfNeeded(_ message: String, isVerbose: Bool = false) {
        if message != lastErrorDescription {
            if isVerbose {
                verboseLog("[minimuxer] heartbeat-task: \(message)")
            } else {
                debugLog("[minimuxer] heartbeat-task: \(message)")
            }
            lastErrorDescription = message
        }
    }

    private func heartbeatLoop() async {
        if self.gateway.requiresUsbmuxd {
            while !self.proxyServer.isListening {
                if Task.isCancelled { return }
                logIfNeeded("Waiting for usbmuxd to be ready...", isVerbose: true)
                try? await Task.sleep(nanoseconds: sleepNs)
            }
            verboseLog("[minimuxer] heartbeat-task: usbmuxd is ready")
        }

        var currentInterval: UInt64 = MinimuxerConstants.heartbeatInterval

        //  CHANGED: thm Task.isCancelled  thot sch khi stop() cancel task.
        while await state.running && !Task.isCancelled {
            let tunnelPeerIp: String
            do {
                tunnelPeerIp = try await self.endpoint.ip()
            } catch {
                logIfNeeded("device IP unavailable", isVerbose: true)
                lastBeatSuccessful = false
                try? await Task.sleep(nanoseconds: sleepNs)
                continue
            }

            // verify tunnel/device reachability first
            let targetPort = self.gateway.servicePort
            if !NetworkUtils.testTCP(ip: tunnelPeerIp, port: targetPort) {
                logIfNeeded("device IP not reachable, waiting...", isVerbose: true)
                lastBeatSuccessful = false
                try? await Task.sleep(nanoseconds: sleepNs)
                continue
            }

            do {
                currentInterval = try await self.gateway.performHeartbeat(interval: currentInterval)
                lastBeatSuccessful = true
                lastErrorDescription = nil
            } catch {
                logIfNeeded("Heartbeat failed: \(error)")
                lastBeatSuccessful = false
                try? await Task.sleep(nanoseconds: sleepNs)
            }
        }
    }
}
