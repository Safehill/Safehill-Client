//
//  Crons.swift
//  
//
//  Created by Gennaro Frazzingaro on 8/3/21.
//

import Foundation
import CoreFoundation

internal func KBScheduleRoutine(withRetryInterval interval: TimeInterval,
                                onQueue queue: DispatchQueue? = nil,
                                when time: DispatchTime = .now(),
                                _ routine: @escaping () -> ()) {
    let timer = DispatchSource.makeTimerSource(queue: queue)
    timer.schedule(deadline: time, repeating: interval)
    timer.setEventHandler(handler: routine)
    timer.resume()
}
