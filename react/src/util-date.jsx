/* global BigInt */
import moment from "moment";

moment.updateLocale('en', {
    relativeTime: {
        future: "in %s",
        past:   "%s",
        s  : '%ds',
        ss : '%ds',
        m:  "1m",
        mm: "%dm",
        h:  "1h",
        hh: "%dh",
        d:  "a day",
        dd: "%d days",
        w:  "a week",
        ww: "%d weeks",
        M:  "a month",
        MM: "%d months",
        y:  "a year",
        yy: "%d years"
    }
})

export var dateFormat = 'YYYY-MM-DD HH:mm:ss.SSS'

const ONE_MINUTE = 60 * 1000
const ONE_HOUR = 60 * ONE_MINUTE

export function durationNanoToString(durationNano) {
    const durationMillis = Number(durationNano / BigInt(1e6))
    var durationDisplay = (durationMillis / ONE_HOUR) + 'h'
    if (durationMillis < ONE_HOUR) {
        durationDisplay = (durationMillis / ONE_MINUTE) + 'm'
    }
    return durationDisplay
}
