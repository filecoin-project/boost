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

var dateFormat = 'YYYY-MM-DD HH:mm:ss.SSS'

export {
    dateFormat
}