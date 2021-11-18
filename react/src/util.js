// TODO: Check licensing
// Copied from
// https://stackoverflow.com/questions/10420352/converting-file-size-in-bytes-to-human-readable-string
export function humanFileSize(bytes, si = false, dp = 1) {
    const thresh = si ? 1000 : 1024;

    if (Math.abs(bytes) < thresh) {
        return bytes + ' B';
    }

    const units = si
        ? ['kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
        : ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];
    let u = -1;
    const r = 10 ** dp;

    do {
        bytes /= thresh;
        ++u;
    } while (Math.round(Math.abs(bytes) * r) / r >= thresh && u < units.length - 1);

    return bytes.toFixed(dp) + ' ' + units[u];
}

var oneFil = 1e18
var oneNanoFil = 1e9
export function humanFIL(atto) {
    // 10^18
    if (atto > oneFil / 1000) {
        let res = (atto / oneFil).toFixed(3)
        res = res.replace(/0+$/, '')
        res = res.replace(/\.$/, '')
        return res + ' FIL'
    }
    // 10^9
    if (atto > oneNanoFil / 1000) {
        let res = (atto / oneNanoFil).toFixed(3)
        res = res.replace(/0+$/, '')
        res = res.replace(/\.$/, '')
        return res + ' nano'
    }
    return atto + ' atto'
}

export function shortDealID(dealID) {
    return dealID.substring(dealID.length-6)
}
