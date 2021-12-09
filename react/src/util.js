/* global BigInt */

// When BigInts are sent over the wire, convert them to a string so as to
// avoid losing precision: Javascript can only handle numbers up to a size
// of 2^53 - 1
// eslint-disable-next-line
BigInt.prototype.toJSON = function() {
    return this.toString()
}

const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB']

export function humanFileSize(size, unitIndex = 0) {
    const isBigInt = typeof size == 'bigint'
    if (size < 1024) {
        size = isBigInt ? Number(size) : size
        var dec = size.toFixed(1).replace(/\.0+$/, '')
        return dec + ' ' + units[unitIndex]
    }

    if (isBigInt) {
        size = size / 1024n
        if (size <= Math.MAX_SAFE_INTEGER) {
            size = Number(size)
        }
    } else {
        size = size / 1024
    }
    return humanFileSize(size, unitIndex+1)
}

var oneNanoFil = 1e9
export const oneFil = BigInt(oneNanoFil)*BigInt(oneNanoFil)
export function humanFIL(atto) {
    atto = BigInt(atto)

    // 10^18
    if (atto > oneFil) {
        if (atto > 100n * oneFil) {
            return (atto / oneFil) + ' FIL' // 123 FIL
        }
        const fil = Number((1000n * atto) / oneFil) / 1000
        return toFixed(fil, 1) + ' FIL' // 12.3 FIL
    }

    // 10^15
    if (atto > oneFil / 1000n) {
        const fil = Number(BigInt(1e6)*atto / oneFil) / 1e6
        return toFixed(fil, 3) + ' FIL' // 0.123 FIL
    }

    // 10^9
    if (atto > oneNanoFil / 1000) {
        const nanoFil = (Number(atto) / oneNanoFil)
        return toFixed(nanoFil, 1) + ' nano' // 123.4 nano
    }

    return atto + ' atto' // 123 atto
}

function toFixed(num, fractionDigits) {
    return num.toFixed(fractionDigits).replace(/\.?0+$/, '')
}

export function shortDealID(dealID) {
    return dealID.substring(dealID.length-6)
}

export function addCommas(num) {
    let withCommas = ''
    const numstr = num + ''
    for (let i = 0; i < numstr.length; i++) {
        withCommas = numstr[numstr.length-1-i] + withCommas
        if (i % 3 === 2) {
            withCommas = ',' + withCommas
        }
    }
    return withCommas
}

export function max(...nums) {
    if (nums.length === 0) {
        return undefined
    }

    let m = nums[0]
    for (let i = 1; i < nums.length; i++) {
        if (nums[i] > m) {
            m = nums[i]
        }
    }
    return m
}


export function pow(num, power) {
    var res = 1n
    for (let i = 0; i < power; i++) {
        res = res * num
    }
    return res
}

export function parseFil(str) {
    var val = 0n

    const pointIndex = str.indexOf('.')
    var beforePoint = str
    if (pointIndex >= 0) {
        beforePoint = str.substring(0, pointIndex)
    }
    const fil = parseInt(beforePoint || '0')
    val += BigInt(fil) * oneFil

    if (pointIndex >= 0) {
        const afterPoint = str.substring(pointIndex + 1)
        for (let i = 0; i < afterPoint.length; i++) {
            val += BigInt(afterPoint[i]) * oneFil / pow(10n, BigInt(i+1))
        }
    }

    return val
}