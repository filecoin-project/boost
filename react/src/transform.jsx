/* global BigInt */

var dateRegExp = /[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]/
export function transformResponse(obj, parent, parentKey) {
    for (let key in obj) {
        if (obj.hasOwnProperty(key)) {
            if (typeof obj === 'string') {
                // If the string is a date, convert it to a date object
                if (obj.match(dateRegExp)) {
                    const asDate = new Date(obj)
                    if (!isNaN(asDate.valueOf())) {
                        parent[parentKey] = asDate
                    }
                }
            } else if (typeof obj === 'object') {
                // If the object represents a BigInt, convert it to a BigInt
                if (obj.__typename === 'BigInt') {
                    parent[parentKey] = BigInt(obj.n)
                    return
                }
                transformResponse(obj[key], obj, key)
            }
        }
    }
    return obj
}
