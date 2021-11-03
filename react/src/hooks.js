// TODO: Figure out if there's a way to hook into apollo to do this
// in a nicer way
import { useSubscription as apolloUseSubscription, useQuery as apolloUseQuery } from "@apollo/react-hooks";

var useSubscription = function(...args) {
    var ret = apolloUseSubscription.apply(null, args)
    if (ret.data) {
        parseDates(ret.data)
    }
    return ret
}
var useQuery = function(...args) {
    var ret = apolloUseQuery.apply(null, args)
    if (ret.data) {
        parseDates(ret.data)
    }
    return ret
}

var dateRegExp = /[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]/
function parseDates(obj, parent, parentKey) {
    for (let key in obj) {
        if (obj.hasOwnProperty(key)) {
            if (typeof obj === 'string') {
                if (obj.match(dateRegExp)) {
                    parent[parentKey] = new Date(obj)
                }
            } else if (typeof obj === 'object') {
                parseDates(obj[key], obj, key)
            }
        }
    }
    return obj
}

export {
    useQuery,
    useSubscription,
};