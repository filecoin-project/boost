/* global BigInt */
// TODO: Figure out if there's a way to hook into apollo to do this
// in a nicer way
import {
    useSubscription as apolloUseSubscription,
    useQuery as apolloUseQuery,
    useMutation as apolloUseMutation,
    useLazyQuery as apolloUseLazyQuery,
} from "@apollo/react-hooks";

var useSubscription = function(...args) {
    var ret = apolloUseSubscription.apply(null, args)
    if (ret.data) {
        customParse(ret.data)
    }
    return ret
}
var useQuery = function(...args) {
    var ret = apolloUseQuery.apply(null, args)
    if (ret.data) {
        customParse(ret.data)
    }
    return ret
}
var useLazyQuery = function(...args) {
    var ret = apolloUseLazyQuery.apply(null, args)
    if (ret.data) {
        customParse(ret.data)
    }
    return ret
}
var useMutation = function(...args) {
    var ret = apolloUseMutation.apply(null, args)
    if (ret.data) {
        customParse(ret.data)
    }
    return ret
}

var dateRegExp = /[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]/
function customParse(obj, parent, parentKey) {
    for (let key in obj) {
        if (obj.hasOwnProperty(key)) {
            if (typeof obj === 'string') {
                // If the string is a date, convert it to a date object
                if (obj.match(dateRegExp)) {
                    parent[parentKey] = new Date(obj)
                }
            } else if (typeof obj === 'object') {
                // If the object represents a BigInt, convert it to a BigInt
                if (obj.__typename === 'BigInt') {
                    parent[parentKey] = BigInt(obj.n)
                    return
                }
                customParse(obj[key], obj, key)
            }
        }
    }
    return obj
}

export {
    useQuery,
    useLazyQuery,
    useSubscription,
    useMutation,
    customParse,
};