var config
var serverHttpEndpoint = window.location.origin

export async function initConfig() {
    var serverEndpoint = window.location.host
    serverHttpEndpoint = window.location.origin

    if (import.meta.env.DEV) {
        serverEndpoint = 'localhost:8080'
        serverHttpEndpoint = 'http://' + serverEndpoint
    }

    const res = await fetch(serverHttpEndpoint+'/config.json')
    config = await res.json()
}

export function setConfig(cfg) {
    config = cfg
}

export function getConfig() {
    return config
}

export function getServerHttpEndpoint() {
    return serverHttpEndpoint
}
