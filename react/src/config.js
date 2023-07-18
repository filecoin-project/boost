var config

export async function initConfig() {
    var serverEndpoint = window.location.host
    var serverHttpEndpoint = window.location.origin

    if (process.env.NODE_ENV === 'development') {
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
