import React from "react";
import './MonitoringAlert.css'
import warnImg from "./bootstrap-icons/icons/exclamation-circle.svg"
import {Link} from "react-router-dom";
import {useQuery} from "@apollo/client";
import {MpoolQuery} from "./gql";

export function MonitoringAlert(props) {
    const alerts = true
    const {data} = useQuery(MpoolQuery, { variables: { alerts },
        pollInterval: 10000,
        fetchPolicy: `network-only`,
    })

    var count = 0
    if (data) {
        count = data.mpool.Count
    }
    if (count < 1) {
        return null
    }

    return (
        <div id="monitoring-alert" className="showing">
            <div className="message">
                <img src={warnImg}  alt={"warning"}/>
                <span>{count} messages stuck in <Link to="/mpool">Message Pool</Link></span>
            </div>
        </div>
    )
}

export function DeprecationAlert(props) {
    return (
        <div id="monitoring-alert" className="showing">
            <div className="message">
                <img src={warnImg}  alt={"warning"}/>
                <span>
                    Boost is deprecated and will no longer be supported in the future. Please{' '}
                    <a href="https://docs.curiostorage.org/curio-market/migrating-from-boost" target="_blank" rel="noopener noreferrer">migrate</a> to{' '}
                    <a href="https://docs.curiostorage.org" target="_blank" rel="noopener noreferrer">Curio</a>.
                </span>
            </div>
        </div>
    )
}
