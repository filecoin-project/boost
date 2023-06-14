import React from "react";
import './MonitoringAlert.css'
import warnImg from "./bootstrap-icons/icons/exclamation-circle.svg"
import {Link} from "react-router-dom";
import {useQuery} from "@apollo/react-hooks";
import {MpoolAlertsQuery} from "./gql";

export function MonitoringAlert(props) {
    const {data} = useQuery(MpoolAlertsQuery, {
        pollInterval: 10000,
        fetchPolicy: `network-only`,
    })

    var count = 0
    if (data) {
        count = data.mpoolAlertsCount
    }
    if (count < 1) {
        return null
    }

    return (
        <div id="monitoring-alert" className="showing">
            <div className="message">
                <img src={warnImg} />
                <span>{count} messages stuck in <Link to="/mpool">Message Pool</Link></span>
            </div>
        </div>
    )
}
