import React from "react";
import './MonitoringAlert.css'
import warnImg from "./bootstrap-icons/icons/exclamation-circle.svg"
import {Link} from "react-router-dom";
import {useQuery} from "@apollo/react-hooks";

export function MonitoringAlert(props) {
    // const {data} = useQuery(MpoolAlertCountQuery, {
    //     pollInterval: 10000,
    //     fetchPolicy: 'network-only',
    // })
    const data = { mpoolAlertCount: 0 }

    var count = 0
    if (data) {
        count = data.mpoolAlertCount
    }
    if (!count) {
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
