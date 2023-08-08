/* global BigInt */
import {useQuery} from "@apollo/react-hooks";
import moment from "moment";
import React, {useEffect, useState} from "react";
import {ExpandableJSObject, PageContainer, ShortCIDTail, ShortPeerID} from "./Components";
import {Link, useNavigate, useParams} from "react-router-dom";
import {dateFormat, durationNanoToString} from "./util-date";
import {TimestampFormat} from "./timestamp";
import './Health.css'
import {Pagination} from "./Pagination";
import {humanFileSize} from "./util";
import {addClassFor} from "./util-ui";
import closeImg from "./bootstrap-icons/icons/x-circle.svg";
import activityImg from "./bootstrap-icons/icons/activity.svg";
import {getConfig} from "./config"

const basePath = '/health'

export function HealthPage(props) {
    return (
    <PageContainer pageType="health" title="Services Health">
        <ServicesHealthContent />
    </PageContainer>
    )
}

export function HealthMenuItem(props) {
    return (
        <div className="menu-item" >
            <img className="icon" alt="" src={activityImg} />
            <Link key="health" to={basePath}>
                <h3>Services Health</h3>
                <div className="menu-desc">
                    <b>4</b> healthy <br />
                    <b>0</b> dead
                </div>
            </Link>
        </div>
    )
}

function ServicesHealthContent() {
    var services = [
      {"name": "booster-http", "healthy": true},
      {"name": "boostd", "healthy": false},
      {"name": "booster-bitswap", "healthy": true},
      {"name": "boostd-data", "healthy": true}
    ]

    return (
      <div>
        <h3>Services Health</h3>

        <table className="services">
            <tbody>
            {services.map(s => (
                <tr key={s.name}>
                    <td className="color">{(s.healthy ? 'green' : 'red')}</td>
                    <td className="state">{s.name}</td>
                    <td className="health">{(s.healthy ? 'healthy' : 'dead')}</td>
                </tr>
            ))}
            </tbody>
        </table>

      </div>
    )
}
