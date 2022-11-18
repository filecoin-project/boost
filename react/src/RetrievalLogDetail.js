/* global BigInt */

import React, {useEffect, useState} from "react";
import {useMutation, useQuery} from "@apollo/react-hooks";
import {
    DealCancelMutation,
    DealFailPausedMutation,
    DealRetryPausedMutation,
    RetrievalLogQuery,
} from "./gql";
import {useNavigate, useParams, Link} from "react-router-dom";
import {dateFormat} from "./util-date";
import moment from "moment";
import {addCommas, humanFIL, humanFileSize} from "./util";
import './RetrievalLogDetail.css'
import closeImg from './bootstrap-icons/icons/x-circle.svg'
import {Info} from "./Info";
import {addClassFor} from "./util-ui";

export function RetrievalLogDetail(props) {
    const params = useParams()
    const navigate = useNavigate()

    // Add a class to the document body when showing the deal detail page
    useEffect(() => {
        document.body.classList.add('modal-open')

        return function () {
            document.body.classList.remove('modal-open')
        }
    })

    var popupTimeout
    function showPopup(msg) {
        clearTimeout(popupTimeout)
        const el = document.body.querySelector('.content .popup')
        popupTimeout = addClassFor(el, 'showing', 2000)
        const msgEl = document.body.querySelector('.content .popup .message')
        msgEl.textContent = msg
    }

    function dealIDToClipboard() {
        navigator.clipboard.writeText(retrieval.DealID+'')
        const el = document.body.querySelector('.content .title .copy')
        addClassFor(el, 'copied', 500)
        showPopup("Copied " + retrieval.DealID + " to clipboard")
    }

    const {loading, error, data} = useQuery(RetrievalLogQuery, {
        pollInterval: 1000,
        variables: {
            peerID: params.peerID,
            dealID: params.dealID,
        },
        fetchPolicy: 'network-only',
    })

    if (error) {
        return <div>Error: {error.message}</div>
    }

    if (loading) {
        return <div>Loading ...</div>
    }

    var retrieval = data.retrievalLog

    var evtRowData = []
    var evts = (retrieval.DTEvents || []).sort((a, b) => a.CreatedAt.getTime() - b.CreatedAt.getTime())
    for (var i = 0; i < evts.length; i++) {
        var evt = retrieval.DTEvents[i]
        var prev = i === 0 ? null : retrieval.DTEvents[i - 1]
        evtRowData.push({evt: evt, prev: prev})
    }

    return <div className="retrieval-log-detail modal" id={retrieval.PeerID + '/' + retrieval.DealID}>
        <div className="content">
            <div className="close" onClick={() => navigate(-1)}>
                <img className="icon" alt="" src={closeImg} />
            </div>
            <div className="popup">
                <div className="message"></div>
            </div>
            <div className="title">
                <span>Retrieval {retrieval.DealID + ''}</span>
                <span className="copy" onClick={dealIDToClipboard} title="Copy deal uuid to clipboard"></span>
            </div>
            <table className="retrieval-fields">
                <tbody>
                <tr>
                    <th>CreatedAt</th>
                    <td>
                        {moment(retrieval.CreatedAt).format(dateFormat)}
                        &nbsp;
                        <span className="aux">({moment(retrieval.CreatedAt).fromNow()} ago)</span>
                    </td>
                </tr>
                <tr>
                    <th>Client Peer ID</th>
                    <td>{retrieval.PeerID}</td>
                </tr>
                <tr>
                    <th>Deal ID</th>
                    <td>{retrieval.DealID+''}</td>
                </tr>
                <tr>
                    <th>Deal Data Root CID</th>
                    <td><Link to={'/inspect/'+retrieval.PayloadCID}>{retrieval.PayloadCID}</Link></td>
                </tr>
                <tr>
                    <th>Piece CID</th>
                    <td><Link to={'/inspect/'+retrieval.PieceCid}>{retrieval.PieceCid}</Link></td>
                </tr>
                <tr>
                    <th>Price per byte</th>
                    <td>{humanFIL(retrieval.PricePerByte)}</td>
                </tr>
                <tr>
                    <th>Unseal price</th>
                    <td>{humanFIL(retrieval.UnsealPrice)}</td>
                </tr>
                <tr>
                    <th>Payment Interval</th>
                    <td>
                        {humanFileSize(retrieval.PaymentInterval)}
                        &nbsp;
                        <span className="aux">({addCommas(retrieval.PaymentInterval)} bytes)</span>
                    </td>
                </tr>
                <tr>
                    <th>Payment Interval Increase</th>
                    <td>
                        {humanFileSize(retrieval.PaymentIntervalIncrease)}
                        &nbsp;
                        <span className="aux">({addCommas(retrieval.PaymentIntervalIncrease)} bytes)</span>
                    </td>
                </tr>
                <tr>
                    <th>Sent</th>
                    <td>
                        {humanFileSize(retrieval.TotalSent)}
                        &nbsp;
                        <span className="aux">({addCommas(retrieval.TotalSent)} bytes)</span>
                    </td>
                </tr>
                <tr>
                    <th>Status</th>
                    <td>
                        {retrieval.Status + (retrieval.Message ? ': '+ retrieval.Message : '')}
                    </td>
                </tr>

                </tbody>
            </table>

            <h3>Data Transfer Logs</h3>

            <table className="retrieval-stages">
                <tbody>
                {evtRowData.map((l, i) => <RetrievalDTEvent key={i} evt={l.evt} prev={l.prev}/>)}
                </tbody>
            </table>
        </div>
    </div>
}

function RetrievalDTEvent(props) {
    var prev = props.prev
    var evt = props.evt
    var sinceLast = ''
    var sinceScale = ''
    if (prev != null) {
        var logMs = evt.CreatedAt.getTime()
        var prevMs = prev.CreatedAt.getTime()
        var deltaMillis = logMs - prevMs
        if (deltaMillis < 1000) {
            sinceScale = 'since-ms'
            sinceLast = (logMs - prevMs) + 'ms'
        } else {
            sinceLast = moment(prev.CreatedAt).from(evt.CreatedAt)
            if (deltaMillis < 10000) {
                sinceScale = 'since-s'
            } else {
                sinceScale = 'since-multi-s'
            }
        }
    }

    return <>
        <tr className={'retrieval-dt-event ' + sinceScale}>
            <td className="at">{moment(evt.CreatedAt).format(dateFormat)}</td>
            <td className="since-last">{sinceLast}</td>
            <td className="log-line">
                {evt.Name}{evt.Message ? ':' + evt.Message : null}
            </td>
        </tr>
    </>
}
