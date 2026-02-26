import React, {useEffect} from "react";
import {useQuery} from "@apollo/client";
import { RetrievalLogQuery } from "./gql";
import {useNavigate, useParams, Link} from "react-router-dom";
import {dateFormat} from "./util-date";
import moment from "moment";
import {addCommas, humanFIL, humanFileSize} from "./util";
import './RetrievalLogDetail.css'
import closeImg from './bootstrap-icons/icons/x-circle.svg'
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

    function transferIDToClipboard() {
        navigator.clipboard.writeText(retrieval.TransferID+'')
        const el = document.body.querySelector('.content .title .copy')
        addClassFor(el, 'copied', 500)
        showPopup("Copied " + retrieval.DealID + " to clipboard")
    }

    const {loading, error, data} = useQuery(RetrievalLogQuery, {
        pollInterval: 10000,
        variables: {
            peerID: params.peerID,
            transferID: params.transferID,
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
    const dtEvts = (retrieval.DTEvents || []).map(e => Object.assign({}, e, {EventType: 'data-transfer'}))
    const mktEvts = (retrieval.MarketEvents || []).map(e => Object.assign({}, e, {EventType: 'market'}))
    const evts = [...dtEvts, ...mktEvts].sort((a, b) => a.CreatedAt.getTime() - b.CreatedAt.getTime())
    for (var i = 0; i < evts.length; i++) {
        var evt = evts[i]
        var prev = i === 0 ? null : evts[i - 1]
        evtRowData.push({evt: evt, prev: prev})
    }

    return <div className="retrieval-log-detail modal" id={retrieval.PeerID + '/' + retrieval.TransferID}>
        <div className="content">
            <div className="close" onClick={() => navigate(-1)}>
                <img className="icon" alt="" src={closeImg} />
            </div>
            <div className="popup">
                <div className="message"></div>
            </div>
            <div className="title">
                <span>Retrieval {retrieval.TransferID + ''}</span>
                <span className="copy" onClick={transferIDToClipboard} title="Copy transfer ID to clipboard"></span>
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
                    <th>Transfer ID</th>
                    <td>{retrieval.TransferID+''}</td>
                </tr>

                {/* If retrieval deal ID is zero, assume it's an indexer retrieval */}
                {retrieval.DealID === 0 ? (
                    <tr>
                        <th>Ad CID</th>
                        <td>{retrieval.PayloadCID}</td>
                    </tr>
                ) : (
                    <>
                        <tr>
                            <th>Retrieval Deal ID</th>
                            <td>{retrieval.DealID+''}</td>
                        </tr>
                        <tr>
                            <th>Deal Data Root CID</th>
                            <td><Link to={'/piece-doctor/'+retrieval.PayloadCID}>{retrieval.PayloadCID}</Link></td>
                        </tr>
                        <tr>
                            <th>Piece CID</th>
                            <td><Link to={'/piece-doctor/'+retrieval.PieceCid}>{retrieval.PieceCid}</Link></td>
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
                                {getDealStatus(retrieval.Status) + (retrieval.Message ? ': '+ retrieval.Message : '')}
                            </td>
                        </tr>
                    </>
                )}

                </tbody>
            </table>

            <h3>Event Logs</h3>

            <table className="retrieval-events">
                <tbody>
                {evtRowData.map((l, i) => {
                    return <RetrievalEvent key={l.evt.EventType+l.evt.CreatedAt.getTime()} evt={l.evt} prev={l.prev}/>
                })}
                </tbody>
            </table>
        </div>
    </div>
}

function RetrievalEvent(props) {
    var prev = props.prev
    var evt = props.evt
    var sinceLast = ''
    var sinceScale = ''
    if (prev != null) {
        var logMs = evt.CreatedAt.getTime()
        var prevMs = prev.CreatedAt.getTime()
        var deltaMillis = logMs - prevMs
        if (deltaMillis < 0) {
            console.log(deltaMillis, 'prv', prev, 'evt', evt)
        }
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
        <tr className={'retrieval-event ' + sinceScale}>
            <td className="at">{moment(evt.CreatedAt).format(dateFormat)}</td>
            <td className="since-last">{sinceLast}</td>
            <td className={"event " + evt.EventType}>
                {evt.EventType === 'data-transfer' ? 'DT:' : null}
                {getEventName(evt.Name)}
            </td>
            <td className="status">{getDealStatus(evt.Status)}</td>
            <td className="message">{evt.Message}</td>
        </tr>
    </>
}

export function getDealStatus(dealStatus) {
    return (dealStatus || '').replace('DealStatus', '')
}

export function getEventName(evtName) {
    return (evtName || '').replace('ProviderEvent', '')
}
