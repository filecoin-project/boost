import React, {useEffect} from "react";
import {useMutation, useSubscription} from "@apollo/react-hooks";
import {DealCancelMutation, DealSubscription} from "./gql";
import {useNavigate} from "react-router-dom";
import {dateFormat} from "./util-date";
import moment from "moment";
import {humanFIL, addCommas, humanFileSize} from "./util";
import {useParams} from "react-router-dom";
import './DealDetail.css'
import closeImg from './bootstrap-icons/icons/x-circle.svg'

export function DealDetail(props) {
    const params = useParams()
    const navigate = useNavigate()

    // Add a class to the document body when showing the deal detail page
    useEffect(() => {
        document.body.classList.add('modal-open')

        return function () {
            document.body.classList.remove('modal-open')
        }
    })

    const [cancelDeal] = useMutation(DealCancelMutation, {
        variables: {id: params.dealID}
    })

    const {loading, error, data} = useSubscription(DealSubscription, {
        variables: {id: params.dealID},
    })

    if (error) {
        return <div>Error: {error.message}</div>
    }

    if (loading) {
        return <div>Loading ...</div>
    }

    var deal = data.dealUpdate
    var logRowData = []
    for (var i = 0; i < (deal.Logs || []).length; i++) {
        var log = deal.Logs[i]
        var prev = i === 0 ? null : deal.Logs[i - 1]
        logRowData.push({log: log, prev: prev})
    }

    return <div className="deal-detail modal" id={deal.ID}>
        <div className="content">
            <div className="close" onClick={() => navigate(-1)}>
                <img className="icon" alt="" src={closeImg} />
            </div>
            <div className="title">Deal {deal.ID}</div>
            <table className="deal-fields">
                <tbody>
                <tr>
                    <th>CreatedAt</th>
                    <td>
                        {moment(deal.CreatedAt).format(dateFormat)}
                        &nbsp;
                        <span className="aux">({moment(deal.CreatedAt).fromNow()} ago)</span>
                    </td>
                </tr>
                <tr>
                    <th>Client Address</th>
                    <td>
                        <a href={"https://filfox.info/en/address/"+deal.ClientAddress} target="_blank" rel="noreferrer">
                            {deal.ClientAddress}
                        </a>
                    </td>
                </tr>
                <tr>
                    <th>Client Peer ID</th>
                    <td>{deal.ClientPeerID}</td>
                </tr>
                <tr>
                    <th>Deal Data Root CID</th>
                    <td>{deal.DealDataRoot}</td>
                </tr>
                <tr>
                    <th>Piece CID</th>
                    <td>{deal.PieceCid}</td>
                </tr>
                <tr>
                    <th>Piece Size</th>
                    <td>
                        {humanFileSize(deal.PieceSize)}
                        &nbsp;
                        <span className="aux">({addCommas(deal.PieceSize)} bytes)</span>
                    </td>
                </tr>
                <tr>
                    <th>Provider Collateral</th>
                    <td>{humanFIL(deal.ProviderCollateral)}</td>
                </tr>
                <tr>
                    <th>Start Epoch</th>
                    <td>{deal.StartEpoch}</td>
                </tr>
                <tr>
                    <th>End Epoch</th>
                    <td>{deal.EndEpoch}</td>
                </tr>
                <tr>
                    <th>Transfer Type</th>
                    <td>{deal.Transfer.Type}</td>
                </tr>
                <tr>
                    <th>Transfer Size</th>
                    <td>
                        {humanFileSize(deal.Transfer.Size)}
                        &nbsp;
                        <span className="aux">({addCommas(deal.Transfer.Size)} bytes)</span>
                    </td>
                </tr>
                {deal.Sector.ID > 0 ? (
                    <>
                    <tr>
                        <th>Sector ID</th>
                        <td>{deal.Sector.ID + ''}</td>
                    </tr>
                    <tr>
                        <th>Sector Data Offset</th>
                        <td>{addCommas(deal.Sector.Offset)}</td>
                    </tr>
                    <tr>
                        <th>Sector Data Length</th>
                        <td>{addCommas(deal.Sector.Length)}</td>
                    </tr>
                    </>
                ) : null}
                <tr>
                    <th>Publish Message CID</th>
                    <td>
                        <a href={"https://filfox.info/en/message/"+deal.PublishCid} target="_blank" rel="noreferrer">
                        {deal.PublishCid}
                        </a>
                    </td>
                </tr>
                <tr>
                    <th>Status</th>
                    <td>{deal.Message}</td>
                </tr>
                </tbody>
            </table>

            {deal.Stage === 'Accepted' ? (
                <div className="buttons">
                    <div className="button cancel" onClick={cancelDeal}>Cancel Transfer</div>
                </div>
            ) : null}

            <h3>Deal Logs</h3>

            <table className="deal-logs">
                <tbody>
                {logRowData.map((l, i) => <DealLog key={i} log={l.log} prev={l.prev}/>)}
                </tbody>
            </table>
        </div>
    </div>
}

function DealLog(props) {
    var prev = props.prev
    var log = props.log
    var sinceLast = ''
    if (prev != null) {
        var logMs = log.CreatedAt.getTime()
        var prevMs = prev.CreatedAt.getTime()
        if (logMs - prevMs < 1000) {
            sinceLast = (logMs - prevMs) + 'ms'
        } else {
            sinceLast = moment(prev.CreatedAt).from(log.CreatedAt)
        }
    }

    var logParams = {}
    if (log.LogParams && typeof log.LogParams === 'string') {
        try {
            const params = JSON.parse(log.LogParams)
            for (let i = 0; i < params.length; i+=2) {
                var k = params[i]
                var v = params[i+1]
                if (typeof k !== "string") {
                    k = JSON.stringify(k)
                }
                if (typeof v === "object") {
                    v = JSON.stringify(v)
                }
                logParams[k] = v
            }
            delete logParams.id
        } catch(_) {
        }
    }
    
    return <tr>
        <td className="at">{moment(log.CreatedAt).format(dateFormat)}</td>
        <td className="since-last">{sinceLast}</td>
        <td className="log-line">
            <div className="message">{log.LogMsg}</div>
            {Object.keys(logParams).map((k, i) => (
                <div className="param" key={i}>
                    {k}: {logParams[k]}
                </div>
            ))}
        </td>
    </tr>
}
