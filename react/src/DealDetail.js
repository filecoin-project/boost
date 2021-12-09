import React, {useEffect} from "react";
import {useMutation, useSubscription} from "@apollo/react-hooks";
import {DealCancelMutation, DealSubscription} from "./gql";
import {dateFormat} from "./util-date";
import moment from "moment";
import {humanFIL, addCommas} from "./util";

export function DealDetail(props) {
    // Add a class to the document body when showing the deal detail page
    useEffect(() => {
        document.body.classList.add('modal-open')

        return function () {
            document.body.classList.remove('modal-open')
        }
    })

    const [cancelDeal] = useMutation(DealCancelMutation, {
        variables: {id: props.deal.ID}
    })

    const {loading, error, data} = useSubscription(DealSubscription, {
        variables: {id: props.deal.ID},
    })

    if (error) {
        return <div>Error: {error.message}</div>
    }

    var deal = props.deal
    if (!loading) {
        deal = data.dealUpdate
    }

    var logRowData = []
    for (var i = 0; i < (deal.Logs || []).length; i++) {
        var log = deal.Logs[i]
        var prev = i === 0 ? null : deal.Logs[i - 1]
        logRowData.push({log: log, prev: prev})
    }

    return <div className="deal-detail modal" id={deal.ID}>
        <div className="content">
            <div className="close" onClick={props.onCloseClick}>
                <div>X</div>
            </div>
            <div className="title">Deal {deal.ID}</div>
            <table className="deal-fields">
                <tbody>
                <tr>
                    <td>CreatedAt</td>
                    <td>{moment(deal.CreatedAt).format(dateFormat)}</td>
                </tr>
                <tr>
                    <td>Client Address</td>
                    <td>{deal.ClientAddress}</td>
                </tr>
                <tr>
                    <td>Client Peer ID</td>
                    <td>{deal.ClientPeerID}</td>
                </tr>
                <tr>
                    <td>Deal Data Root CID</td>
                    <td>{deal.DealDataRoot}</td>
                </tr>
                <tr>
                    <td>Piece CID</td>
                    <td>{deal.PieceCid}</td>
                </tr>
                <tr>
                    <td>Piece Size</td>
                    <td>{addCommas(deal.PieceSize)}</td>
                </tr>
                <tr>
                    <td>Provider Collateral</td>
                    <td>{humanFIL(deal.ProviderCollateral)}</td>
                </tr>
                <tr>
                    <td>Start Epoch</td>
                    <td>{deal.StartEpoch}</td>
                </tr>
                <tr>
                    <td>End Epoch</td>
                    <td>{deal.EndEpoch}</td>
                </tr>
                <tr>
                    <td>Transfer Type</td>
                    <td>{deal.Transfer.Type}</td>
                </tr>
                <tr>
                    <td>Transfer Size</td>
                    <td>{addCommas(deal.Transfer.Size)}</td>
                </tr>
                <tr>
                    <td>Publish Message CID</td>
                    <td>{deal.PublishCid}</td>
                </tr>
                <tr>
                    <td>Status</td>
                    <td>{deal.Message}</td>
                </tr>
                </tbody>
            </table>

            {deal.Stage === 'Accepted' ? (
                <div className="buttons">
                    <div className="button cancel" onClick={cancelDeal}>Cancel Transfer</div>
                </div>
            ) : null}

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

    return <tr>
        <td>{moment(log.CreatedAt).format(dateFormat)}</td>
        <td className="since-last">{sinceLast}</td>
        <td>{log.Text}</td>
    </tr>
}
