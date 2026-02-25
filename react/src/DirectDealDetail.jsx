import React, {useEffect} from "react";
import {useQuery,} from "@apollo/client";
import {DirectDealQuery, EpochQuery} from "./gql";
import {useNavigate, useParams, Link} from "react-router-dom";
import {dateFormat} from "./util-date";
import moment from "moment";
import {addCommas, humanFileSize, isContractAddress} from "./util";
import './DealDetail.css'
import './Components.css'
import closeImg from './bootstrap-icons/icons/x-circle.svg'
import {addClassFor} from "./util-ui";
import {DealActions, DealLog, DealStatusInfo, getAllDataAsText} from "./DealDetail";

export function DirectDealDetail(props) {
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
        navigator.clipboard.writeText(deal.ID)
        const el = document.body.querySelector('.content .title .copy')
        addClassFor(el, 'copied', 500)
        showPopup("Copied " + deal.ID + " to clipboard")
    }

    function allToClipboard() {
        const detailTableEl = document.body.querySelector('.deal-detail .deal-fields')
        const allDataAsText = getAllDataAsText(detailTableEl, deal.ID, logs)
        navigator.clipboard.writeText(allDataAsText)
        const el = document.body.querySelector('.content .title .copy-all')
        addClassFor(el, 'copied', 500)
        showPopup("Copied all data to clipboard")
    }

    const currentEpochData = useQuery(EpochQuery)

    const {loading, error, data} = useQuery(DirectDealQuery, {
        pollInterval: 10000,
        variables: {id: params.dealID},
    })

    if (error) {
        return <div>Error: {error.message}</div>
    }

    if (loading) {
        return <div>Loading ...</div>
    }

    var deal = data.directDeal

    const currentEpoch = (((currentEpochData || {}).data || {}).epoch || {}).Epoch
    var startEpochTime, endEpochTime
    if (currentEpoch) {
        const secondsPerEpoch = currentEpochData.data.epoch.SecondsPerEpoch
        const startEpochDelta = Number(deal.StartEpoch - currentEpoch)
        startEpochTime = new Date(new Date().getTime() + startEpochDelta*secondsPerEpoch*1000)
        const endEpochDelta = Number(deal.EndEpoch - currentEpoch)
        endEpochTime = new Date(new Date().getTime() + endEpochDelta*secondsPerEpoch*1000)
    }

    var logRowData = []
    var logs = [...(deal.Logs || [])].sort((a, b) => a.CreatedAt.getTime() - b.CreatedAt.getTime())
    for (var i = 0; i < logs.length; i++) {
        var log = logs[i]
        var prev = i === 0 ? null : logs[i - 1]
        logRowData.push({log: log, prev: prev})
    }

    return <div className="deal-detail modal" id={deal.ID}>
        <div className="content">
            <div className="close" onClick={() => navigate(-1)}>
                <img className="icon" alt="" src={closeImg} />
            </div>
            <div className="popup">
                <div className="message"></div>
            </div>
            <div className="title">
                <span>Deal {deal.ID}</span>
                <span className="copy" onClick={dealIDToClipboard} title="Copy deal uuid to clipboard"></span>
                <span className="copy-all" onClick={allToClipboard} title="Copy all deal info to clipboard"></span>
            </div>
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
                    <th>Allocation ID</th>
                    <td>{deal.AllocationID ? addCommas(deal.AllocationID) : null}</td>
                </tr>
                <tr>
                    <th>Client Address</th>
                    <td>
                        <a href={"https://filfox.info/en/address/"+deal.ClientAddress} target="_blank" rel="noreferrer"
                           className={isContractAddress(deal.ClientAddress) ? 'contract' : ''}
                        >
                            {deal.ClientAddress}
                            {isContractAddress(deal.ClientAddress) ? <span className="aux"> (Contract)</span> : ''}
                        </a>
                    </td>
                </tr>
                <tr>
                    <th>Verified</th>
                    <td>{'Yes'}</td>
                </tr>
                <tr>
                    <th>Keep Unsealed Copy</th>
                    <td>{deal.KeepUnsealedCopy ? 'Yes' : 'No'}</td>
                </tr>
                <tr>
                    <th>Announce To IPNI</th>
                    <td>{deal.AnnounceToIPNI ? 'Yes' : 'No'}</td>
                </tr>
                <tr>
                    <th>Piece CID</th>
                    <td><Link to={'/piece-doctor/'+deal.PieceCid}>{deal.PieceCid}</Link></td>
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
                    <th>Current Epoch</th>
                    <td>{currentEpoch ? addCommas(currentEpoch) : null}</td>
                </tr>
                <tr>
                    <th>Start Epoch</th>
                    <td>
                        {addCommas(deal.StartEpoch)}
                        <span className="aux">
                            {startEpochTime ? ' (' + moment(startEpochTime).fromNow() + ')' : null}
                        </span>
                    </td>
                </tr>
                <tr>
                    <th>End Epoch</th>
                    <td>
                        {addCommas(deal.EndEpoch)}
                        <span className="aux">
                            {endEpochTime ? ' (' + moment(endEpochTime).fromNow() + ')' : null}
                        </span>
                    </td>
                </tr>
                <tr>
                    <th>Duration</th>
                    <td>
                        {addCommas(deal.EndEpoch-deal.StartEpoch)}
                        <span className="aux">
                            {startEpochTime && endEpochTime ? ' (' + moment(endEpochTime).diff(startEpochTime, 'days') + ' days)' : null}
                        </span>
                    </td>
                </tr>
                <tr>
                    <th>Inbound File Path</th>
                    <td>{deal.InboundFilePath}</td>
                </tr>
                <tr>
                    <th>Delete After Add Piece</th>
                    <td>{deal.CleanupData ? 'Yes' : 'No'}</td>
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
                    <th>Checkpoint</th>
                    <td>
                        {deal.Checkpoint}
                        {deal.CheckpointAt+'' !== (new Date(0))+'' ? (
                            <span>
                            &nbsp;
                                <span className="aux">({moment(deal.CheckpointAt).fromNow()} ago)</span>
                          </span>
                        ) : null}
                    </td>
                </tr>
                <tr>
                    <th>Status</th>
                    <td>
                        {deal.Message}
                        <DealStatusInfo />
                    </td>
                </tr>

                </tbody>
            </table>

            <DealActions deal={deal} />

            <h3>Deal Logs</h3>

            <table className="deal-logs">
                <tbody>
                {logRowData.map((l, i) => <DealLog key={i} log={l.log} prev={l.prev}/>)}
                </tbody>
            </table>
        </div>
    </div>
}
