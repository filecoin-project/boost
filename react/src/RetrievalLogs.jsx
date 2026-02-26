/* global BigInt */
import {useQuery} from "@apollo/client";
import {
    GraphsyncRetrievalMinerAddressesQuery,
    MinerAddressQuery,
    RetrievalLogsCountQuery, RetrievalLogsListQuery,
} from "./gql";
import moment from "moment";
import React, {useState} from "react";
import {PageContainer, ShortCID, ShortPeerID} from "./Components";
import {Link, useNavigate, useParams} from "react-router-dom";
import {dateFormat, durationNanoToString} from "./util-date";
import {TimestampFormat} from "./timestamp";
import './RetrievalLogs.css'
import {Pagination} from "./Pagination";
import {humanFileSize} from "./util";
import {addClassFor} from "./util-ui";
import listCheckImg from "./bootstrap-icons/icons/list-check.svg";
import {getDealStatus} from "./RetrievalLogDetail";

const basePath = '/retrieval-logs'

export function RetrievalLogsPage(props) {
    return <PageContainer pageType="retrieval-logs" title="Graphsync Retrievals">
        <RetrievalLogsContent />
    </PageContainer>
}

function RetrievalLogsContent() {
    return <>
        <RetrievalMinerAddrs />
        <RetrievalLogsList />
    </>
}

function RetrievalMinerAddrs() {
    const minerAddrQuery =useQuery(MinerAddressQuery)
    const gsRetrievalMinerAddrsQuery =useQuery(GraphsyncRetrievalMinerAddressesQuery)

    if (!minerAddrQuery.data || !gsRetrievalMinerAddrsQuery.data) {
        return null
    }

    const minerAddr = minerAddrQuery.data.minerAddress.MinerID
    const retrievalMinerAddrs = gsRetrievalMinerAddrsQuery.data.graphsyncRetrievalMinerAddresses

    if (retrievalMinerAddrs.length === 1 && retrievalMinerAddrs[0] === minerAddr) {
        return null
    }

    return <div className="retrieval-miner-addrs">
        <h3>Retrieval Miners</h3>
        {retrievalMinerAddrs.map(a => <div key={a} className="miner-addr">{a}</div>)}
    </div>
}

function RetrievalLogsList() {
    const navigate = useNavigate()
    const params = useParams()
    const pageNum = (params.pageNum && parseInt(params.pageNum)) || 1

    const [timestampFormat, setTimestampFormat] = useState(TimestampFormat.load)
    const saveTimestampFormat = (val) => {
        TimestampFormat.save(val)
        setTimestampFormat(val)
    }

    var [rowsPerPage, setRowsPerPage] = useState(RowsPerPage.load)
    const onLogsPerPageChange = (e) => {
        const val = parseInt(e.target.value)
        RowsPerPage.save(val)
        setRowsPerPage(val)
        navigate(basePath)
        scrollTop()
    }

    // Fetch logs on this page
    const listOffset = (pageNum-1) * rowsPerPage
    var queryCursor = null
    if (pageNum > 1 && params.cursor) {
        try {
            queryCursor = BigInt(params.cursor)
        } catch {}
    }
    const {loading, error, data} = useQuery(RetrievalLogsListQuery, {
        pollInterval: 10000,
        variables: {
            cursor: queryCursor,
            offset: listOffset,
            limit: rowsPerPage,
            isIndexer: false,
        },
        fetchPolicy: 'network-only',
    })

    if (error) return <div>Error: {error.message + " - check connection to Boost server"}</div>
    if (loading) return <div>Loading...</div>

    var res = data.retrievalLogs
    var logs = res.logs
    if (pageNum === 1) {
        logs = [...logs].sort((a, b) => Number(b.RowID - a.RowID))
        logs = logs.slice(0, rowsPerPage)
    }
    const totalCount = res.totalCount

    var cursor = params.cursor
    if (pageNum === 1 && logs.length) {
        cursor = Number(logs[0].RowID)
    }

    var toggleTimestampFormat = () => saveTimestampFormat(!timestampFormat)

    const paginationParams = {
        basePath, cursor, pageNum, totalCount, rowsPerPage,
        moreRows: res.more,
        onRowsPerPageChange: onLogsPerPageChange,
        onLinkClick: scrollTop,
    }

    return <div className="retrieval-logs">
        <div className="popup">
            <div className="message"></div>
        </div>
        <table>
            <tbody>
            <tr>
                <th onClick={toggleTimestampFormat} className="start">Start</th>
                <th>Peer ID</th>
                <th>Transfer ID</th>
                <th>Payload CID</th>
                <th>Sent</th>
                <th>Status</th>
                <th>Message</th>
            </tr>

            {logs.map(row => (
                <TableRow
                    key={row.RowID}
                    row={row}
                    timestampFormat={timestampFormat}
                    toggleTimestampFormat={toggleTimestampFormat}
                />
            ))}
            </tbody>
        </table>

        <Pagination {...paginationParams} />
    </div>
}

var popupTimeout
function showPopup(msg) {
    clearTimeout(popupTimeout)
    const el = document.body.querySelector('.retrieval-logs .popup')
    popupTimeout = addClassFor(el, 'showing', 2000)
    const msgEl = document.body.querySelector('.retrieval-logs .popup .message')
    msgEl.textContent = msg
}

function TableRow(props) {
    var row = props.row
    var start = moment(row.CreatedAt).format(dateFormat)
    if (props.timestampFormat !== TimestampFormat.DateTime) {
        start = '1m'
        if (new Date().getTime() - row.CreatedAt.getTime() > 60 * 1000) {
            start = moment(row.CreatedAt).fromNow()
        }
    }

    function fieldToClipboard(fieldValue, elId) {
        navigator.clipboard.writeText(fieldValue)
        const el = document.getElementById(elId)
        addClassFor(el, 'copied', 500)
        showPopup("Copied " + fieldValue + " to clipboard")
    }

    const copyPeerId = "copy-"+row.CreatedAt+row.PeerID
    const peerIDToClipboard = () => fieldToClipboard(row.PeerID, copyPeerId)
    const copyTransferId = "copy-"+row.CreatedAt+row.TransferID
    const transferIDToClipboard = () => fieldToClipboard(row.TransferID, copyTransferId)

    var status = getDealStatus(row.DTStatus)
    if (row.DTStatus !== status && row.DTStatus !== '') {
        status += ": " + row.DTStatus
    }
    var msg = row.Message
    if (row.DTMessage !== '') {
        if (msg !== '') {
            msg += ' - '
        }
        msg += row.DTMessage
    }
    return (
        <tr>
            <td className="start" onClick={props.toggleTimestampFormat}>
                {start}
            </td>
            <td className="peer-id">
                <span id={copyPeerId} className="copy" onClick={peerIDToClipboard} title="Copy peer ID to clipboard"></span>
                <ShortPeerID peerId={row.PeerID} />
            </td>
            <td className="transfer-id">
                <span id={copyTransferId} className="copy" onClick={transferIDToClipboard} title="Copy transfer ID to clipboard"></span>
                <Link to={basePath+'/'+row.PeerID+'/'+row.TransferID}>
                    {'…'+(row.TransferID+'').slice(-8)}
                </Link>
            </td>
            <td className="payload-cid">
                <Link to={'/piece-doctor/'+row.PayloadCID}>
                    <ShortCID cid={row.PayloadCID} />
                </Link>
            </td>
            <td className="sent">
                {humanFileSize(row.TotalSent)}
            </td>
            <td className="status">
                {status}
            </td>
            <td className="message">
                {msg}
            </td>
        </tr>
    )
}

export function RetrievalLogsMenuItem(props) {
    const {data} = useQuery(RetrievalLogsCountQuery, {
        pollInterval: 10000,
        fetchPolicy: 'network-only',
        variables: {
            isIndexer: false,
        }
    })

    var durationDisplay = ''
    var count = 0
    if (data && data.retrievalLogsCount) {
        const plc = data.retrievalLogsCount
        count = plc.Count
        durationDisplay = durationNanoToString(plc.Period)
    }

    return (
        <div className="menu-item" >
            <img className="icon" alt="" src={listCheckImg} />
            <Link key="proposal-logs" to={basePath}>
                <h3>Retrievals {durationDisplay && '('+durationDisplay+')'}</h3>
                <div className="menu-desc">
                    <b>{count}</b> retrievals
                </div>
            </Link>
        </div>
    )
}

function scrollTop() {
    window.scrollTo({ top: 0, behavior: "smooth" })
}

const RowsPerPage = {
    Default: 10,

    settingsKey: "settings.retrieval-logs.per-page",

    load: () => {
        const saved = localStorage.getItem(RowsPerPage.settingsKey)
        return JSON.parse(saved) || RowsPerPage.Default
    },

    save: (val) => {
        localStorage.setItem(RowsPerPage.settingsKey, JSON.stringify(val));
    }
}
