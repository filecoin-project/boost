/* global BigInt */
import {useQuery} from "@apollo/client";
import {
    ProposalLogsCountQuery,
    ProposalLogsListQuery,
} from "./gql";
import moment from "moment";
import React, {useState} from "react";
import {PageContainer, ShortClientAddress, ShortDealLink} from "./Components";
import {Link, useNavigate, useParams} from "react-router-dom";
import {dateFormat, durationNanoToString} from "./util-date";
import {TimestampFormat} from "./timestamp";
import './ProposalLogs.css'
import {Pagination} from "./Pagination";
import {humanFileSize} from "./util";
import {addClassFor} from "./util-ui";
import listCheckImg from "./bootstrap-icons/icons/list-check.svg";

const basePath = '/proposal-logs'

export function ProposalLogsPage(props) {
    return <PageContainer pageType="proposal-logs" title="Deal Proposal Accept Logs">
        <ProposalLogsContent />
    </PageContainer>
}

function ProposalLogsContent(props) {
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
    const {loading, error, data} = useQuery(ProposalLogsListQuery, {
        pollInterval: 10000,
        variables: {
            cursor: queryCursor,
            offset: listOffset,
            limit: rowsPerPage,
        },
        fetchPolicy: 'network-only',
    })

    if (error) return <div>Error: {error.message + " - check connection to Boost server"}</div>
    if (loading) return <div>Loading...</div>

    var res = data.proposalLogs
    var logs = res.logs
    if (pageNum === 1) {
        logs = [...logs].sort((a, b) => b.CreatedAt.getTime() - a.CreatedAt.getTime())
        logs = logs.slice(0, rowsPerPage)
    }
    const totalCount = res.totalCount

    var cursor = params.cursor
    if (pageNum === 1 && logs.length) {
        cursor = logs[0].DealUUID
    }

    var toggleTimestampFormat = () => saveTimestampFormat(!timestampFormat)

    const paginationParams = {
        basePath, cursor, pageNum, totalCount, rowsPerPage,
        moreRows: res.more,
        onRowsPerPageChange: onLogsPerPageChange,
        onLinkClick: scrollTop,
    }

    return <div className="logs">
        <div className="popup">
            <div className="message"></div>
        </div>
        <table>
            <tbody>
            <tr>
                <th onClick={toggleTimestampFormat} className="start">Start</th>
                <th>Deal ID</th>
                <th>Piece Size</th>
                <th>Client</th>
                <th>Status</th>
            </tr>

            {logs.map(log => (
                <LogRow
                    key={log.DealUUID}
                    log={log}
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
    const el = document.body.querySelector('.logs .popup')
    popupTimeout = addClassFor(el, 'showing', 2000)
    const msgEl = document.body.querySelector('.logs .popup .message')
    msgEl.textContent = msg
}

function LogRow(props) {
    var log = props.log
    var start = moment(log.CreatedAt).format(dateFormat)
    if (props.timestampFormat !== TimestampFormat.DateTime) {
        start = '1m'
        if (new Date().getTime() - log.CreatedAt.getTime() > 60 * 1000) {
            start = moment(log.CreatedAt).fromNow()
        }
    }

    function fieldToClipboard(fieldValue, elId) {
        navigator.clipboard.writeText(fieldValue)
        const el = document.getElementById(elId)
        addClassFor(el, 'copied', 500)
        showPopup("Copied " + fieldValue + " to clipboard")
    }

    const copyId = "copy-"+log.DealUUID
    const dealIDToClipboard = () => fieldToClipboard(log.DealUUID, copyId)
    const clientCopyId = "client-addr-"+log.DealUUID
    const clientAddrToClipboard = () => fieldToClipboard(log.ClientAddress, clientCopyId)

    return (
        <tr className={log.Accepted ? 'accepted' : 'rejected'}>
            <td className="start" onClick={props.toggleTimestampFormat}>
                {start}
            </td>
            <td className="deal-id">
                <span id={copyId} className="copy" onClick={dealIDToClipboard} title="Copy deal uuid to clipboard"></span>
                <div className="deal-id">
                    {log.Accepted ? <ShortDealLink id={log.DealUUID} /> : log.DealUUID}
                </div>
            </td>
            <td className="size">
                {humanFileSize(log.PieceSize)}
            </td>
            <td className="client">
                <span id={clientCopyId} className="copy" onClick={clientAddrToClipboard} title="Copy client address to clipboard"></span>
                <ShortClientAddress address={log.ClientAddress} />
            </td>
            <td className="reason">
                {log.Accepted ? 'Accepted' : log.Reason || 'Rejected'}
            </td>
        </tr>
    )
}

export function ProposalLogsMenuItem(props) {
    const {data} = useQuery(ProposalLogsCountQuery, {
        pollInterval: 10000,
        fetchPolicy: 'network-only',
        variables: {
            accepted: true,
        }
    })

    var durationDisplay = ''
    var acceptCount = 0
    var rejectCount = 0
    if (data && data.proposalLogsCount) {
        const plc = data.proposalLogsCount
        acceptCount = plc.Accepted
        rejectCount = plc.Rejected
        durationDisplay = durationNanoToString(plc.Period)
    }

    return (
        <div className="menu-item" >
            <img className="icon" alt="" src={listCheckImg} />
            <Link key="proposal-logs" to={basePath}>
                <h3>Deal Proposals {durationDisplay && '('+durationDisplay+')'}</h3>
                <div className="menu-desc">
                    <b>{acceptCount}</b> accepted
                </div>
                <div className="menu-desc">
                    <b>{rejectCount}</b> rejected
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

    settingsKey: "settings.proposal-logs.per-page",

    load: () => {
        const saved = localStorage.getItem(RowsPerPage.settingsKey)
        return JSON.parse(saved) || RowsPerPage.Default
    },

    save: (val) => {
        localStorage.setItem(RowsPerPage.settingsKey, JSON.stringify(val));
    }
}
