/* global BigInt */
import {useMutation, useQuery} from "@apollo/react-hooks";
import {DealPublishNowMutation, DealPublishQuery, SectorsListQuery, SectorsListRefreshMutation} from "./gql";
import React, {useState} from "react";
import {PageContainer} from "./Components";
import {useNavigate, useParams} from "react-router-dom";
import './SectorsList.css'
import './Loading.css'
import {Pagination} from "./Pagination";
import {addCommas, humanFileSize} from "./util";
import moment from "moment/moment";

const basePath = '/sectors-list'

export function SectorsListPage(props) {
    return <PageContainer pageType="sectors-list" title="Sectors List">
        <SectorsListContent />
    </PageContainer>
}

function SectorsListContent(props) {
    const navigate = useNavigate()
    const params = useParams()
    const pageNum = (params.pageNum && parseInt(params.pageNum)) || 1

    var [rowsPerPage, setRowsPerPage] = useState(RowsPerPage.load)
    const onRowsPerPageChange = (e) => {
        const val = parseInt(e.target.value)
        RowsPerPage.save(val)
        setRowsPerPage(val)
        navigate(basePath)
        scrollTop()
    }

    // Fetch logs on this page
    const listOffset = (pageNum-1) * rowsPerPage
    var queryCursor = null
    if (pageNum > 1 && params.cursor !== '') {
        try {
            queryCursor = parseInt(params.cursor)
        } catch {}
    }
    const {loading, error, data, refetch} = useQuery(SectorsListQuery, {
        pollInterval: 10000,
        variables: {
            cursor: queryCursor,
            offset: listOffset,
            limit: rowsPerPage,
        },
        fetchPolicy: 'network-only',
    })
    const [refreshList, mutationRes] = useMutation(SectorsListRefreshMutation, {
        onCompleted: function () {
            refetch()
        }
    })

    if (error) return <div>Error: {error.message + " - check connection to Boost server"}</div>
    if (loading) return <div>Loading...</div>

    var res = data.sectorsList
    var rows = res.sectors
    if (pageNum === 1) {
        rows = rows.slice(0, rowsPerPage)
    }
    const totalCount = res.totalCount

    var cursor = params.cursor
    if (pageNum === 1 && rows.length) {
        cursor = rows[0].SectorNumber
    }

    const paginationParams = {
        basePath, cursor, pageNum, totalCount, rowsPerPage,
        moreRows: res.more,
        onRowsPerPageChange: onRowsPerPageChange,
        onLinkClick: scrollTop,
    }

    return <div className="sectors-list">
        <div className="snapshot-at">
            <div className="refresh-button">
                <span className="button" onClick={refreshList}>Refresh</span><br />
                {mutationRes.loading ? (
                    <div className="loading"><div className="dot-flashing"></div></div>
                ): null}
            </div>
            <div className="description">Updated: {moment(res.at).format('HH:mm:ss')}</div>
        </div>
        <table>
            <tbody>
            <tr>
                <th>Number</th>
                <th>Unsealed Copy</th>
                <th>Deals</th>
                <th>Status</th>
                <th>Active</th>
                <th>Deal Weight</th>
                <th>Verified Power</th>
                <th>Expiration</th>
                <th>On Chain</th>
            </tr>

            {rows.map(row => <Row key={row.SectorNumber} row={row} />)}

            </tbody>
        </table>

        <Pagination {...paginationParams} />
    </div>
}

function Row({ row }) {
    return (
        <tr>
            <td className="number">
                {row.SectorNumber}
            </td>
            <td className="unsealed">
                {row.Unsealed ? 'Yes' : 'No'}
            </td>
            <td className="deals">
                {row.DealCount}
            </td>
            <td className="status">
                {row.Status}
            </td>
            <td className="active">
                {row.Active ? 'Yes' : 'No'}
            </td>
            <td className="deal-weight">
                {humanFileSize(row.DealWeight)}
            </td>
            <td className="verified-power">
                {humanFileSize(row.VerifiedPower)}
            </td>
            <td className="expiration">
                {addCommas(row.Expiration)}
            </td>
            <td className="on-chain">
                {row.OnChain ? 'Yes' : 'No'}
            </td>
        </tr>
    )
}

function scrollTop() {
    window.scrollTo({ top: 0, behavior: "smooth" })
}

const RowsPerPage = {
    Default: 10,

    settingsKey: "settings.sectors-list.per-page",

    load: () => {
        const saved = localStorage.getItem(RowsPerPage.settingsKey)
        return JSON.parse(saved) || RowsPerPage.Default
    },

    save: (val) => {
        localStorage.setItem(RowsPerPage.settingsKey, JSON.stringify(val));
    }
}

