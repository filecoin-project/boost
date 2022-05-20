import {useQuery} from "@apollo/react-hooks";
import {LegacyDealsCountQuery, LegacyDealsListQuery} from "./gql";
import moment from "moment";
import {humanFileSize} from "./util";
import React, {useState} from "react";
import {PageContainer, ShortClientAddress} from "./Components";
import {NavLink, useNavigate, useParams} from "react-router-dom";
import './Deals.css'
import './LegacyDeals.css'
import {dateFormat} from "./util-date";
import {TimestampFormat} from "./timestamp";
import {DealsPerPage} from "./deals-per-page";
import {Pagination} from "./Pagination";
import {StorageDealsIcon} from "./Deals";

export function LegacyStorageDealsPage(props) {
    return <PageContainer icon={<StorageDealsIcon />} title="Legacy Storage Deals">
        <LegacyStorageDealsContent />
    </PageContainer>
}

function LegacyStorageDealsContent(props) {
    const navigate = useNavigate()
    const params = useParams()
    const pageNum = params.pageNum ? parseInt(params.pageNum) : 1
    const [timestampFormat, setTimestampFormat] = useState(TimestampFormat.load)
    const saveTimestampFormat = (val) => {
        TimestampFormat.save(val)
        setTimestampFormat(val)
    }
    var toggleTimestampFormat = () => saveTimestampFormat(!timestampFormat)

    var [dealsPerPage, setDealsPerPage] = useState(DealsPerPage.load)
    const onDealsPerPageChange = (e) => {
        const val = parseInt(e.target.value)
        DealsPerPage.save(val)
        setDealsPerPage(val)
        navigate('/legacy-storage-deals')
        scrollTop()
    }

    const dealListOffset = (pageNum-1) * dealsPerPage
    const queryCursor = pageNum === 1 ? null : params.cursor
    const {loading, error, data} = useQuery(LegacyDealsListQuery, {
        pollInterval: pageNum === 1 ? 5000 : undefined,
        variables: {
            cursor: queryCursor,
            limit: dealsPerPage,
            offset: dealListOffset,
        }
    })
    if (error) return <div>Error: {error.message + " - check connection to Boost server"}</div>;
    if (loading) return <div>Loading...</div>;

    const deals = data.legacyDeals.deals
    const totalCount = data.legacyDeals.totalCount

    var cursor = params.cursor
    if (pageNum === 1 && deals.length) {
        cursor = deals[0].ID
    }

    const paginationParams = {
        basePath: '/legacy-storage-deals',
        cursor, pageNum, totalCount,
        moreRows: data.legacyDeals.more,
        rowsPerPage: dealsPerPage,
        onRowsPerPageChange: onDealsPerPageChange,
        onLinkClick: scrollTop,
    }

    return <div className="section">
        <table className="table table-striped">
            <thead>
                <tr>
                    <th onClick={toggleTimestampFormat} className="start">Start</th>
                    <th>Deal ID</th>
                    <th>Piece Size</th>
                    <th>Client</th>
                    <th>State</th>
                </tr>
            </thead>
            <tbody>
                {deals.map(deal => (
                    <DealRow
                        key={deal.ID}
                        deal={deal}
                        timestampFormat={timestampFormat}
                        toggleTimestampFormat={toggleTimestampFormat}
                    />
                ))}
            </tbody>
        </table>
        <Pagination {...paginationParams} />
    </div>
}

function DealRow(props) {
    var deal = props.deal

    var start = moment(deal.CreatedAt).format(dateFormat)
    if (props.timestampFormat !== TimestampFormat.DateTime) {
        start = '1m'
        if (new Date().getTime() - deal.CreatedAt.getTime() > 60 * 1000) {
            start = moment(deal.CreatedAt).fromNow()
        }
    }

    var message = deal.Status
    if (deal.Message) {
        message += ": " + deal.Message
    }

    return (
        <tr>
            <td className="start" onClick={props.toggleTimestampFormat}>{start}</td>
            <td className="deal-id">
                <NavLink to={"/legacy-deals/" + deal.ID}>
                    <div className="short-deal-id">{deal.ID.substring(0, 12) + '…'}</div>
                </NavLink>
            </td>
            <td className="piece-size">{humanFileSize(deal.PieceSize)}</td>
            <td className="client">
                <ShortClientAddress address={deal.ClientAddress} />
            </td>
            <td className="message">{message}</td>
        </tr>
    )
}

export function LegacyStorageDealsCount(props) {
    const {data} = useQuery(LegacyDealsCountQuery, {
        pollInterval: 5000,
        fetchPolicy: 'network-only',
    })

    if (!data) {
        return null
    }

    return (
        // <NavLink key="legacy-storage-deals" to="/legacy-storage-deals">
        //     <span className="figure">{data.legacyDealsCount}</span>
        //     <span className="label">Legacy Deal{data.legacyDealsCount === 1 ? '' : 's'}</span>
        // </NavLink>
        <div>
            <span className="figure">{data.legacyDealsCount}</span>
            <span className="label">Legacy Deal{data.legacyDealsCount === 1 ? '' : 's'}</span>
        </div>
    )
}

function scrollTop() {
    window.scrollTo({ top: 0, behavior: "smooth" })
}
