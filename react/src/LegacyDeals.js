import {useQuery} from "@apollo/react-hooks";
import {LegacyDealsCountQuery, LegacyDealsListQuery} from "./gql";
import moment from "moment";
import {humanFileSize} from "./util";
import React, {useState} from "react";
import {PageContainer, ShortClientAddress} from "./Components";
import {Link, useNavigate, useParams} from "react-router-dom";
import './Deals.css'
import './LegacyDeals.css'
import {dateFormat} from "./util-date";
import {TimestampFormat} from "./timestamp";
import {DealsPerPage} from "./deals-per-page";
import {Pagination} from "./Deals";

export function LegacyStorageDealsPage(props) {
    return <PageContainer pageType="legacy-storage-deals" title="Legacy Storage Deals">
        <LegacyStorageDealsContent />
    </PageContainer>
}

function LegacyStorageDealsContent(props) {
    const navigate = useNavigate()
    const params = useParams()
    const pageNum = params.pageNum ? parseInt(params.pageNum) : 1
    const [first, setFirst] = useState(null)
    const [previous, setPrevious] = useState([])
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
    console.log('params', params)
    console.log({
            first: queryCursor,
            limit: dealsPerPage,
            offset: dealListOffset,
    })
    const {loading, error, data} = useQuery(LegacyDealsListQuery, {
        pollInterval: 5000,
        variables: {
            first: queryCursor,
            limit: dealsPerPage,
            offset: dealListOffset,
        }
    })
    if (error) return <div>Error: {error.message + " - check connection to Boost server"}</div>;
    if (loading) return <div>Loading...</div>;

    const deals = data.legacyDeals.deals
    const totalCount = data.legacyDeals.totalCount
    var totalPages = Math.ceil(totalCount / dealsPerPage)

    function pageForward() {
        if (!data.legacyDeals.next) {
            return
        }
        window.scrollTo({ top: 0, behavior: "smooth" })
        setPrevious(previous.concat([first]))
        setFirst(data.legacyDeals.next)
        // setPageNum(pageNum+1)
    }

    function pageBack() {
        if (previous.length === 0) {
            return
        }
        window.scrollTo({ top: 0, behavior: "smooth" })
        setFirst(previous[previous.length-1])
        setPrevious(previous.slice(0, previous.length-1))
        // setPageNum(pageNum-1)
    }

    function pageFirst() {
        if (previous.length === 0) {
            return
        }
        window.scrollTo({ top: 0, behavior: "smooth" })
        setFirst(null)
        setPrevious([])
        // setPageNum(1)
    }

    var cursor = params.cursor
    if (pageNum === 1 && deals.length) {
        cursor = deals[0].ID
    }

    const paginationParams = {
        basePath: '/legacy-storage-deals',
        moreDeals: data.legacyDeals.more,
        cursor, pageNum, totalCount, dealsPerPage, onDealsPerPageChange
    }

    return <div className="deals">
        <table>
            <tbody>
            <tr>
                <th className="start" onClick={toggleTimestampFormat}>Start</th>
                <th>Deal ID</th>
                <th>Piece Size</th>
                <th>Client</th>
                <th>State</th>
            </tr>

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
                <Link to={"/legacy-deals/" + deal.ID}>
                    <div className="short-deal-id">{deal.ID.substring(0, 12) + '…'}</div>
                </Link>
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
        <Link key="legacy-storage-deals" to="/legacy-storage-deals">
            <div className="menu-desc">
                <b>{data.legacyDealsCount}</b> legacy deal{data.legacyDealsCount === 1 ? '' : 's'}
            </div>
        </Link>
    )
}

function scrollTop() {
    window.scrollTo({ top: 0, behavior: "smooth" })
}
