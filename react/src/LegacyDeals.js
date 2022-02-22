import {useQuery} from "@apollo/react-hooks";
import {LegacyDealsCountQuery, LegacyDealsListQuery} from "./gql";
import moment from "moment";
import {humanFileSize} from "./util";
import React, {useState} from "react";
import {PageContainer, ShortClientAddress, ShortDealID} from "./Components";
import {Link} from "react-router-dom";
import './Deals.css'
import './LegacyDeals.css'
import {dateFormat} from "./util-date";

var dealsPerPage = 10

export function LegacyStorageDealsPage(props) {
    return <PageContainer pageType="legacy-storage-deals" title="Legacy Storage Deals">
        <LegacyStorageDealsContent />
    </PageContainer>
}

function LegacyStorageDealsContent(props) {
    const [first, setFirst] = useState(null)
    const [previous, setPrevious] = useState([])
    const [pageNum, setPageNum] = useState(1)
    const {loading, error, data} = useQuery(LegacyDealsListQuery, {
        variables: {first}
    })

    if (error) return <div>Error: {error.message}</div>;
    if (loading) return <div>Loading...</div>;

    console.log(data)
    const deals = data.legacyDeals.deals
    const totalCount = data.legacyDeals.totalCount
    var totalPages = Math.ceil(totalCount / dealsPerPage)

    function pageForward() {
        if (!data.legacyDeals.next) {
            return
        }
        setPrevious(previous.concat([first]))
        setFirst(data.legacyDeals.next)
        setPageNum(pageNum+1)
    }

    function pageBack() {
        if (previous.length === 0) {
            return
        }
        setFirst(previous[previous.length-1])
        setPrevious(previous.slice(0, previous.length-1))
        setPageNum(pageNum-1)
    }

    return <div className="deals">
        <table>
            <tbody>
            <tr>
                <th>Start</th>
                <th>Deal ID</th>
                <th>Piece Size</th>
                <th>Client</th>
                <th>State</th>
            </tr>

            {deals.map(deal => (
                <DealRow key={deal.ID} deal={deal} />
            ))}
            </tbody>
        </table>

        <div className="pagination">
            <div className="controls">
                <div className="left" onClick={pageBack}>&lt;</div>
                <div className="page">{pageNum} of {totalPages}</div>
                <div className="right" onClick={pageForward}>&gt;</div>
                <div className="total">{totalCount} deals</div>
            </div>
        </div>
    </div>
}

function DealRow(props) {
    var deal = props.deal
    var start = moment(deal.CreatedAt).format(dateFormat)
    var since = '1m'
    if (new Date().getTime() - deal.CreatedAt.getTime() > 60 * 1000) {
        since = moment(deal.CreatedAt).fromNow()
    }

    return (
        <tr>
            <td className="start">
                {start} <span className="since">{since}</span>
            </td>
            <td className="deal-id">
                <Link to={"/legacy-deals/" + deal.ID}>
                    <ShortDealID id={deal.ID} />
                </Link>
            </td>
            <td className="piece-size">{humanFileSize(deal.PieceSize)}</td>
            <td className="client">
                <ShortClientAddress address={deal.ClientAddress} />
            </td>
            <td className="message">{deal.Message}</td>
        </tr>
    )
}

class DealsPagination {
    constructor(setPageNum, dealsListQuery) {
        this.pageCursors = {}
        this.setPageNum = setPageNum
        this.dealsListQuery = dealsListQuery
    }

    addPageCursor(num, cursor) {
        var newPageCursors = {}
        newPageCursors[num] = cursor
        this.pageCursors = Object.assign(this.pageCursors, newPageCursors)
    }

    async nextPage(currentPage) {
        var newPageNum = currentPage + 1
        var nextCursor = this.pageCursors[newPageNum]
        if (!nextCursor) {
            return
        }

        var dealList = await this.dealsListQuery(nextCursor)
        this.setPageNum(newPageNum)
        this.addPageCursor(newPageNum + 1, dealList.next)
    }

    async prevPage(currentPage) {
        if (currentPage <= 1) {
            return
        }

        var newPageNum = currentPage - 1
        var prevCursor = this.pageCursors[newPageNum]
        await this.dealsListQuery(prevCursor)
        this.setPageNum(newPageNum)
    }
}

export function LegacyStorageDealsCount(props) {
    const {data, error} = useQuery(LegacyDealsCountQuery, {
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
