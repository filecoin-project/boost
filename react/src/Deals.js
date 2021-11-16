import {parseDates, useSubscription} from "./hooks";
import {DealsListQuery, DealSubscription, gqlQuery, gqlSubscribe, NewDealsSubscription} from "./gql";
import moment from "moment";
import {humanFileSize} from "./util";
import React, {useEffect, useState} from "react";
import {DealDetail} from "./DealDetail";

var dealsPerPage = 10

class NewDealsSubscriber {
    constructor(initialDeals, onNewDeal) {
        this.pageNum = 1
        this.deals = initialDeals
        this.onNewDeal = onNewDeal
    }

    updateFields(deals, pageNum) {
        newDealsSubscriber.deals = deals
        newDealsSubscriber.pageNum = pageNum
    }

    async subscribe() {
        var that = this

        try {
            var res = await gqlSubscribe({
                query: NewDealsSubscription
            })
            res.subscribe({
                next(r) {
                    parseDates(r)

                    // Don't add new deals to the list if we're not on the first page
                    if (this.pageNum > 1) {
                        return
                    }

                    var nextCursor
                    var dealNew = r.data.dealNew
                    that.deals = uniqDeals([dealNew, ...that.deals])
                    if (that.deals.length > dealsPerPage) {
                        nextCursor = that.deals[dealsPerPage].ID
                        that.deals = that.deals.slice(0, dealsPerPage)
                        that.onNewDeal(that.deals, nextCursor)
                    }
                },
                error(e) {
                    console.log('new deals subscription error:', e)
                }
            })
        } catch (e) {
            console.log('new deals subscription error:', e)
        }
    }
}

var newDealsSubscriber
var dealsPagination

export function StorageDealsPage(props) {
    const [dealToShow, setDealToShow] = useState(null)
    const [deals, setDeals] = useState([])
    const [totalCount, setTotalCount] = useState(0)
    const [pageNum, setPageNum] = useState(1)
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState()

    async function dealsListQuery(cursor) {
        try {
            var res = await gqlQuery({
                query: DealsListQuery,
                variables: {
                    limit: dealsPerPage,
                    first: cursor,
                },
                fetchPolicy: 'network-only',
            })

            var dealList = res.data.deals
            setDeals(dealList.deals)
            setTotalCount(dealList.totalCount)
            setLoading(false)

            return dealList
        } catch (e) {
            setError(e)
        }
    }

    // Runs the first time the page is rendered
    useEffect(() => {
        // Create a pagination manager
        dealsPagination = new DealsPagination(setPageNum, dealsListQuery)

        async function onStart() {
            // Make a query to get the current list of deals
            var res = await dealsListQuery()
            if (res.deals) {
                // Subscribe to "new deal" events
                newDealsSubscriber = new NewDealsSubscriber(res.deals, function (newDeals, nextCursor) {
                    if (newDealsSubscriber.pageNum === 1) {
                        // If it's the first page, update the list of deals
                        // that is currently being displayed
                        setDeals(newDeals)
                        dealsPagination.addPageCursor(2, nextCursor)
                    }
                    setTotalCount(res.totalCount + 1)
                })

                dealsPagination.addPageCursor(2, res.next)
            }
            newDealsSubscriber.subscribe()
        }

        onStart()
    }, [])

    // When the deals being displayed or the page number changes, update the
    // deals subscriber fields
    useEffect(() => {
        newDealsSubscriber && newDealsSubscriber.updateFields(deals, pageNum)
    }, [deals, pageNum])

    if (loading) return <div>Loading...</div>;
    if (error) return <div>Error: {error.message}</div>;

    var dealDetail
    if (dealToShow) {
        dealDetail = deals.find(dl => dl.ID === dealToShow)
    }

    var totalPages = Math.ceil(totalCount / dealsPerPage)

    return <div className="deals">
        <table>
            <tbody>
            <tr>
                <th>Start</th>
                <th>Deal ID</th>
                <th>Size</th>
                <th>Client</th>
                <th>State</th>
            </tr>

            {deals.map(deal => (
                <DealRow key={deal.ID} deal={deal} onDealRowClick={setDealToShow}/>
            ))}
            </tbody>
        </table>

        <div className="pagination">
            <div className="controls">
                <div className="left" onClick={() => dealsPagination.prevPage(pageNum)}>&lt;</div>
                <div className="page">{pageNum} of {totalPages}</div>
                <div className="right" onClick={() => dealsPagination.nextPage(pageNum)}>&gt;</div>
                <div className="total">{totalCount} deals</div>
            </div>
        </div>

        {dealDetail && (
            <div id="deal-detail">
                <DealDetail key={dealDetail.ID} deal={dealDetail} onCloseClick={() => setDealToShow("")}/>
            </div>
        )}
    </div>
}

function DealRow(props) {
    const {loading, error, data} = useSubscription(DealSubscription, {
        variables: {id: props.deal.ID},
    })

    if (error) {
        return <tr>
            <td colSpan={5}>Error: {error.message}</td>
        </tr>
    }

    var deal = props.deal
    if (!loading) {
        deal = data.dealUpdate
    }

    return (
        <tr>
            <td>{moment(deal.CreatedAt).fromNow()}</td>
            <td className="deal-id" onClick={() => props.onDealRowClick(deal.ID)}>
                {deal.ID}
            </td>
            <td>{humanFileSize(deal.PieceSize, false, 0)}</td>
            <td>{deal.ClientAddress}</td>
            <td>{deal.Message}</td>
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

function uniqDeals(deals) {
    return new Array(...new Map(deals.map(el => [el.ID, el])).values())
}
