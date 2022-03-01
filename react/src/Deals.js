import {useQuery, useSubscription} from "@apollo/react-hooks";
import {DealsCountQuery, DealsListQuery, DealSubscription, gqlClient, NewDealsSubscription} from "./gql";
import moment from "moment";
import {humanFileSize} from "./util";
import React, {useEffect, useState} from "react";
import {PageContainer, ShortClientAddress, ShortDealLink} from "./Components";
import {Link} from "react-router-dom";
import columnsGapImg from './bootstrap-icons/icons/columns-gap.svg'
import './Deals.css'
import {dateFormat} from "./util-date";
import {LegacyStorageDealsCount} from "./LegacyDeals";
import {TimestampFormat} from "./timestamp";

var dealsPerPage = 10

class NewDealsSubscriber {
    constructor(initialDeals, dealCount, onNewDeal) {
        this.pageNum = 1
        this.deals = initialDeals
        this.onNewDeal = onNewDeal
        this.dealCount = dealCount
    }

    updateFields(deals, pageNum) {
        newDealsSubscriber.deals = deals
        newDealsSubscriber.pageNum = pageNum
    }

    async subscribe() {
        var that = this

        try {
            var res = await gqlClient.subscribe({
                query: NewDealsSubscription
            })

            return res.subscribe({
                next(r) {
                    // Don't add new deals to the list if we're not on the first page
                    if (that.pageNum > 1) {
                        return
                    }

                    var nextCursor
                    var dealNew = r.data.dealNew
                    var prevLength = that.deals.length
                    that.deals = uniqDeals([dealNew, ...that.deals])
                    const isNewDeal = that.deals.length > prevLength
                    if (that.deals.length > dealsPerPage) {
                        nextCursor = that.deals[dealsPerPage].ID
                        that.deals = that.deals.slice(0, dealsPerPage)
                    }

                    // If a new deal was added, call the onNewDeal callback
                    if (isNewDeal) {
                        that.dealCount++
                        that.onNewDeal(that.deals, nextCursor, that.dealCount)
                    }
                },
                error(e) {
                    console.error('new deals subscription error:', e)
                }
            })
        } catch (e) {
            console.error('new deals subscription error:', e)
        }
    }
}

var newDealsSubscriber
var dealsPagination

export function StorageDealsPage(props) {
    return <PageContainer pageType="storage-deals" title="Storage Deals">
        <StorageDealsContent />
    </PageContainer>
}

function StorageDealsContent(props) {
    const [deals, setDeals] = useState([])
    const [totalCount, setTotalCount] = useState(0)
    const [pageNum, setPageNum] = useState(1)
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState()

    const [timestampFormat, setTimestampFormat] = useState(TimestampFormat.load)
    const saveTimestampFormat = (val) => {
        TimestampFormat.save(val)
        setTimestampFormat(val)
    }

    async function dealsListQuery(cursor) {
        try {
            var res = await gqlClient.query({
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
            console.error(e)
            e.message += " - check connection to Boost server"
            setError(e)
        }
    }

    // Runs the first time the page is rendered
    useEffect(() => {
        // Create a pagination manager
        dealsPagination = new DealsPagination(setPageNum, dealsListQuery)

        var sub
        async function onStart() {
            // Make a query to get the current list of deals
            var res = await dealsListQuery()
            if (!(res || {}).deals) {
                return
            }

            // Subscribe to "new deal" events
            function onNewDeal(newDeals, nextCursor, count) {
                if (newDealsSubscriber.pageNum === 1) {
                    // If it's the first page, update the list of deals
                    // that is currently being displayed
                    setDeals(newDeals)
                    dealsPagination.addPageCursor(2, nextCursor)
                }
                setTotalCount(count)
            }
            newDealsSubscriber = new NewDealsSubscriber(res.deals, res.totalCount, onNewDeal)
            sub = newDealsSubscriber.subscribe()

            dealsPagination.addPageCursor(2, res.next)
        }

        onStart()

        return async function () {
            if (sub) {
                (await sub).unsubscribe()
            }
        }
    }, [])

    // When the deals being displayed or the page number changes, update the
    // deals subscriber fields
    useEffect(() => {
        newDealsSubscriber && newDealsSubscriber.updateFields(deals, pageNum)
    }, [deals, pageNum])

    if (error) return <div>Error: {error.message}</div>;
    if (loading) return <div>Loading...</div>;

    var totalPages = Math.ceil(totalCount / dealsPerPage)

    var toggleTimestampFormat = () => saveTimestampFormat(!timestampFormat)

    return <div className="deals">
        <table>
            <tbody>
            <tr>
                <th onClick={toggleTimestampFormat} className="start">Start</th>
                <th>Deal ID</th>
                <th>Size</th>
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

        <div className="pagination">
            <div className="controls">
                <div className="left" onClick={() => dealsPagination.prevPage(pageNum)}>&lt;</div>
                <div className="page">{pageNum} of {totalPages}</div>
                <div className="right" onClick={() => dealsPagination.nextPage(pageNum)}>&gt;</div>
                <div className="total">{totalCount} deals</div>
            </div>
        </div>
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

    var start = moment(deal.CreatedAt).format(dateFormat)
    if (props.timestampFormat !== TimestampFormat.DateTime) {
        start = '1m'
        if (new Date().getTime() - deal.CreatedAt.getTime() > 60 * 1000) {
            start = moment(deal.CreatedAt).fromNow()
        }
    }

    return (
        <tr>
            <td className="start" onClick={props.toggleTimestampFormat}>
                {start}
            </td>
            <td className="deal-id">
                <ShortDealLink id={deal.ID} />
            </td>
            <td className="size">{humanFileSize(deal.Transfer.Size)}</td>
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

function uniqDeals(deals) {
    return new Array(...new Map(deals.map(el => [el.ID, el])).values())
}

export function StorageDealsMenuItem(props) {
    const {data} = useQuery(DealsCountQuery, {
        pollInterval: 5000,
        fetchPolicy: 'network-only',
    })

    return (
        <div className="menu-item" >
            <img className="icon" alt="" src={columnsGapImg} />
            <Link key="storage-deals" to="/storage-deals">
                    <h3>Storage Deals</h3>
            </Link>
            {data ? (
                <Link key="legacy-storage-deals" to="/storage-deals">
                    <div className="menu-desc">
                        <b>{data.dealsCount}</b> deal{data.dealsCount === 1 ? '' : 's'}
                    </div>
                </Link>
            ) : null}

            <LegacyStorageDealsCount />
        </div>
    )
}
