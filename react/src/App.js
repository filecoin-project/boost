import './App.css';
import moment from 'moment';
import React, { useState, useEffect } from 'react';
import { ApolloProvider } from "@apollo/react-hooks";
import {useSubscription, useMutation, parseDates, useQuery} from "./hooks";
import {
    gqlClient,
    gqlQuery,
    gqlSubscribe,
    DealsListQuery,
    DealSubscription,
    DealCancelMutation,
    NewDealsSubscription,
    StorageQuery,
    FundsQuery,
} from "./gql";

moment.locale('en', {
    relativeTime: {
        future: "in %s",
        past:   "%s",
        s  : '%ds',
        ss : '%ds',
        m:  "1m",
        mm: "%dm",
        h:  "1h",
        hh: "%dh",
        d:  "a day",
        dd: "%d days",
        w:  "a week",
        ww: "%d weeks",
        M:  "a month",
        MM: "%d months",
        y:  "a year",
        yy: "%d years"
    }
})

var dateFormat = 'YYYY-MM-DD HH:mm:ss.SSS'

function App(props) {
    const [pageToShow, setPageToShow] = useState('storage-deals');

    return (
        <div id="content">
            <table className="content-table">
                <tbody>
                <tr>
                    <Menu pages={pages} pageToShow={pageToShow} onMenuItemClick={setPageToShow} />
                    <Pages pages={pages} pageToShow={pageToShow} />
                </tr>
                </tbody>
            </table>
        </div>
    )
}

function AppRoot(props) {
    return (
        <ApolloProvider client={gqlClient}>
            <App />
        </ApolloProvider>
    );
}

function Menu(props) {
    return (
        <td className="menu">
            {props.pages.map(page => (
                <div key={page.pageType} className="menu-item" onClick={() => props.onMenuItemClick(page.pageType)}>
                    {page.title}
                </div>
            ))}
        </td>
    )
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

    return  <tr>
        <td>{moment(log.CreatedAt).format(dateFormat)}</td>
        <td className="since-last">{sinceLast}</td>
        <td>{log.Text}</td>
    </tr>
}

function DealDetail(props) {
    // Add a class to the document body when showing the deal detail page
    useEffect(() => {
        document.body.classList.add('modal-open')

        return function () {
            document.body.classList.remove('modal-open')
        }
    })

    const [cancelDeal] = useMutation(DealCancelMutation, {
        variables: { id: props.deal.ID }
    })

    const { loading, error, data } = useSubscription(DealSubscription, {
        variables: { id: props.deal.ID },
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
        var prev = i === 0 ? null : deal.Logs[i-1]
        logRowData.push({ log: log, prev: prev })
    }

    return <div className="deal-detail modal" id={deal.ID}>
        <div className="content">
            <div className="close" onClick={props.onCloseClick}>
                <div className="button">X</div>
            </div>
            <div className="title">Deal {deal.ID}</div>
            <table className="deal-fields">
                <tbody>
                <tr>
                    <td>CreatedAt</td>
                    <td>{moment(deal.CreatedAt).format(dateFormat)}</td>
                </tr>
                <tr>
                    <td>Client</td>
                    <td>{deal.ClientAddress}</td>
                </tr>
                <tr>
                    <td>Amount</td>
                    <td>{'0.2 FIL'}</td>
                </tr>
                <tr>
                    <td>Size</td>
                    <td>{deal.PieceSize}</td>
                </tr>
                <tr>
                    <td>State</td>
                    <td>{deal.Message}</td>
                </tr>
                </tbody>
            </table>

            <div className="buttons">
                <div className="button cancel" onClick={cancelDeal}>Cancel</div>
                <div className="button retry">Retry</div>
            </div>

            <table className="deal-logs">
                <tbody>
                {logRowData.map((l, i) => <DealLog key={i} log={l.log} prev={l.prev} />)}
                </tbody>
            </table>
        </div>
    </div>
}

function DealRow(props) {
    const { loading, error, data } = useSubscription(DealSubscription, {
        variables: { id: props.deal.ID },
    })

    if (error) {
        return <tr><td colSpan={5}>Error: {error.message}</td></tr>
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

function uniqDeals(deals) {
    return new Array(...new Map(deals.map(el => [el.ID, el])).values())
}

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
        var newPageNum = currentPage+1
        var nextCursor = this.pageCursors[newPageNum]
        if (!nextCursor) {
            return
        }

        var dealList = await this.dealsListQuery(nextCursor)
        this.setPageNum(newPageNum)
        this.addPageCursor(newPageNum+1, dealList.next)
    }

    async prevPage(currentPage) {
        if (currentPage <= 1) {
            return
        }

        var newPageNum = currentPage-1
        var prevCursor = this.pageCursors[newPageNum]
        await this.dealsListQuery(prevCursor)
        this.setPageNum(newPageNum)
    }
}

var newDealsSubscriber
var dealsPagination

function StorageDealsPage(props) {
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
                    setTotalCount(res.totalCount+1)
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
                    <DealRow key={deal.ID} deal={deal} onDealRowClick={setDealToShow} />
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

function BarChart(props) {
    var max = 0
    for (let field of props.fields) {
        field.Type = field.Name.replace(/[ )]/, '-')
        if (field.Capacity > max) {
            max = field.Capacity
        }
    }

    return <div className="bar-chart">
        <table>
            <tbody>
            <tr>
                { props.fields.map(field => <ChartBar key={field.Type} usage={field} max={max} />) }
            </tr>
            <tr>
                { props.fields.map(field => <td key={field.Name} className="label">{field.Name}</td>) }
            </tr>
            </tbody>
        </table>
    </div>
}

function ChartBar(props) {
    var barHeightRatio = props.usage.Capacity / props.max
    var barHeight = Math.round(barHeightRatio * 100)
    var fillerHeight = 100 - barHeight
    var usedPct = Math.floor(100 * props.usage.Used / (props.usage.Capacity || 1))

    return <td className={'field ' + props.usage.Type}>
        <div className="filler" style={{ height: fillerHeight+'%' }}></div>
        <div className="bar" style={{ height: barHeight+'%' }}>
            <div className="size">{humanFileSize(props.usage.Capacity)}</div>
            { props.usage.Used ? (
                <div style={{ height: '100%' }}>
                    <div className="unused" style={{ height: (100 - usedPct)+'%' }} />
                    <div className="used" style={{ height: usedPct+'%' }} />
                </div>
            ) : null}
        </div>
    </td>
}

function StorageSpacePage(props) {
    const {loading, error, data} = useQuery(StorageQuery)

    if (loading) {
        return <div>Loading...</div>
    }
    if (error) {
        return <div>Error: {error.message}</div>
    }

    return <BarChart fields={data.storage} />
}

function FundsPage(props) {
    const { loading, error, data } = useQuery(FundsQuery)

    if (loading) {
        return <div>Loading...</div>
    }
    if (error) {
        return <div>Error: {error.message}</div>
    }

    return <BarChart fields={data.funds} />
}

var pages = [{
        title: 'Storage Deals',
        pageType: 'storage-deals',
    }, {
        title: 'Storage Space',
        pageType: 'storage-space',
    }, {
        title: 'Funds',
        pageType: 'funds',
    }
]

class Pages extends React.Component {
    render() {
        return (
            <td>
                {this.props.pages.map(page => (
                    <div key={page.pageType} id={page.pageType} style={this.props.pageToShow === page.pageType ? {} : {display: 'none'}}>
                        <div className="page-title">{page.title}</div>
                        <div className="page-content">{this.renderPage(page)}</div>
                    </div>
                ))}
            </td>)
    }

    renderPage(page) {
        switch (page.pageType) {
            case 'storage-deals':
                return <StorageDealsPage key={page.pageType} />
            case 'storage-space':
                return <StorageSpacePage key={page.pageType} />
            case 'funds':
                return <FundsPage key={page.pageType} />
            default:
                throw new Error("unrecognized page type " + page.pageType)
        }
    }
}

// TODO: Check licensing
// Copied from
// https://stackoverflow.com/questions/10420352/converting-file-size-in-bytes-to-human-readable-string
function humanFileSize(bytes, si=false, dp=1) {
    const thresh = si ? 1000 : 1024;

    if (Math.abs(bytes) < thresh) {
        return bytes + ' B';
    }

    const units = si
        ? ['kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
        : ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];
    let u = -1;
    const r = 10**dp;

    do {
        bytes /= thresh;
        ++u;
    } while (Math.round(Math.abs(bytes) * r) / r >= thresh && u < units.length - 1);

    return bytes.toFixed(dp) + ' ' + units[u];
}

export default AppRoot;