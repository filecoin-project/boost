import {useQuery, useSubscription} from "@apollo/react-hooks";
import {
    DealsCountQuery,
    DealsListQuery,
    DealSubscription,
    NewDealsSubscription,
} from "./gql";
import moment from "moment";
import {humanFileSize} from "./util";
import React, {useState} from "react";
import {PageContainer, ShortClientAddress, ShortDealLink} from "./Components";
import {Link, useNavigate, useParams} from "react-router-dom";
import {dateFormat} from "./util-date";
import {LegacyStorageDealsCount} from "./LegacyDeals";
import {TimestampFormat} from "./timestamp";
import {DealsPerPage} from "./deals-per-page";
import columnsGapImg from './bootstrap-icons/icons/columns-gap.svg'
// import './Deals.css'
import {Pagination} from "./Pagination";
import {Warn} from "./Info";

export function StorageDealsPage(props) {
    // return <PageContainer pageType="storage-deals" title="Storage Deals">
    //     <StorageDealsContent />
    // </PageContainer>
    return <StorageDealsContent />
}

function StorageDealsContent(props) {
    const navigate = useNavigate()
    const params = useParams()
    const [subDeals, setSubDeals] = useState([])

    const [timestampFormat, setTimestampFormat] = useState(TimestampFormat.load)
    const saveTimestampFormat = (val) => {
        TimestampFormat.save(val)
        setTimestampFormat(val)
    }

    var [dealsPerPage, setDealsPerPage] = useState(DealsPerPage.load)
    const onDealsPerPageChange = (e) => {
        const val = parseInt(e.target.value)
        DealsPerPage.save(val)
        setDealsPerPage(val)
        navigate('/storage-deals')
        scrollTop()
    }

    // Fetch deals on this page
    const pageNum = (params.pageNum && parseInt(params.pageNum)) || 1
    const dealListOffset = (pageNum-1) * dealsPerPage
    const queryCursor = (pageNum === 1) ? null : params.cursor
    const {loading, error, data} = useQuery(DealsListQuery, {
        variables: {
            cursor: queryCursor,
            offset: dealListOffset,
            limit: dealsPerPage,
        },
        fetchPolicy: 'network-only',
    })

    // Watch for new deals
    const sub = useSubscription(NewDealsSubscription)
    const subNewDeal = ((sub || {}).data || {}).dealNew
    if (subNewDeal) {
        // Check if the new deal is already in the list of deals
        const deals = (((data || {}).deals || {}).deals || [])
        const inDeals = deals.find(el => el.ID === subNewDeal.deal.ID)
        const inSubDeals = subDeals.find(el => el.ID === subNewDeal.deal.ID)
        if (!inDeals && !inSubDeals) {
            // New deal is not in the list of deals so add it to the subDeals array
            setSubDeals([subNewDeal.deal, ...subDeals].slice(0, dealsPerPage))
        }
    }

    if (error) return <div>Error: {error.message + " - check connection to Boost server"}</div>
    if (loading) return <div>Loading...</div>

    var deals = data.deals.deals
    if (pageNum === 1) {
        deals = uniqDeals([...data.deals.deals, ...subDeals])
        deals.sort((a, b) => b.CreatedAt.getTime() - a.CreatedAt.getTime())
        deals = deals.slice(0, dealsPerPage)
    }
    const totalCount = subNewDeal ? subNewDeal.totalCount : data.deals.totalCount
    const moreDeals = data.deals.more

    var cursor = params.cursor
    if (pageNum === 1 && deals.length) {
        cursor = deals[0].ID
    }

    var toggleTimestampFormat = () => saveTimestampFormat(!timestampFormat)

    const paginationParams = {
        basePath: '/storage-deals',
        cursor, pageNum, totalCount,
        rowsPerPage: dealsPerPage,
        moreRows: moreDeals,
        onRowsPerPageChange: onDealsPerPageChange,
        onLinkClick: scrollTop,
    }

    return (
        <div className="section">
            <div className="table-wrapper">
                <table className="table table-striped">
                    <thead>
                        <tr>
                            <th onClick={toggleTimestampFormat} className="start">Start</th>
                            <th>Deal ID</th>
                            <th>Size</th>
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
        </div>
    )
        // <table>
        //     <tbody>
        //     <tr>
        //         <th onClick={toggleTimestampFormat} className="start">Start</th>
        //         <th>Deal ID</th>
        //         <th>Size</th>
        //         <th>Client</th>
        //         <th>State</th>
        //     </tr>
        //
        //     {deals.map(deal => (
        //         <DealRow
        //             key={deal.ID}
        //             deal={deal}
        //             timestampFormat={timestampFormat}
        //             toggleTimestampFormat={toggleTimestampFormat}
        //         />
        //     ))}
        //     </tbody>
        // </table>
}

function DealRow(props) {
    const {loading, error, data} = useSubscription(DealSubscription, {
        variables: {id: props.deal.ID},
    })

    if (error) {
        console.error('Error subscribing to deal ' + props.deal.ID, error)
    }

    var deal = props.deal
    if (!loading && !error) {
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
        <tr className={error ? "error" : ""}>
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
            <td className="message">
                {deal.Message}
                {error ? (
                    <Warn>{"Web UI Subscription Error: " + error.message}</Warn>
                ) : null}
            </td>
        </tr>
    )
}

function uniqDeals(deals) {
    return new Array(...new Map(deals.map(el => [el.ID, el])).values())
}

export function StorageDealsMenuItem(props) {
    var {data} = useQuery(DealsCountQuery, {
        pollInterval: 5000,
        fetchPolicy: 'network-only',
    })
    data = data || { dealsCount: '...' }

    return (
        <Link key="storage-deals" to="/storage-deals" className="sidebar-item sidebar-item-deals">
            <span className="sidebar-icon">
                <svg width="24" height="26" viewBox="0 0 24 26" fill="none" xmlns="http://www.w3.org/2000/svg">
                    <rect x="1" y="1" width="9" height="7" rx="2" stroke-width="2"/>
                    <rect x="23" y="25" width="9" height="7" rx="2" transform="rotate(-180 23 25)" stroke-width="2"/>
                    <rect x="1" y="12" width="9" height="13" rx="2" stroke-width="2"/>
                    <rect x="23" y="14" width="9" height="13" rx="2" transform="rotate(-180 23 14)" stroke-width="2"/>
                </svg>
            </span>
            <span className="sidebar-title">Storage Deals</span>
            <div className="sidebar-item-excerpt">
                <span className="figure">{data.dealsCount}</span>
                <span className="label">Deal{data.dealsCount === 1 ? '' : 's'}</span>
            </div>
        </Link>
    //
    //
    // <div className="menu-item" >
    //         <img className="icon" alt="" src={columnsGapImg} />
    //         <Link key="storage-deals" to="/storage-deals">
    //                 <h3>Storage Deals</h3>
    //         </Link>
    //         {data ? (
    //             <Link key="legacy-storage-deals" to="/storage-deals">
    //                 <div className="menu-desc">
    //                     <b>{data.dealsCount}</b> deal{data.dealsCount === 1 ? '' : 's'}
    //                 </div>
    //             </Link>
    //         ) : null}
    //
    //         <LegacyStorageDealsCount />
    //     </div>
    )
}

function scrollTop() {
    window.scrollTo({ top: 0, behavior: "smooth" })
}
