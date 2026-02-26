import {useQuery} from "@apollo/client";
import {
    DealsListQuery, DirectDealsCountQuery, DirectDealsListQuery,
} from "./gql";
import moment from "moment";
import {isContractAddress} from "./util";
import React, {useState} from "react";
import {PageContainer, ShortClientAddress, ShortDealID} from "./Components";
import {Link, useNavigate, useParams} from "react-router-dom";
import {dateFormat} from "./util-date";
import {TimestampFormat} from "./timestamp";
import {DealsPerPage} from "./deals-per-page";
import columnsGapImg from './bootstrap-icons/icons/columns-gap.svg'
import './Deals.css'
import {Pagination} from "./Pagination";
import {DealActions, IsPaused, IsOfflineWaitingForData} from "./DealDetail";
import {SealingStatusInfo, SearchBox} from "./Deals";
import {Info} from "./Info";

const dealsBasePath = '/direct-deals'

export function DirectDealsPage() {
    return <PageContainer pageType="storage-deals" title="Direct Deals">
        <DirectDealsContent />
    </PageContainer>
}

function DirectDealsContent() {
    const navigate = useNavigate()
    const params = useParams()
    const pageNum = (params.pageNum && parseInt(params.pageNum)) || 1

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
        navigate(dealsBasePath)
        scrollTop()
    }

    const [searchQuery, setSearchQuery] = useState('')
    const handleSearchQueryChange = (event) => {
        if (pageNum !== 1) {
            navigate(dealsBasePath)
        }
        setSearchQuery(event.target.value)
    }
    const clearSearchBox = () => {
        if (pageNum !== 1) {
            navigate(dealsBasePath)
        }
        setSearchQuery('')
    }

    const [displayFilters, setDisplayFilters] = useState(false)
    const toggleFilters = () => {
        setDisplayFilters(!displayFilters)
    }

    const [searchFilters, setSearchFilters] = useState(null)
    const handleFiltersChanged = (event) => {
        var value = event.target.value
        if (value === "true") value = true
        if (value === "false") value = false

        var newFilters = {
            ...searchFilters || {},
            [event.target.name]: value
        }

        if (event.target.value === "") delete newFilters[event.target.name]
        if (Object.keys(newFilters).length === 0) newFilters = null

        setSearchFilters(newFilters)
    }

    // Fetch deals on this page
    const dealListOffset = (pageNum-1) * dealsPerPage
    const queryCursor = (pageNum === 1) ? null : params.cursor
    const {loading, error, data} = useQuery(DirectDealsListQuery, {
        pollInterval: searchQuery ? undefined : 10000,
        variables: {
            query: searchQuery,
            filter: searchFilters,
            cursor: queryCursor,
            offset: dealListOffset,
            limit: dealsPerPage,
        },
        fetchPolicy: 'network-only',
    })

    if (error) return <div>Error: {error.message + " - check connection to Boost server"}</div>
    if (loading) return <div>Loading...</div>

    var res = data.directDeals
    var deals = res.deals
    if (pageNum === 1) {
        deals = [...deals].sort((a, b) => b.CreatedAt.getTime() - a.CreatedAt.getTime())
        deals = deals.slice(0, dealsPerPage)
    }
    const totalCount = res.totalCount
    const moreDeals = res.more

    var cursor = params.cursor
    if (pageNum === 1 && deals.length) {
        cursor = deals[0].ID
    }

    var toggleTimestampFormat = () => saveTimestampFormat(!timestampFormat)

    const paginationParams = {
        basePath: dealsBasePath,
        cursor, pageNum, totalCount,
        rowsPerPage: dealsPerPage,
        moreRows: moreDeals,
        onRowsPerPageChange: onDealsPerPageChange,
        onLinkClick: scrollTop,
    }

    return <div className="deals">
        <SearchBox
            value={searchQuery}
            displayFilters={displayFilters}
            clearSearchBox={clearSearchBox}
            onChange={handleSearchQueryChange}
            toggleFilters={toggleFilters}
            searchFilters={searchFilters}
            handleFiltersChanged={handleFiltersChanged} />

        <table>
            <tbody>
            <tr>
                <th onClick={toggleTimestampFormat} className="start">Start</th>
                <th>Deal ID</th>
                <th>Allocation ID</th>
                <th>Client</th>
                <th>Sealing State<SealingStatusInfo/></th>
                <th>Deal State<DDODealStatusInfo/></th>
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

    const showActions = (IsPaused(deal) || IsOfflineWaitingForData(deal))
    var rowClassName = ''
    if (showActions) {
        rowClassName = 'show-actions'
    }

    return (
        <tr className={rowClassName}>
            <td className="start" onClick={props.toggleTimestampFormat}>
                {start}
            </td>
            <td className="deal-id">
                <Link to={dealsBasePath + '/' + props.deal.ID}>
                    <ShortDealID id={props.deal.ID} />
                </Link>
            </td>
            <td className="size">{deal.AllocationID+''}</td>
            <td className={'client ' + (isContractAddress(deal.ClientAddress) ? 'contract' : '')}>
                <ShortClientAddress address={deal.ClientAddress} />
            </td>
                <td className="sealing">
                        <div className="message-content">
                            <span className="message-text">
                    {deal.SealingState}
                            </span>
                        </div>
                </td>
            <td className="message">
                <div className="message-content">
                    <span className="message-text">
                        {deal.Message}
                    </span>
                    {showActions ? <DealActions deal={props.deal} refetchQueries={[DealsListQuery]} compact={true} /> : null}
                </div>
            </td>
        </tr>
    )
}

export function DirectDealsMenuItem(props) {
    const {data} = useQuery(DirectDealsCountQuery, {
        pollInterval: 10000,
        fetchPolicy: 'network-only',
    })

    return (
        <div className="menu-item" >
            <img className="icon" alt="" src={columnsGapImg} />
            <Link key="storage-deals" to={dealsBasePath}>
                    <h3>Direct Deals</h3>
            </Link>
            {data ? (
                <Link key="legacy-storage-deals" to={dealsBasePath}>
                    <div className="menu-desc">
                        <b>{data.dealsCount}</b> deal{data.dealsCount === 1 ? '' : 's'}
                    </div>
                </Link>
            ) : null}
        </div>
    )
}

export function DirectDealsCount(props) {
    const {data} = useQuery(DirectDealsCountQuery, {
        pollInterval: 10000,
        fetchPolicy: 'network-only',
    })

    if (!data) {
        return null
    }

    return (
        <Link key="direct-deals" to="/direct-deals">
            <div className="menu-desc">
                <b>{data.directDealsCount}</b> direct deal{data.directDealsCount === 1 ? '' : 's'}
            </div>
        </Link>
    )
}

function scrollTop() {
    window.scrollTo({ top: 0, behavior: "smooth" })
}

 function DDODealStatusInfo(props) {
         return <span className="deal-status-info">
         <Info>
             The deal can be in one of the following states:
             <p>
                 <i>Transfer Queued</i><br/>
                 <span>
                     The storage deal proposal has been accepted, and Boost is
                     about to start the data transfer.
                 </span>
             </p>
             <p>
                 <i>Awaiting Offline Data Import</i><br/>
                 <span>
                     The client has made an offline deal proposal, and Boost is
                     waiting for the Storage Provider operator to import the deal
                     data.
                 </span>
             </p>
             <p>
                 <i>Transferring</i><br/>
                 <span>
                     The data for the deal is transferring.
                 </span>
             </p>
             <p>
                 <i>Transfer Complete</i><br/>
                 <span>
                     The data transfer is complete and Boost is verifying the data matches commp.
                 </span>
             </p>
             <p>
                 <i>Ready to Publish</i><br/>
                 <span>
                     The deal is in the batch publish queue, ready to be published.
                 </span>
             </p>
             <p>
                 <i>Awaiting Publish Confirmation</i><br/>
                 <span>
                     Boost sent a publish deal message for the deal and is waiting for on-chain confirmation.
                 </span>
             </p>
             <p>
                 <i>Adding to Sector</i><br/>
                 <span>
                     Boost is handing the deal off to the lotus-miner sealing subsystem
                     to be added to a sector.
                 </span>
             </p>
             <p>
                 <i>Indexing</i><br/>
                 <span>
                     Boost is indexing the deal in the Local Index Directory(LID)
                     and will be announcing the deal to the network so that clients know where to retrieve it from.
                 </span>
             </p>
             <p>
                 <i>IndexedAndAnnounced</i><br/>
                 <span>
                     The deal has been indexed locally and announced to the network indexers.
                 </span>
             </p>
             <p>
                 <i>Complete</i><br/>
                 <span>
                     The deal process has finished execution.
                 </span>
             </p>
             <p>
                 <i>Cancelled</i><br/>
                 <span>
                     The deal was cancelled.
                 </span>
             </p>
         </Info>
     </span>
}
