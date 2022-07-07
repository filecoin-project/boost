import {useQuery} from "@apollo/react-hooks";
import {
    DealsCountQuery,
    DealsListQuery,
} from "./gql";
import moment from "moment";
import {DebounceInput} from 'react-debounce-input';
import {humanFileSize} from "./util";
import React, {useState} from "react";
import {PageContainer, ShortClientAddress, ShortDealLink} from "./Components";
import {Link, useNavigate, useParams} from "react-router-dom";
import {dateFormat} from "./util-date";
import {LegacyStorageDealsCount} from "./LegacyDeals";
import {TimestampFormat} from "./timestamp";
import {DealsPerPage} from "./deals-per-page";
import columnsGapImg from './bootstrap-icons/icons/columns-gap.svg'
import xImg from './bootstrap-icons/icons/x-lg.svg'
import './Deals.css'
import {Pagination} from "./Pagination";
import {DealActions, IsPaused, IsTransferring} from "./DealDetail";

const dealsBasePath = '/storage-deals'

export function StorageDealsPage(props) {
    return <PageContainer pageType="storage-deals" title="Storage Deals">
        <StorageDealsContent />
    </PageContainer>
}

function StorageDealsContent(props) {
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

    // Fetch deals on this page
    const dealListOffset = (pageNum-1) * dealsPerPage
    const queryCursor = (pageNum === 1) ? null : params.cursor
    const {loading, error, data} = useQuery(DealsListQuery, {
        pollInterval: searchQuery ? undefined : 1000,
        variables: {
            query: searchQuery,
            cursor: queryCursor,
            offset: dealListOffset,
            limit: dealsPerPage,
        },
        fetchPolicy: 'network-only',
    })

    if (error) return <div>Error: {error.message + " - check connection to Boost server"}</div>
    if (loading) return <div>Loading...</div>

    var res = data.deals
    var deals = res.deals
    if (pageNum === 1) {
        deals.sort((a, b) => b.CreatedAt.getTime() - a.CreatedAt.getTime())
        deals = deals.slice(0, dealsPerPage)
    }
    const totalCount = data.deals.totalCount
    const moreDeals = data.deals.more

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
        <SearchBox value={searchQuery} clearSearchBox={clearSearchBox} onChange={handleSearchQueryChange} />
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

        <Pagination {...paginationParams} />
    </div>
}

function SearchBox(props) {
    return <div className="search">
        <DebounceInput
            autoFocus={!!props.value}
            minLength={4}
            debounceTimeout={300}
            value={props.value}
            onChange={props.onChange} />
        { props.value ? <img alt="clear" class="clear-text" onClick={props.clearSearchBox} src={xImg} /> : null }
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

    const showActions = (IsPaused(deal) || IsTransferring(deal))
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
                <ShortDealLink id={deal.ID} />
            </td>
            <td className="size">{humanFileSize(deal.Transfer.Size)}</td>
            <td className="client">
                <ShortClientAddress address={deal.ClientAddress} />
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

export function StorageDealsMenuItem(props) {
    const {data} = useQuery(DealsCountQuery, {
        pollInterval: 5000,
        fetchPolicy: 'network-only',
    })

    return (
        <div className="menu-item" >
            <img className="icon" alt="" src={columnsGapImg} />
            <Link key="storage-deals" to={dealsBasePath}>
                    <h3>Storage Deals</h3>
            </Link>
            {data ? (
                <Link key="legacy-storage-deals" to={dealsBasePath}>
                    <div className="menu-desc">
                        <b>{data.dealsCount}</b> deal{data.dealsCount === 1 ? '' : 's'}
                    </div>
                </Link>
            ) : null}

            <LegacyStorageDealsCount />
        </div>
    )
}

function scrollTop() {
    window.scrollTo({ top: 0, behavior: "smooth" })
}
