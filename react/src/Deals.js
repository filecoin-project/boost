import {useQuery} from "@apollo/react-hooks";
import {
    DealsCountQuery,
    DealsListQuery, LegacyDealsCountQuery,
} from "./gql";
import moment from "moment";
import {DebounceInput} from 'react-debounce-input';
import {humanFileSize, isContractAddress} from "./util";
import React, {useState, useEffect, useRef} from "react";
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
import {DealActions, IsPaused, IsTransferring, IsOfflineWaitingForData} from "./DealDetail";
import {humanTransferRate} from "./DealTransfers";

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
    const {loading, error, data} = useQuery(DealsListQuery, {
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
        <LegacyDealsLink />
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

function LegacyDealsLink(props) {
    const {data} = useQuery(LegacyDealsCountQuery, {
        pollInterval: 10000,
        fetchPolicy: 'network-only',
    })

    if (!data || !data.legacyDealsCount) {
        return null
    }

    return (
        <Link key="legacy-storage-deals" className="legacy-storage-deals-link" to="/legacy-storage-deals">
            Show legacy deals ➜
        </Link>
    )
}

export function SearchBox(props) {
    const searchFilters = props.searchFilters || {}
    const displayFilters = props.displayFilters
    const toggleFilters = props.toggleFilters
    const ref = useRef()

    useEffect(() => {
        const checkIfClickedOutside = e => {
          // If the menu is open and the clicked target is not within the menu,
          // then close the menu
          if (displayFilters && ref.current && !ref.current.contains(e.target)) {
            toggleFilters()
          }
        }

        document.addEventListener("mousedown", checkIfClickedOutside)

        return () => {
          document.removeEventListener("mousedown", checkIfClickedOutside)
        }
      }, [displayFilters])

    return <div className="search">
        <DebounceInput
            autoFocus={!!props.value}
            minLength={4}
            debounceTimeout={300}
            value={props.value}
            onChange={props.onChange} />
        { props.value ? <img alt="clear" className="clear-text" onClick={props.clearSearchBox} src={xImg} /> : null }
        <div ref={ref} className={(props.displayFilters ? "active": "") + " search-toggle"}>
            <div className="toggle" onClick={props.toggleFilters}>
                <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" className="bi bi-list" viewBox="0 0 16 16">
                    <path fillRule="evenodd" d="M2.5 12a.5.5 0 0 1 .5-.5h10a.5.5 0 0 1 0 1H3a.5.5 0 0 1-.5-.5zm0-4a.5.5 0 0 1 .5-.5h10a.5.5 0 0 1 0 1H3a.5.5 0 0 1-.5-.5zm0-4a.5.5 0 0 1 .5-.5h10a.5.5 0 0 1 0 1H3a.5.5 0 0 1-.5-.5z"/>
                </svg>
            </div>
            <div className={(props.displayFilters ? "": "hidden") + " search-filters"}>
                <h3>Checkpoint</h3>
                <div>
                    <RadioGroup prefix="CP" field="Checkpoint" value="" label="Any"
                        searchFilters={searchFilters} handleFiltersChanged={props.handleFiltersChanged} />
                    <RadioGroup prefix="CP" field="Checkpoint" value="Accepted"
                        searchFilters={searchFilters} handleFiltersChanged={props.handleFiltersChanged} />
                    <RadioGroup prefix="CP" field="Checkpoint" value="Transferred"
                        searchFilters={searchFilters} handleFiltersChanged={props.handleFiltersChanged} />
                    <RadioGroup prefix="CP" field="Checkpoint" value="Published"
                        searchFilters={searchFilters} handleFiltersChanged={props.handleFiltersChanged} />
                    <RadioGroup prefix="CP" field="Checkpoint" value="PublishConfirmed"
                        searchFilters={searchFilters} handleFiltersChanged={props.handleFiltersChanged} />
                    <RadioGroup prefix="CP" field="Checkpoint" value="AddedPiece"
                        searchFilters={searchFilters} handleFiltersChanged={props.handleFiltersChanged} />
                    <RadioGroup prefix="CP" field="Checkpoint" value="IndexedAndAnnounced"
                        searchFilters={searchFilters} handleFiltersChanged={props.handleFiltersChanged} />
                    <RadioGroup prefix="CP" field="Checkpoint" value="Complete"
                        searchFilters={searchFilters} handleFiltersChanged={props.handleFiltersChanged} />
                </div>
                <hr />
                <h3>IsOffline</h3>
                <div>
                    <RadioGroup prefix="IO" field="IsOffline" value="" label="Any"
                        searchFilters={searchFilters} handleFiltersChanged={props.handleFiltersChanged} />
                    <RadioGroup prefix="IO" field="IsOffline" value={true}
                        searchFilters={searchFilters} handleFiltersChanged={props.handleFiltersChanged} />
                    <RadioGroup prefix="IO" field="IsOffline" value={false}
                        searchFilters={searchFilters} handleFiltersChanged={props.handleFiltersChanged} />
                </div>
            </div>
        </div>
    </div>
}

function RadioGroup (props) {
    const {
        prefix,
        field,
        value,
        label,
        searchFilters,
        handleFiltersChanged
    } = props

    const htmlFor = prefix + '-' + value
    var checked = false

    if (searchFilters[field] === value) {
        checked = true
    } else if (value === "" && searchFilters[field] === undefined) {
        checked = true
    }
    return (
        <span className="radio-group">
            <input type="radio" id={htmlFor} name={field} value={value.toString()}
                checked={checked}
                onChange={handleFiltersChanged} />
            <label htmlFor={htmlFor}>{value === "" ? label : value.toString()}</label>
        </span>
    )
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

    const showActions = (IsPaused(deal) || IsTransferring(deal) || IsOfflineWaitingForData(deal))
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
            <td className={'client ' + (isContractAddress(deal.ClientAddress) ? 'contract' : '')}>
                <ShortClientAddress address={deal.ClientAddress} />
            </td>
            <td className="message">
                <div className="message-content">
                    <span className="message-text">
                        {deal.Message}
                        <TransferRate deal={deal} />
                    </span>
                    {showActions ? <DealActions deal={props.deal} refetchQueries={[DealsListQuery]} compact={true} /> : null}
                </div>
            </td>
        </tr>
    )
}

function TransferRate({deal}) {
    if (!IsTransferring(deal) || IsPaused(deal) || deal.Transferred === 0 || deal.IsTransferStalled) {
        return null
    }

    if(deal.TransferSamples.length < 2) {
        return null
    }

    // Clone from read-only to writable array and sort points
    var points = deal.TransferSamples.map(p => ({ At: p.At, Bytes: p.Bytes }))
    points.sort((a, b) => a.At.getTime() - b.At.getTime())

    // Get the average rate from the last 10 seconds of samples.
    points = points.slice(-10)
    // Allow for some clock skew, but ignore samples older than 2 minutes
    const cutOff = new Date(new Date().getTime() - 2*60*1000)
    var samples = []
    for (const pt of points) {
        if (pt.At > cutOff) {
            samples.push(pt)
        }
    }
    if (!samples.length) {
        return null
    }

    // Get the delta between the first sample and last sample.
    const delta = samples[samples.length-1].Bytes - samples[0].Bytes

    return <span className="transfer-rate">
        {humanTransferRate(Number(delta) / samples.length)}
    </span>
}

export function StorageDealsMenuItem(props) {
    const {data} = useQuery(DealsCountQuery, {
        pollInterval: 10000,
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
