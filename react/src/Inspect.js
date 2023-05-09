import {useQuery} from "@apollo/react-hooks";
import {
    PieceStatusQuery, PiecesWithPayloadCidQuery, PiecesWithRootPayloadCidQuery
} from "./gql";
import moment from "moment";
import {DebounceInput} from 'react-debounce-input';
import React, {useState} from "react";
import {PageContainer, ShortDealLink} from "./Components";
import {Link, useParams} from "react-router-dom";
import {dateFormat} from "./util-date";
import xImg from './bootstrap-icons/icons/x-lg.svg'
import inspectImg from './bootstrap-icons/icons/wrench.svg'
import './Inspect.css'

export function InspectMenuItem(props) {
    return (
        <Link key="inspect" className="menu-item" to="/inspect">
            <img className="icon" alt="" src={inspectImg} />
            <h3>Inspect</h3>
        </Link>
    )
}

export function InspectPage(props) {
    return <PageContainer title="Inspect Piece metadata">
        <InspectContent />
    </PageContainer>
}

function InspectContent(props) {
    const params = useParams()
    const [searchQuery, setSearchQuery] = useState(params.query)
    const handleSearchQueryChange = (event) => {
        setSearchQuery(event.target.value)
    }
    const clearSearchBox = () => {
        setSearchQuery('')
    }

    // Look up pieces by payload
    const payloadRes = useQuery(PiecesWithPayloadCidQuery, {
        variables: {
            payloadCid: searchQuery
        },
        // Don't do this query if the search query is empty
        skip: !searchQuery
    })

    // Look up pieces by root payload cid
    const rootPayloadRes = useQuery(PiecesWithRootPayloadCidQuery, {
        variables: {
            payloadCid: searchQuery
        },
        // Don't do this query if the search query is empty
        skip: !searchQuery
    })

    // If the requests for payload CID & root payload CID have completed
    var pieceCid = null
    var pieceCids = []
    if ((payloadRes || {}).data && (rootPayloadRes || {}).data) {
        pieceCids = [...new Set([
            ...payloadRes.data.piecesWithPayloadCid,
            ...rootPayloadRes.data.piecesWithRootPayloadCid
        ])]
        if (pieceCids.length === 0) {
            // If there were no results for the lookup by payload CID, use the search
            // query for a lookup by piece CID
            pieceCid = searchQuery
        } else if (pieceCids.length === 1) {
            // If there was exactly one result for the lookup by payload CID, use
            // the piece CID for the lookup by piece CID
            pieceCid = pieceCids[0]
        }
    }

    // Lookup a piece by piece CID
    const pieceRes = useQuery(PieceStatusQuery, {
        variables: {
            pieceCid: pieceCid,
        },
        // Don't do this query if there is no piece CID yet
        skip: !pieceCid
    })

    if ((pieceRes || {}).loading || (payloadRes || {}).loading || (rootPayloadRes || {}).loading) {
        return <div>Loading ...</div>
    }

    var errorMsg = ""
    if ((pieceRes || {}).error || payloadRes.error) {
        errorMsg = ((pieceRes || {}).error ? pieceRes.error.message : payloadRes.error.message)
    }

    const pieceStatus = ((pieceRes || {}).data || {}).pieceStatus
    var showPieceStats = false
    if (pieceStatus) {
        const hasPieceDeals = (pieceStatus.Deals || []).length
        const hasPieceInfos = (pieceStatus.PieceInfoDeals || []).length
        const indexStatus = ((pieceStatus || {}).IndexStatus || {}).Status
        const hasIndexInfo = indexStatus !== 'NotFound'
        showPieceStats = hasPieceDeals || hasPieceInfos || hasIndexInfo
    }

    const showPayload = pieceCids.length > 1
    var content = null
    if (!errorMsg && !pieceStatus && !showPayload) {
        content = <p>Enter piece CID or payload CID into the search box</p>
    } else if (!showPayload && !showPieceStats) {
        content = <p>No piece found with piece CID or payload CID {pieceCid}</p>
    } else {
        content = <>
            { pieceStatus ? <PieceStatus pieceCid={pieceCid} pieceStatus={pieceStatus} searchQuery={searchQuery} /> : null }
            { showPayload ? <PiecesWithPayload payloadCid={searchQuery} pieceCids={pieceCids} setSearchQuery={setSearchQuery} /> : null }
        </>
    }
    return <div className="inspect">
        <SearchBox value={searchQuery} clearSearchBox={clearSearchBox} onChange={handleSearchQueryChange} />
        { errorMsg ? <div>Error: {errorMsg}</div>  : null}
        { content }
    </div>
}

function PiecesWithPayload({payloadCid, pieceCids, setSearchQuery}) {
    return <div>
        <div className="title">Pieces with payload CID {payloadCid}:</div>
        {pieceCids.map(pc => (
            <div key={pc} className="payload-cid">
                <Link onClick={() => setSearchQuery(pc)} to={"/inspect/"+pc}>{pc}</Link>
            </div>
        ))}
    </div>
}

function PieceStatus({pieceCid, pieceStatus, searchQuery}) {
    if (!pieceStatus) {
        return <div>No piece found with piece CID {pieceCid}</div>
    }

    const rootCid = pieceStatus.Deals.length ? pieceStatus.Deals[0].Deal.DealDataRoot : null
    const searchIsPayloadCid = searchQuery && searchQuery != pieceCid && searchQuery != rootCid

    return <div className="piece-detail" id={pieceCid}>
        <div className="content">
            <table className="piece-fields">
                <tbody>
                {searchIsPayloadCid ? (
                    <tr key="payload cid">
                        <th>Searched CID (non-root)</th>
                        <td>
                            <span>{searchQuery}</span>
                            &nbsp;
                            <a className="download" target="_blank" href={"/download/block/"+searchQuery}>
                                Download block
                            </a>
                        </td>
                    </tr>
                ) : null}
                {rootCid ? (
                    <tr key="data root cid">
                        <th>Data Root CID</th>
                        <td>
                            <span>{rootCid}</span>
                            &nbsp;
                            <a className="download" target="_blank" href={"/download/block/"+rootCid}>
                                Download block
                            </a>
                        </td>
                    </tr>
                ) : null}
                <tr key="piece cid">
                    <th>Piece CID</th>
                    <td>{pieceCid}</td>
                </tr>
                <tr key="index status">
                    <th>Index Status</th>
                    <td>{pieceStatus.IndexStatus.Status}</td>
                </tr>
                </tbody>
            </table>

            <h3>Piece Store</h3>
            {pieceStatus.PieceInfoDeals.length ? (
                <table className="deals">
                    <tbody>
                    <tr>
                        <th>Chain Deal ID</th>
                        <th>Sector Number</th>
                        <th>Piece Offset</th>
                        <th>Piece Length</th>
                        <th>Unsealed</th>
                    </tr>
                    {pieceStatus.PieceInfoDeals.map(deal => (
                        <tr key={deal.ChainDealID+''}>
                            <td>{deal.ChainDealID+''}</td>
                            <td>{deal.Sector.ID+''}</td>
                            <td>{deal.Sector.Offset+''}</td>
                            <td>{deal.Sector.Length+''}</td>
                            <td><SealStatus status={deal.SealStatus} /></td>
                        </tr>
                    ))}
                    </tbody>
                </table>
            ) : (
                <p>No data found in piece store for piece CID {pieceCid}</p>
            )}

            <h3>Deals</h3>
            {pieceStatus.PieceInfoDeals.length ? (
                <table className="deals">
                    <tbody>
                    <tr>
                        <th>CreatedAt</th>
                        <th>Deal ID</th>
                        <th>Legacy Deal</th>
                        <th>Sector Number</th>
                        <th>Piece Offset</th>
                        <th>Piece Length</th>
                        <th>Unsealed</th>
                    </tr>
                    {pieceStatus.Deals.map(deal => (
                        <tr key={deal.Deal.ID}>
                            <td>{moment(deal.Deal.CreatedAt).format(dateFormat)}</td>
                            <td><ShortDealLink id={deal.Deal.ID} isLegacy={deal.Deal.IsLegacy} /></td>
                            <td>{deal.Deal.IsLegacy ? 'Yes' : 'No'}</td>
                            <td>{deal.Sector.ID+''}</td>
                            <td>{deal.Sector.Offset+''}</td>
                            <td>{deal.Sector.Length+''}</td>
                            <td><SealStatus status={deal.SealStatus} /></td>
                        </tr>
                    ))}
                    </tbody>
                </table>
            ) : (
                <p>No deals found with piece CID {pieceCid}</p>
            )}
        </div>
    </div>
}

function SealStatus({status}) {
    if (status.Error) {
        return status.Error
    }
    return status.IsUnsealed ? 'Yes' : 'No'
}

function SearchBox(props) {
    return <div className="search">
        <DebounceInput
            placeholder="piece or payload CID"
            autoFocus={!!props.value}
            minLength={4}
            debounceTimeout={300}
            value={props.value}
            onChange={props.onChange} />
        { props.value ? <img alt="clear" className="clear-text" onClick={props.clearSearchBox} src={xImg} /> : null }
    </div>
}
