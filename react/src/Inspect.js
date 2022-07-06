import {useQuery} from "@apollo/react-hooks";
import {
    DealsCountQuery, PieceStatusQuery, PiecesWithPayloadCidQuery, SealingPipelineQuery,
} from "./gql";
import moment from "moment";
import {DebounceInput} from 'react-debounce-input';
import React, {useState} from "react";
import {PageContainer, ShortDealLink} from "./Components";
import {Link, useParams} from "react-router-dom";
import {dateFormat} from "./util-date";
import columnsGapImg from './bootstrap-icons/icons/columns-gap.svg'
import xImg from './bootstrap-icons/icons/x-lg.svg'
import './Inspect.css'

const piecesBasePath = '/pieces'

export function InspectPage(props) {
    return <PageContainer title="Piece Metadata">
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


    // Payload
    // bafk2bzacedsnewk7clxwab2wgwyoi7u5tzdhldx7fkxpqdq7unrxz2zoy4d2g
    // Piece
    // 'baga6ea4seaqgbkvk6apsnfbtvnbjabpu4tsv6ns45c36452fdhkqxznt3vlnedq'

    const payloadRes = useQuery(PiecesWithPayloadCidQuery, {
        variables: {
            payloadCid: searchQuery
        },
        skip: !searchQuery
    })

    var pieceCid = null
    var pieceCids = []
    if (payloadRes && payloadRes.data) {
        console.log('pieces with payload cid', payloadRes.data.piecesWithPayloadCid)
        pieceCids = payloadRes.data.piecesWithPayloadCid
        if (pieceCids.length === 0) {
            pieceCid = searchQuery
        } else if (pieceCids.length === 1) {
            pieceCid = pieceCids[0]
        }
    }

    console.log('piece cid', pieceCid)
    const pieceRes = useQuery(PieceStatusQuery, {
        variables: {
            pieceCid: pieceCid,
        },
        skip: !pieceCid
    })

    if ((pieceRes || {}).loading || payloadRes.loading) {
        return <div>Loading ...</div>
    }

    var errorMsg = ""
    if ((pieceRes || {}).error || payloadRes.error) {
        errorMsg = ((pieceRes || {}).error ? pieceRes.error.message : payloadRes.error.message)
    }

    const pieceStatus = ((pieceRes || {}).data || {}).pieceStatus
    const showPayload = pieceCids.length > 1
    const showInstructions = !errorMsg && !pieceStatus && !showPayload
    return <div className="inspect">
        <SearchBox value={searchQuery} clearSearchBox={clearSearchBox} onChange={handleSearchQueryChange} />
        { errorMsg ? <div>Error: {errorMsg}</div>  : null}
        { pieceStatus ? <PieceStatus pieceCid={pieceCid} pieceStatus={pieceStatus} /> : null }
        { showPayload ? <PiecesWithPayload payloadCid={searchQuery} pieceCids={pieceCids} setSearchQuery={setSearchQuery} /> : null }
        { showInstructions ? <p>Enter the piece CID or payload CID into the Search Box</p> : null }
    </div>
}

function PiecesWithPayload({payloadCid, pieceCids, setSearchQuery}) {
    return <div>
        <div className="title">Pieces with payload CID {payloadCid}</div>
        {pieceCids.map(pc => (
            <div key={pc}>
                <Link onClick={() => setSearchQuery(pc)} to={"/inspect/"+pc}>{pc}</Link>
            </div>
        ))}
    </div>
}

function PieceStatus({pieceCid, pieceStatus}) {
    if (!pieceStatus) {
        return <div>No piece found with piece CID {pieceCid}</div>
    }

    const rootCid = pieceStatus.Deals.length ? pieceStatus.Deals[0].Deal.DealDataRoot : null

    return <div className="piece-detail" id={pieceCid}>
        <div className="content">
            <table className="piece-fields">
                <tbody>
                <tr key="piece cid">
                    <th>Piece CID</th>
                    <td>{pieceCid}</td>
                </tr>
                {rootCid ? (
                    <tr key="data root cid">
                        <th>Data Root CID</th>
                        <td>{rootCid}</td>
                    </tr>
                ) : null}
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

export function PiecesMenuItem(props) {
    const {data} = useQuery(DealsCountQuery, {
        pollInterval: 5000,
        fetchPolicy: 'network-only',
    })

    return (
        <div className="menu-item" >
            <img className="icon" alt="" src={columnsGapImg} />
            <Link key="pieces" to={piecesBasePath}>
                <h3>Pieces</h3>
            </Link>
        </div>
    )
}

function searchPayloadCid(payloadCid) {
    const pieces = []
    for (const p of mockPieces) {
        if (p.RootCid == payloadCid) {
            pieces.push(p)
        }
    }

    return {
        data: {
            payload: {
                RootCid: payloadCid,
                Pieces: pieces
            }
        }
    }
}

function searchPieceCid(pieceCid) {
    for (const p of mockPieces) {
        if (p.PieceCid == pieceCid) {
            return {
                data: { piece: p }
            }
        }
    }

    return {
        data: { piece: null }
    }
}

const mockPieces = [{
    PieceCid: 'baga6ea4seaqgbkvk6apsnfbtvnbjabpu4tsv6ns45c36452fdhkqxznt3vlnedq',
    RootCid: 'bafybeiagwnqiviaae5aet2zivwhhsorg75x2wka2pu55o7grr23ulx5kxm',
    CidCount: '3214',
    IndexStatus: 'Complete',
    Deals: [{
        DealUUID: '237fb3d0-fc11-40dc-a77a-0b75363ffa5e',
        ChainDealID: '1',
        SectorID: 1,
        PieceLength: 2048,
        PieceOffset: 0,
        IsUnsealed: true,
    }, {
        DealUUID: '25332be9-54b4-471e-a669-0757f6b61fe8',
        ChainDealID: '2',
        SectorID: 4,
        PieceLength: 2048,
        PieceOffset: 2048,
        IsUnsealed: false,
    }]
}, {
    PieceCid: 'bafybeig3yg2msah74sgvow25uxddqbabex3f3mh6hysess3w5kmgiv6zqy',
    RootCid: 'bafybeiagwnqiviaae5aet2zivwhhsorg75x2wka2pu55o7grr23ulx5kxm',
    CidCount: '53234',
    IndexStatus: 'Indexing',
    Deals: [{
        DealUUID: '237fb3d0-fc11-40dc-a77a-0b75363ffa5e',
        ChainDealID: '1',
        SectorID: 1,
        PieceLength: 2048,
        PieceOffset: 0,
        IsUnsealed: true,
    }, {
        DealUUID: '25332be9-54b4-471e-a669-0757f6b61fe8',
        ChainDealID: '2',
        SectorID: 4,
        PieceLength: 2048,
        PieceOffset: 2048,
        IsUnsealed: false,
    }]
}, {
    PieceCid: 'bafybeiaij7wqrolyv5gx2glkordkq22yacpgg23bdwyweenwlknk37zjyu',
    RootCid: 'bafybeigbb6jrtwwdlqtqu23uzvsezm5m7qpw57emqipy5muclgp5dakpmq',
    CidCount: '14921',
    IndexStatus: 'Complete',
    Deals: [{
        DealUUID: '237fb3d0-fc11-40dc-a77a-0b75363ffa5e',
        ChainDealID: '1',
        SectorID: 1,
        PieceLength: 2048,
        PieceOffset: 0,
        IsUnsealed: true,
    }, {
        DealUUID: '25332be9-54b4-471e-a669-0757f6b61fe8',
        ChainDealID: '2',
        SectorID: 4,
        PieceLength: 2048,
        PieceOffset: 2048,
        IsUnsealed: false,
    }]
}, {
    PieceCid: 'bafybeicl6ujc6ncfktctxxroxognfn7d2fqavvrryoc2lv6m4i6hpbkfti',
    Deals: [{
        DealUUID: '237fb3d0-fc11-40dc-a77a-0b75363ffa5e',
        ChainDealID: '1',
        SectorID: 1,
        PieceLength: 2048,
        PieceOffset: 0,
        IsUnsealed: true,
    }, {
        DealUUID: '25332be9-54b4-471e-a669-0757f6b61fe8',
        ChainDealID: '2',
        SectorID: 4,
        PieceLength: 2048,
        PieceOffset: 2048,
        IsUnsealed: true,
    }, {
        DealUUID: '237fb3d0-fc11-40dc-a77a-0b75363ffa5e',
        ChainDealID: '3',
        SectorID: 6,
        PieceLength: 2048,
        PieceOffset: 0,
        IsUnsealed: false,
    }]
}, {
    PieceCid: 'bafybeihhponjv2pdbmg33nxvccrgj34546avahl3nojk5cplawofvn2d3m',
    Deals: [{
        DealUUID: '25332be9-54b4-471e-a669-0757f6b61fe8',
        ChainDealID: '2',
        SectorID: 4,
        PieceLength: 2048,
        PieceOffset: 2048,
        IsUnsealed: true,
    }]
}]
