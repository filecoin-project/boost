import {useQuery} from "@apollo/react-hooks";
import {
    DealsCountQuery,
} from "./gql";
import moment from "moment";
import {DebounceInput} from 'react-debounce-input';
import React, {useState} from "react";
import {PageContainer, ShortDealLink} from "./Components";
import {Link} from "react-router-dom";
import {dateFormat} from "./util-date";
import columnsGapImg from './bootstrap-icons/icons/columns-gap.svg'
import xImg from './bootstrap-icons/icons/x-lg.svg'
import './PieceMeta.css'
import {addCommas} from "./util";

const piecesBasePath = '/pieces'

export function PieceMetaPage(props) {
    return <PageContainer title="Piece Metadata">
        <PieceMetaContent />
    </PageContainer>
}

function PieceMetaContent(props) {
    const [searchQuery, setSearchQuery] = useState('')
    const handleSearchQueryChange = (event) => {
        setSearchQuery(event.target.value)
    }
    const clearSearchBox = () => {
        setSearchQuery('')
    }

    const pieceRes = searchPieceCid(searchQuery)
    const payloadRes = searchPayloadCid(searchQuery)

    if (pieceRes.error || payloadRes.error) {
        return <div>Error: {pieceRes.error ? pieceRes.error.message : payloadRes.error.message}</div>
    }

    if (pieceRes.loading || payloadRes.loading) {
        return <div>Loading ...</div>
    }

    const showPieces = !!pieceRes.data.piece
    const showPayload = !!payloadRes.data.payload.Pieces.length
    const showInstructions = !showPieces && !showPayload
    return <div className="piece-meta">
        <SearchBox value={searchQuery} clearSearchBox={clearSearchBox} onChange={handleSearchQueryChange} />

        { showPieces ? <PieceMetaDetail piece={pieceRes.data.piece} /> : null }
        { showPayload ? <PayloadMetaDetail payload={payloadRes.data.payload} /> : null }
        { showInstructions ? <p>Enter the piece CID or payload CID into the Search Box</p> : null }
    </div>
}

export function PieceMetaDetail({piece}) {
    if (!piece) {
        return <div>No piece found with piece CID {piece.PieceCid}</div>
    }

    return <div className="piece-detail" id={piece.PieceCid}>
        <div className="content">
            <table className="piece-fields">
                <tbody>
                <tr>
                    <th>Piece CID</th>
                    <td>{piece.PieceCid}</td>
                </tr>
                <tr>
                    <th>Data Root CID</th>
                    <td>{piece.RootCid}</td>
                </tr>
                <tr>
                    <th>Indexed CIDs</th>
                    <td>{addCommas(piece.CidCount)}</td>
                </tr>
                <tr>
                    <th>Index Status</th>
                    <td>{piece.IndexStatus}</td>
                </tr>
                </tbody>
            </table>

            <div className="title">Deals</div>
            <table className="deals">
                <tbody>
                <tr>
                    <th>CreatedAt</th>
                    <th>Deal UUID</th>
                    <th>Chain Deal ID</th>
                    <th>Sector Number</th>
                    <th>Piece Offset</th>
                    <th>Piece Length</th>
                    <th>Unsealed</th>
                </tr>
                {piece.Deals.map(deal => (
                    <tr key={deal.DealUUID}>
                        <td>{moment().format(dateFormat)}</td>
                        <td><ShortDealLink id={deal.DealUUID} /></td>
                        <td>{deal.ChainDealID}</td>
                        <td>{deal.SectorID}</td>
                        <td>{deal.PieceOffset}</td>
                        <td>{deal.PieceLength}</td>
                        <td>{deal.IsUnsealed ? 'Yes' : 'No'}</td>
                    </tr>
                ))}
                </tbody>
            </table>
        </div>
    </div>
}

export function PayloadMetaDetail({payload}) {
    if (payload.Pieces.length === 0) {
        return <div>No piece found with payload CID {payload.RootCid}</div>
    }

    return <div>
        <div class="title">Pieces containing payload CID {payload.RootCid}</div>
        {payload.Pieces.map(piece => <PieceMetaDetail key={piece.PieceCid} piece={piece} />)}
    </div>
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
        { props.value ? <img alt="clear" class="clear-text" onClick={props.clearSearchBox} src={xImg} /> : null }
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
