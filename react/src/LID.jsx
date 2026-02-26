import {useMutation, useQuery} from "@apollo/client";
import {
    LIDQuery,
    FlaggedPiecesQuery, PieceBuildIndexMutation,
    PieceStatusQuery, PiecesWithPayloadCidQuery, FlaggedPiecesCountQuery, PiecePayloadCidsQuery,
} from "./gql";
import moment from "moment";
import {DebounceInput} from 'react-debounce-input';
import React, {useState, useEffect} from "react";
import {PageContainer, ShortDealLink} from "./Components";
import {Link, useNavigate, useParams} from "react-router-dom";
import {dateFormat} from "./util-date";
import xImg from './bootstrap-icons/icons/x-lg.svg'
import lidImg from './bootstrap-icons/icons/wrench.svg'
import './LID.css'
import './Inspect.css'
import {Pagination} from "./Pagination";
import {Info, InfoListItem} from "./Info";
import {CumulativeBarChart, CumulativeBarLabels} from "./CumulativeBarChart";
import {addCommas} from "./util";
import closeImg from "./bootstrap-icons/icons/x-circle.svg";
import {getServerHttpEndpoint} from "./config";

const lidBasePath = '/piece-doctor'
const downloadBlockHref = (cid) => `${getServerHttpEndpoint()}/download/block/${cid}`

export function LIDMenuItem(props) {
    return (
        <Link key="lid" className="menu-item" to={"/lid"}>
            <img className="icon" alt="" src={lidImg} />
            <h3>Local Index Directory</h3>
        </Link>
    )
}

// Landing page for LID
export function LIDPage(props) {
    return <PageContainer title="Local Index Directory">
        <LIDContent />
    </PageContainer>
}

function LIDContent() {
    const {loading, error, data} = useQuery(LIDQuery, { pollInterval: 30000 })

    if (loading) {
        return <div>Loading...</div>
    }
    if (error) {
        return <div>Error: {error.message}</div>
    }

    const d = data.lid

    const piecesBars = [{
        name: 'Indexed',
        className: 'indexed',
        amount: d.Pieces.Indexed,
        description: ''
    }, {
        name: 'Flagged (unsealed)',
        className: 'flagged',
        amount: d.Pieces.FlaggedUnsealed,
        description: ''
    }, {
        name: 'Flagged (sealed only)',
        className: 'sealed',
        amount: d.Pieces.FlaggedSealed,
        description: ''
    }]

    const barsSuc = [{
        name: 'Unsealed',
        className: 'unsealed',
        amount: d.SectorUnsealedCopies.Unsealed,
        description: ''
    }, {
        name: 'Sealed Only',
        className: 'sealed',
        amount: d.SectorUnsealedCopies.Sealed,
        description: ''
    }]

    const barsSps = [{
        name: 'Active',
        className: 'active',
        amount: d.SectorProvingState.Active,
        description: ''
    }, {
        name: 'Inactive',
        className: 'inactive',
        amount: d.SectorProvingState.Inactive,
        description: ''
    }]

    var lastScanMsg = ''
    var lastScan = d.ScanProgress.LastScan
    const now = new Date()
    if (!lastScan) {
        lastScan = now
    }
    if (lastScan > now) {
        lastScanMsg = 'just now'
    } else if (now.getTime() - lastScan.getTime() < 60 * 1000) {
        lastScanMsg = 'just now'
    } else {
        lastScanMsg = moment(lastScan).fromNow() + ' ago'
    }
    return <div className="lid">
      <div className="last-scan">
          Last updated: {moment(lastScan).format(dateFormat)} ({lastScanMsg})
      </div>

      <h3>Pieces<PiecesInfo/></h3>

      <div className="storage-chart">
          <CumulativeBarChart bars={piecesBars} />
          <CumulativeBarLabels bars={piecesBars} />
      </div>

      <div className="flagged-pieces-link">
          <h3>
              Flagged Pieces
              <Info>
                  Flagged Pieces are pieces that have been flagged by the Piece Doctor because it was
                  not possible to index the piece data. This could be because there was no unsealed copy
                  of the piece data, or because the piece data was inaccessible or corrupted.
              </Info>
          </h3>
          <p>
              <b>{addCommas(d.FlaggedPieces)}</b> Flagged Pieces
              <Link to={"/piece-doctor"} className="button">View Flagged Pieces</Link>
          </p>
      </div>

      <div>
          <h3>
              Deal Sectors Copies
              <Info>
                  Deal Sectors Copies indicates how many sectors contain deals, and how many of those
                  sectors have an unsealed copy.
              </Info>
          </h3>

          <div className="storage-chart">
              <CumulativeBarChart bars={barsSuc} />
              <CumulativeBarLabels bars={barsSuc} />
          </div>
      </div>

      <div>
          <h3>
              Sectors Proving State
              <Info>
                Sectors Proving State indicates how many sectors this SP is actively proving on chain
              </Info>
          </h3>

          <div className="storage-chart">
              <CumulativeBarChart bars={barsSps} />
              <CumulativeBarLabels bars={barsSps} />
          </div>
      </div>
    </div>
}

function PiecesInfo() {
    return <Info>
        The pieces stored by the Local Index Directory are in one of these states:
        <p>
            <b>Indexed</b><br/>
            The piece was successfully indexed and all CIDs within it are retrievable
        </p>
        <p>
            <b>Flagged (unsealed)</b><br/>
            Flagged by the Piece Doctor because there was some problem
            creating an index. This could be because it was not possible
            to read the data from the sealing subsystem, the data is
            corrupt, etc.
        </p>
        <p>
            <b>Flagged (sealed only)</b><br/>
            Flagged by the Piece Doctor because there is no unsealed copy
            of the piece data. This usually means the unsealed copy of the
            sector containing the piece was deleted.
        </p>
    </Info>
}

// Page listing pieces flagged by the piece doctor
export function PieceDoctorPage(props) {
    return <PageContainer title="Piece Doctor">
        <PieceDoctorContent />
    </PageContainer>
}

function PieceDoctorContent() {
    const params = useParams()
    const [searchQuery, setSearchQuery] = useState(params.query)

    const flaggedPiecesContent = searchQuery ? null : <FlaggedPieces setSearchQuery={setSearchQuery}  />

    const showSearchPrompt = flaggedPiecesContent == null
    return <div className="inspect-content">
        { <SearchResults searchQuery={searchQuery} setSearchQuery={setSearchQuery} showSearchPrompt={showSearchPrompt} /> }
        { flaggedPiecesContent }
    </div>
}

function FlaggedPieces({setSearchQuery}) {
    const navigate = useNavigate()
    const params = useParams()
    const pageNum = (params.pageNum && parseInt(params.pageNum)) || 1

    var [rowsPerPage, setRowsPerPage] = useState(RowsPerPage.load)
    const onRowsPerPageChange = (e) => {
        const val = parseInt(e.target.value)
        RowsPerPage.save(val)
        setRowsPerPage(val)
        navigate("/piece-doctor")
        scrollTop()
    }

    // Fetch rows on this page
    const listOffset = (pageNum-1) * rowsPerPage
    const queryCursor = (pageNum === 1) ? null : params.cursor
    const {loading, error, data} = useQuery(FlaggedPiecesQuery, {
        pollInterval: 10000,
        variables: {
            cursor: queryCursor,
            offset: listOffset,
            limit: rowsPerPage,
            hasUnsealedCopy: true,
        },
        fetchPolicy: 'network-only',
    })

    if (error) return <div>Error: {error.message + " - check connection to Boost server"}</div>
    if (loading) return <div>Loading...</div>

    var res = data.piecesFlagged
    var rows = res.pieces
    const totalCount = data.piecesFlagged.totalCount
    const moreRows = data.piecesFlagged.more

    var cursor = params.cursor
    if (pageNum === 1 && rows.length) {
        cursor = rows[0].CreatedAt.getTime()
    }

    const paginationParams = {
        basePath: "/piece-doctor",
        cursor, pageNum, totalCount,
        rowsPerPage: rowsPerPage,
        moreRows: moreRows,
        onRowsPerPageChange: onRowsPerPageChange,
        onLinkClick: scrollTop,
    }

    return <div className="flagged-pieces">
        <NoUnsealedSectorLink />

        <h3>
            Flagged pieces ({totalCount})
        </h3>

        { totalCount ? (
            <>
            <table>
                <tbody>
                <tr>
                    <th>Piece CID</th>
                    <th>Index</th>
                    <th>Deals</th>
                </tr>

                {rows.map(piece => (
                    <FlaggedPieceRow
                        key={piece.PieceCid}
                        piece={piece}
                        setSearchQuery={setSearchQuery}
                    />
                ))}
                </tbody>
            </table>

            <Pagination {...paginationParams} />
            </>
        ) : (
            <div className="flagged-pieces-none">
                Boost doctor did not find any pieces with errors
            </div>
        )}
    </div>
}

function NoUnsealedSectorLink() {
    const {loading, error, data} = useQuery(FlaggedPiecesCountQuery, {
        pollInterval: 10000,
        variables: {
            hasUnsealedCopy: false,
        },
        fetchPolicy: 'network-only',
    })

    if (error) return <div>Error: {error.message}</div>
    if (loading) {
        return <div>&nbsp;</div>
    }

    return <div>
        <Link className="nav-link" to="/no-unsealed">See {data.piecesFlaggedCount} pieces with no unsealed copy ➜</Link>
    </div>
}

function FlaggedPieceRow({piece}) {
    return <tr>
        <td>
            <Link to={"/piece-doctor/piece/"+piece.PieceCid}>
                {piece.PieceCid}
            </Link>
        </td>
        <td>{piece.IndexStatus.Status}</td>
        <td>{piece.DealCount}</td>
    </tr>
}

function hasUnsealedCopy(piece) {
    for (var dl of piece.Deals) {
        if (dl.SealStatus.Status === 'HasUnsealedCopy') {
            return true
        }
    }
    return false
}

export function NoUnsealedSectorPage(props) {
    return <PageContainer title="Piece Doctor">
        <NoUnsealedSectorPieces />
    </PageContainer>
}

function NoUnsealedSectorPieces() {
    const navigate = useNavigate()
    const params = useParams()
    const pageNum = (params.pageNum && parseInt(params.pageNum)) || 1

    var [rowsPerPage, setRowsPerPage] = useState(RowsPerPage.load)
    const onRowsPerPageChange = (e) => {
        const val = parseInt(e.target.value)
        RowsPerPage.save(val)
        setRowsPerPage(val)
        navigate(lidBasePath)
        scrollTop()
    }

    // Fetch rows on this page
    const listOffset = (pageNum-1) * rowsPerPage
    const queryCursor = (pageNum === 1) ? null : params.cursor
    const {loading, error, data} = useQuery(FlaggedPiecesQuery, {
        pollInterval: 10000,
        variables: {
            cursor: queryCursor,
            offset: listOffset,
            limit: rowsPerPage,
            hasUnsealedCopy: false,
        },
        fetchPolicy: 'network-only',
    })

    if (error) return <div>Error: {error.message + " - check connection to Boost server"}</div>
    if (loading) return <div>Loading...</div>

    var res = data.piecesFlagged
    var rows = res.pieces
    const totalCount = data.piecesFlagged.totalCount
    const moreRows = data.piecesFlagged.more

    var cursor = params.cursor
    if (pageNum === 1 && rows.length) {
        cursor = rows[0].CreatedAt.getTime()
    }

    const paginationParams = {
        basePath: '/no-unsealed',
        cursor, pageNum, totalCount,
        rowsPerPage: rowsPerPage,
        moreRows: moreRows,
        onRowsPerPageChange: onRowsPerPageChange,
        onLinkClick: scrollTop,
    }

    return <div className="flagged-pieces inspect-content">
        <FlaggedPiecesLink />

        <h3>
            Pieces with no unsealed sector ({totalCount})
        </h3>

        { totalCount ? (
            <>
            <table>
                <tbody>
                <tr>
                    <th>Piece CID</th>
                    <th>Index</th>
                    <th>Deals</th>
                </tr>

                {rows.map(piece => (
                    <FlaggedPieceRow
                        key={piece.PieceCid}
                        piece={piece}
                    />
                ))}
                </tbody>
            </table>

            <Pagination {...paginationParams} />
            </>
        ) : (
            <div className="flagged-pieces-none">
                Boost doctor did not find any pieces with no unsealed sector
            </div>
        )}
    </div>
}

function FlaggedPiecesLink() {
    const {loading, error, data} = useQuery(FlaggedPiecesCountQuery, {
        pollInterval: 10000,
        variables: {
            hasUnsealedCopy: true,
        },
        fetchPolicy: 'network-only',
    })

    if (error) return <div>Error: {error.message}</div>
    if (loading) {
        return <div>&nbsp;</div>
    }

    return <div>
        <Link className="nav-link" to="/piece-doctor">See {data.piecesFlaggedCount} flagged pieces ➜</Link>
    </div>
}


// Page showing information about a particular piece
export function InspectPiecePage(props) {
    const params = useParams()

    return <PageContainer title="Inspect Piece metadata">
        <div className="inspect-content">
            <SearchResults searchQuery={params.pieceCID} />
        </div>
    </PageContainer>
}

function SearchResults({searchQuery, setSearchQuery, showSearchPrompt}) {
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

    // If the requests for payload CID have completed
    var pieceCid = null
    var pieceCids = []
    if ((payloadRes || {}).data) {
        if (payloadRes.data.piecesWithPayloadCid.length === 0) {
            // If there were no results for the lookup by payload CID, use the search
            // query for a lookup by piece CID
            pieceCid = searchQuery
        } else if (payloadRes.data.piecesWithPayloadCid.length === 1) {
            // If there was exactly one result for the lookup by payload CID, use
            // the piece CID for the lookup by piece CID
            pieceCid = payloadRes.data.piecesWithPayloadCid[0]
        }
    }

    // Lookup a piece by piece CID
    const pieceRes = useQuery(PieceStatusQuery, {
        pollInterval: 10000,
        variables: {
            pieceCid: pieceCid,
        },
        // Don't do this query if there is no piece CID yet
        skip: !pieceCid
    })

    if ((pieceRes || {}).loading || (payloadRes || {}).loading) {
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
        if (showSearchPrompt) {
            content = <p>Enter piece CID or payload CID into the search box</p>
        }
    } else if (!showPayload && !showPieceStats) {
        content = <p>No piece found with piece CID or payload CID {pieceCid}</p>
    } else {
        content = <>
            { pieceStatus ? <PieceStatus pieceCid={pieceCid} pieceStatus={pieceStatus} searchQuery={searchQuery} /> : null }
            { showPayload ? <PiecesWithPayload payloadCid={searchQuery} pieceCids={pieceCids} setSearchQuery={setSearchQuery} /> : null }
        </>
    }
    return <div className="inspect">
        { setSearchQuery ? (
            <SearchBox value={searchQuery} clearSearchBox={clearSearchBox} onChange={handleSearchQueryChange} />
        ) : null }
        { errorMsg ? <div>Error: {errorMsg}</div>  : null}
        { content }
    </div>
}

function PiecesWithPayload({payloadCid, pieceCids, setSearchQuery}) {
    return <div>
        <div className="title">Pieces with payload CID {payloadCid}:</div>
        {pieceCids.map(pc => (
            <div key={pc} className="payload-cid">
                <Link onClick={() => setSearchQuery(pc)} to={"/piece-doctor/"+pc}>{pc}</Link>
            </div>
        ))}
    </div>
}

function PieceStatus({pieceCid, pieceStatus, searchQuery}) {
    // Re-build index
    const [buildIndex, buildIndexResp] = useMutation(PieceBuildIndexMutation, {
        // refetchQueries: props.refetchQueries,
        variables: {pieceCid: pieceCid}
    })

    if (!pieceStatus) {
        return <div>No piece found with piece CID {pieceCid}</div>
    }

    const searchIsAnyCid = searchQuery && searchQuery !== pieceCid
    const searchIsPieceCid = searchQuery && searchQuery === pieceCid
    const indexFailed = pieceStatus.IndexStatus.Status === 'Failed'
    const indexRegistered = pieceStatus.IndexStatus.Status === 'Registered'
    const canReIndex = (indexFailed || indexRegistered) && hasUnsealedCopy(pieceStatus)

    return <div className="piece-detail" id={pieceCid}>
        <div className="content">
            <table className="piece-fields">
                <tbody>
                {searchIsAnyCid ? (
                    <tr key="payload cid">
                        <th>Searched CID</th>
                        <td>
                            <span><strong>{searchQuery}</strong></span>
                            &nbsp;
                            <a className="download" target="_blank" href={downloadBlockHref(searchQuery)} rel="noreferrer">
                                Download block
                            </a>
                        </td>
                    </tr>
                ) : null}
                <tr key="piece cid">
                    <th>Piece CID</th>
                    {searchIsPieceCid ? (
                      <td>
                          <strong>{pieceCid}</strong>
                          &nbsp;
                          <a className="payload-icon" href={"/piece-doctor/piece-payload/"+pieceCid}>
                              Payload CIDs
                          </a>
                      </td>
                    ) : (
                      <td>{pieceCid}
                          &nbsp;
                          <a className="payload-icon" href={"/piece-doctor/piece-payload/"+pieceCid}>
                              Payload CIDs
                          </a>
                      </td>
                    )}
                </tr>
                <tr key="index status">
                    <th>Index Status</th>
                    <td>
                        <span>
                            {pieceStatus.IndexStatus.Status}
                            {indexFailed && pieceStatus.IndexStatus.Error ? ': ' + pieceStatus.IndexStatus.Error : '' }
                            <IndexStatusInfo />
                        </span>
                        <br/>
                        {canReIndex ? (
                            <div>
                                <div className="button build-index" title="Re-build index" onClick={buildIndex}>
                                    Re-index
                                </div>
                                {buildIndexResp.error ? <div>{buildIndexResp.error + ''}</div> : null}
                            </div>
                        ) : null}
                    </td>
                </tr>
                </tbody>
            </table>

            <h3>Local Index Directory</h3>
            {pieceStatus.PieceInfoDeals.length ? (
                <table className="deals">
                    <tbody>
                    <tr>
                        <th>Miner Address</th>
                        <th>Chain Deal ID</th>
                        <th>Sector Number</th>
                        <th>Piece Offset</th>
                        <th>Piece Length</th>
                        <th>Unsealed</th>
                    </tr>
                    {pieceStatus.PieceInfoDeals.map(deal => (
                        <tr key={deal.ChainDealID+''}>
                            <td>{deal.MinerAddress+''}</td>
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
                <p>No deals found in Local Index Directory for piece CID {pieceCid}</p>
            )}

            <h3>Deals</h3>
            {pieceStatus.Deals.length ? (
                <table className="deals">
                    <tbody>
                    <tr>
                        <th>CreatedAt</th>
                        <th>Deal ID</th>
                        <th>Legacy Deal</th>
                        <th>Direct Deal</th>
                        <th>Sector Number</th>
                        <th>Piece Offset</th>
                        <th>Piece Length</th>
                        <th>Unsealed</th>
                    </tr>
                    {pieceStatus.Deals.map(deal => (
                        <tr key={deal.Deal.ID}>
                            <td>{moment(deal.Deal.CreatedAt).format(dateFormat)}</td>
                            <td><ShortDealLink id={deal.Deal.ID} isLegacy={deal.Deal.IsLegacy} isDirect={deal.Deal.IsDirect}/></td>
                            <td>{deal.Deal.IsLegacy ? 'Yes' : 'No'}</td>
                            <td>{deal.Deal.IsDirect ? 'Yes' : 'No'}</td>
                            <td>{deal.Sector.ID + ''}</td>
                            <td>{deal.Sector.Offset + ''}</td>
                            <td>{deal.Sector.Length + ''}</td>
                            <td><SealStatus status={deal.SealStatus}/></td>
                        </tr>
                    ))}
                    </tbody>
                </table>
            ) : (
                <p>No deals found with piece CID {pieceCid} on this miner</p>
            )}
        </div>
    </div>
}

function SealStatus({status}) {
    if (status.Error) {
        return status.Error
    }
    switch (status.Status) {
        case 'HasUnsealedCopy': return 'Yes';
        case 'NoUnsealedCopy':  return 'No';
        default: return <>
            <span>Unknown</span>
            <Info>
                The sealing status of the sector is unknown.
                This could be because the sealing status caches are out of sync, or
                it could be because the sector is corrupted.
                Check the contents of the sector to ensure it is not corrupted.
            </Info>
        </>
    }
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

function IndexStatusInfo() {
    return <Info>
        <InfoListItem title="NotFound">
            There was no information found for this piece CID in the Local Index Directory.
        </InfoListItem>
        <InfoListItem title="Registered">
            The piece has been added to the Local Index Directory but has not yet been indexed.
        </InfoListItem>
        <InfoListItem title="Indexing">
            The piece is currently being indexed.
        </InfoListItem>
        <InfoListItem title="Complete">
            The piece has been indexed successfully.
        </InfoListItem>
        <InfoListItem title="Failed">
            There was an error indexing the piece.
        </InfoListItem>
    </Info>
}

const RowsPerPage = {
    Default: 10,

    settingsKey: "settings.flagged-pieces.per-page",

    load: () => {
        const saved = localStorage.getItem(RowsPerPage.settingsKey)
        return JSON.parse(saved) || RowsPerPage.Default
    },

    save: (val) => {
        localStorage.setItem(RowsPerPage.settingsKey, JSON.stringify(val));
    }
}

function scrollTop() {
    window.scrollTo({ top: 0, behavior: "smooth" })
}

export function PiecePayloadCids() {
    const params = useParams()
    const navigate = useNavigate()

    // Add a class to the document body when showing the ad detail page
    useEffect(() => {
        document.body.classList.add('modal-open')

        return function () {
            document.body.classList.remove('modal-open')
        }
    })

    const {loading, error, data} = useQuery(PiecePayloadCidsQuery, {
        variables: {pieceCid: params.pieceCID},
    })

    if (error) {
        return <div className="payloads modal" id={params.pieceCID}>
            <div className="content">
                <div className="close" onClick={() => navigate(-1)}>
                    <img className="icon" alt="" src={closeImg} />
                </div>
                <div className="title">
                    <span>Payloads CIDs for {params.pieceCID}</span>
                </div>
                <div><span>Error: {error.message}</span></div>
            </div>
        </div>
    }

    if (loading) {
        return <div>Loading ...</div>
    }

    var payload = data.piecePayloadCids
    return <div className="payloads modal" id={params.pieceCID}>
        <div className="content">
            <div className="close" onClick={() => navigate(-1)}>
                <img className="icon" alt="" src={closeImg} />
            </div>
            <div className="title">
                <span>Payloads CIDs for {params.pieceCID}</span>
            </div>
            <div>
                { payload.length === 0 ? (
                    <span>Piece {params.pieceCID} either has no payload CIDs or is not indexed</span>
                ) : (
                    <table>
                        <tbody>
                        <tr>
                            <th>Number</th>
                            <th>Payload CID</th>
                            <th>Multihash (Hexadecimal)</th>
                            <th>Download Link</th>
                        </tr>
                        {payload.map((payload, i) => {
                            return <tr key={payload.PayloadCid}>
                                <td>{i+1}.</td>
                                <td>
                                    {payload.PayloadCid}
                                </td>
                                <td>
                                    {payload.Multihash}
                                </td>
                                <td>
                                    <a className="download" target="_blank" href={downloadBlockHref(payload.PayloadCid)} rel="noreferrer">
                                        Download block
                                    </a>
                                </td>
                            </tr>
                        })}
                        </tbody>
                    </table>
                )}
            </div>
        </div>
    </div>
}
