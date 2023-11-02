/* global BigInt */
import {useQuery} from "@apollo/react-hooks";
import {
    IpniAdEntriesCountQuery,
    IpniAdEntriesQuery,
    IpniAdQuery,
    IpniProviderInfoQuery,
    IpniLatestAdQuery,
    RetrievalLogsCountQuery, RetrievalLogsListQuery, IpniDistanceFromLatestAdQuery,
} from "./gql";
import moment from "moment";
import React, {useEffect, useState} from "react";
import {ExpandableJSObject, PageContainer, ShortCIDTail, ShortPeerID} from "./Components";
import {Link, useNavigate, useParams} from "react-router-dom";
import {dateFormat, durationNanoToString} from "./util-date";
import {TimestampFormat} from "./timestamp";
import './Ipni.css'
import {Pagination} from "./Pagination";
import {humanFileSize} from "./util";
import {addClassFor} from "./util-ui";
import bezier2Img from "./bootstrap-icons/icons/bezier2.svg";
import {getDealStatus} from "./RetrievalLogDetail";
import closeImg from "./bootstrap-icons/icons/x-circle.svg";
import {getConfig} from "./config"

const basePath = '/ipni'

function indexerHost() {
    const cfg = getConfig()
    return cfg.Ipni.IndexerHost
}

export function IpniPage(props) {
    return <PageContainer pageType="ipni" title="Network Indexer">
        <div className="ipni">
            <ProviderInfo />
        </div>
    </PageContainer>
}

function ProviderInfo(props) {
    const {loading, error, data} = useQuery(IpniProviderInfoQuery)

    if (error) return <div>Error: {error.message + " - check connection to Boost server"}</div>
    if (loading) return <div>Loading...</div>
    if (!((data || {}).ipniProviderInfo || {}).Config) return null

    return <>
        <ProviderIpniInfo peerId={data.ipniProviderInfo.PeerID} />
        <ProviderConfig configJson={data.ipniProviderInfo.Config} />
    </>
}

function ProviderIpniInfo({peerId}) {
    const head = useQuery(IpniLatestAdQuery)
    const [{loading, error, data}, setResp] = useState({ loading: true })
    const idxHost = indexerHost()

    // When running boost on a local devnet, hard-code the peer ID to
    // sofiaminer's peer ID so we get real provider info back from the indexer
    if (process.env.NODE_ENV === 'development') {
        peerId = '12D3KooWE8yt84RVwW3sFcd6WMjbUdWrZer2YtT4dmtj3dHdahSZ'
    }
    useEffect(() => {
        fetch('https://'+idxHost+'/providers/'+peerId).then((res) => {
            if (res.status !== 200) {
                throw Error("Failed to fetch provider info for "+peerId+" with status code "+res.status)
            }
            return res.json()
        }).then((data) => {
            setResp({ loading: false, data })
        }).catch((err) => setResp({ loading: false, error: err }))
    }, [idxHost, peerId])

    if (error) return <div>Error: {"Fetching provider info from "+idxHost+": "+error.message}</div>
    if (loading) return <div>Loading...</div>
    if (!data) return null
    if (!head.data) return null

    return <>
        <ProviderIpniInfoRender data={data} idxHost={idxHost} lad={head.data.ipniLatestAdvertisement} />
    </>
}

function ProviderIpniInfoRender(props){
    const data = props.data
    const idxHost = props.idxHost
    const lad = props.lad
    let adCid = data.LastAdvertisement['/']
    if (process.env.NODE_ENV === 'development') {
        adCid = 'baguqeera4d4mgsbukpnlwu4bxuwir2pbchhdar4gmz3ti75cxuwhiviyowua'
    }
    const distance = useQuery(IpniDistanceFromLatestAdQuery, {
        variables: {
            latestAdcid: lad,
            adcid: adCid
        }
    })
    return <div className="ipni-prov-info">
        <h3>Provider Indexer Info</h3>
        <div className="subtitle">
            From <a href={"https://"+idxHost+"/providers/"+data.Publisher.ID}>{idxHost} provider record</a>
        </div>
        <table>
            <tbody>
            <tr>
                <th>Publisher Address</th>
                <td>{data.Publisher.Addrs}</td>
            </tr>
            <tr>
                <th>Publisher Peer ID</th>
                <td>{data.Publisher.ID}</td>
            </tr>
            <tr>
                <th>Last Advertisement Processed by {idxHost}</th>
                <td>
                    {data.LastAdvertisement['/']}
                    &nbsp;
                    <span className="aux">({moment(data.LastAdvertisementTime).fromNow()} ago)</span>
                    &nbsp;
                    {distance.data ? <span className="aux">({distance.data.ipniDistanceFromLatestAd} behind)</span>: ''}
                </td>
            </tr>
            <tr>
                <th>Latest Advertisement on Boost</th>
                <td>
                    {lad ? <Link to={'/ipni/ad/'+lad}>{lad}</Link>: ''}
                </td>
            </tr>
            <tr>
                <th>Last Ingestion</th>
                <td>
                    {data.LastError ? data.LastError : 'Success'}
                    &nbsp;
                    <span className="aux">({moment(data.LastErrorTime).fromNow()} ago)</span>
                </td>
            </tr>
            </tbody>
        </table>
    </div>
}

function ProviderConfig({configJson}) {
    const cfg = JSON.parse(configJson)
    return <div>
        <h3>Index Provider Config</h3>
        <ExpandableJSObject v={cfg} topLevel={false} expanded={true} key={'config'} />
    </div>
}

function RetrievalLogsContent(props) {
    const navigate = useNavigate()
    const params = useParams()
    const pageNum = (params.pageNum && parseInt(params.pageNum)) || 1

    const [timestampFormat, setTimestampFormat] = useState(TimestampFormat.load)
    const saveTimestampFormat = (val) => {
        TimestampFormat.save(val)
        setTimestampFormat(val)
    }

    var [rowsPerPage, setRowsPerPage] = useState(RowsPerPage.load)
    const onLogsPerPageChange = (e) => {
        const val = parseInt(e.target.value)
        RowsPerPage.save(val)
        setRowsPerPage(val)
        navigate(basePath)
        scrollTop()
    }

    // Fetch logs on this page
    const listOffset = (pageNum-1) * rowsPerPage
    var queryCursor = null
    if (pageNum > 1 && params.cursor) {
        try {
            queryCursor = BigInt(params.cursor)
        } catch {}
    }
    const {loading, error, data} = useQuery(RetrievalLogsListQuery, {
        pollInterval: 10000,
        variables: {
            cursor: queryCursor,
            offset: listOffset,
            limit: rowsPerPage,
            isIndexer: true,
        },
        fetchPolicy: 'network-only',
    })

    if (error) return <div>Error: {error.message + " - check connection to Boost server"}</div>
    if (loading) return <div>Loading...</div>

    var res = data.retrievalLogs
    var logs = res.logs
    if (pageNum === 1) {
        logs.sort((a, b) => Number(b.RowID - a.RowID))
        logs = logs.slice(0, rowsPerPage)
    }
    const totalCount = res.totalCount

    var cursor = params.cursor
    if (pageNum === 1 && logs.length) {
        cursor = Number(logs[0].RowID)
    }

    var toggleTimestampFormat = () => saveTimestampFormat(!timestampFormat)

    const paginationParams = {
        basePath, cursor, pageNum, totalCount, rowsPerPage,
        moreRows: res.more,
        onRowsPerPageChange: onLogsPerPageChange,
        onLinkClick: scrollTop,
    }

    return <div className="ipni-retrievals" id="ipni-retrievals">
        <div className="popup">
            <div className="message"></div>
        </div>
        <h3>Indexer Retrieval Logs</h3>
        <table>
            <tbody>
            <tr>
                <th onClick={toggleTimestampFormat} className="start">Start</th>
                <th>Indexer Peer ID</th>
                <th>Transfer ID</th>
                <th>Ad CID</th>
                <th>Sent</th>
                <th>Status</th>
                <th>Message</th>
            </tr>

            {logs.map(row => (
                <TableRow
                    key={row.RowID}
                    row={row}
                    timestampFormat={timestampFormat}
                    toggleTimestampFormat={toggleTimestampFormat}
                />
            ))}
            </tbody>
        </table>

        <Pagination {...paginationParams} />
    </div>
}

var popupTimeout
function showPopup(msg) {
    clearTimeout(popupTimeout)
    const el = document.body.querySelector('.retrieval-logs .popup')
    popupTimeout = addClassFor(el, 'showing', 2000)
    const msgEl = document.body.querySelector('.retrieval-logs .popup .message')
    msgEl.textContent = msg
}

function TableRow(props) {
    var row = props.row
    var start = moment(row.CreatedAt).format(dateFormat)
    if (props.timestampFormat !== TimestampFormat.DateTime) {
        start = '1m'
        if (new Date().getTime() - row.CreatedAt.getTime() > 60 * 1000) {
            start = moment(row.CreatedAt).fromNow()
        }
    }

    function fieldToClipboard(fieldValue, elId) {
        navigator.clipboard.writeText(fieldValue)
        const el = document.getElementById(elId)
        addClassFor(el, 'copied', 500)
        showPopup("Copied " + fieldValue + " to clipboard")
    }

    const copyPeerId = "copy-"+row.CreatedAt+row.PeerID
    const peerIDToClipboard = () => fieldToClipboard(row.PeerID, copyPeerId)
    const copyTransferId = "copy-"+row.CreatedAt+row.TransferID
    const transferIDToClipboard = () => fieldToClipboard(row.TransferID, copyTransferId)

    var status = getDealStatus(row.DTStatus)
    if (row.DTStatus !== status && row.DTStatus !== '') {
        status += ": " + row.DTStatus
    }
    var msg = row.Message
    if (row.DTMessage !== '') {
        if (msg !== '') {
            msg += ' - '
        }
        msg += row.DTMessage
    }
    return (
        <tr>
            <td className="start" onClick={props.toggleTimestampFormat}>
                {start}
            </td>
            <td className="peer-id">
                <span id={copyPeerId} className="copy" onClick={peerIDToClipboard} title="Copy peer ID to clipboard"></span>
                <ShortPeerID peerId={row.PeerID} />
            </td>
            <td className="transfer-id">
                <span id={copyTransferId} className="copy" onClick={transferIDToClipboard} title="Copy transfer ID to clipboard"></span>
                <Link to={'/retrieval-logs/'+row.PeerID+'/'+row.TransferID}>
                    {'…'+(row.TransferID+'').slice(-8)}
                </Link>
            </td>
            <td className="ad-cid">
                <Link to={'/ipni/ad/'+row.PayloadCID}>
                    <ShortCIDTail cid={row.PayloadCID} />
                </Link>
            </td>
            <td className="sent">
                {humanFileSize(row.TotalSent)}
            </td>
            <td className="status">
                {status}
            </td>
            <td className="message">
                {msg}
            </td>
        </tr>
    )
}

export function IpniAdDetail() {
    const params = useParams()
    const navigate = useNavigate()

    // Add a class to the document body when showing the ad detail page
    useEffect(() => {
        document.body.classList.add('modal-open')

        return function () {
            document.body.classList.remove('modal-open')
        }
    })

    return <div className="ad-detail modal" id={params.adCid}>
        <div className="content">
            <div className="close" onClick={() => navigate(-1)}>
                <img className="icon" alt="" src={closeImg} />
            </div>
            <div className="title">
                <span>Advertisement {'…'+params.adCid.slice(-8)}</span>
            </div>
            <IpniAdDetailFields adCid={params.adCid} />
        </div>
    </div>
}

function IpniAdDetailFields({adCid}) {
    const {loading, error, data} = useQuery(IpniAdQuery, {
        variables: {adCid},
    })

    if (error) {
        return <div>Error: {error.message}</div>
    }

    if (loading) {
        return <div>Loading ...</div>
    }

    var ad = data.ipniAdvertisement

    const idxHost = indexerHost()

    return <div>
        <table className="ad-fields">
        <tbody>
        <tr>
            <th>Advertisement Cid</th>
            <td>{adCid}</td>
        </tr>
        <tr>
            <th>Context ID</th>
            <td>{ad.ContextID}</td>
        </tr>
        <tr>
            <th>Advertisement Type</th>
            <td>{ad.IsRemove ? 'Remove Entries' : 'Add Entries'}</td>
        </tr>
        <tr>
            <th>Metadata</th>
            <td><IpniAdMetadata metadata={ad.Metadata} /></td>
        </tr>
        <tr>
            <th>Entries</th>
            <td><IpniAdDetailEntryCount adCid={adCid} /></td>
        </tr>
        <tr>
            <th>Previous Advertisement</th>
            <td><Link to={'/ipni/ad/'+ad.PreviousEntry}>{ad.PreviousEntry}</Link></td>
        </tr>
        <tr>
            <th>Provider</th>
            <td><a href={'https://'+idxHost+'/providers/'+ad.Provider}>{ad.Provider}</a></td>
        </tr>
        <tr>
            <th>Addresses</th>
            <td>{ad.Addresses.map(addr => <div key={addr}>{addr}</div>)}</td>
        </tr>
        </tbody>
        </table>

        <IpniExtendedProviders ext={ad.ExtendedProviders} />
    </div>
}

function IpniExtendedProviders({ext}) {
    if (!ext || !ext.Providers || !ext.Providers.length) {
        return null
    }

    return <div>
        <h3>Extended Providers</h3>
        <table>
        <tbody>
            <tr>
                <td>Override</td>
                <td>{ext.Override ? 'Yes' : 'No'}</td>
            </tr>
            {(ext.Providers || []).map(prov => {
                return <>
                    <tr key={prov.ID}>
                        <td>Provider ID</td>
                        <td>{prov.ID}</td>
                    </tr>
                    <tr key={prov.ID+'-addr'}>
                        <td>Addresses</td>
                        <td>{prov.Addresses.map(addr => <>{addr}<br/></>)}</td>
                    </tr>
                    <tr key={prov.ID+'-meta'}>
                        <td>Metadata</td>
                        <td><IpniAdMetadata metadata={prov.Metadata} /></td>
                    </tr>
                </>
            })}
        </tbody>
        </table>
    </div>
}

function IpniAdMetadata({metadata}) {
    if (!metadata || !metadata.length) {
        return null
    }

    return metadata.map(md => {
        var protoMetadata = md.Metadata
        if (md.Metadata) {
            try {
                const obj = JSON.parse(md.Metadata)
                protoMetadata = Object.entries(obj).map(([k, v]) => {
                    if (k === 'PieceCID') {
                        v = <Link to={'/piece-doctor/piece/'+v}>{v}</Link>
                    } else {
                        v = v+''
                    }
                    return <div className="metadata-field">
                        <span className="metadata-field-name">{k}</span>:&nbsp;
                        <span className="metadata-field-value">{v}</span>
                    </div>
                })
            } catch (e) {}
        }

        return <div key={md.Protocol} className="metadata">
            <div className="metadata-protocol">{md.Protocol}</div>
            {protoMetadata ? <div className="metadata-content">{protoMetadata}</div> : null}
        </div>
    })
}

function IpniAdDetailEntryCount({adCid}) {
    const {loading, error, data} = useQuery(IpniAdEntriesCountQuery, {
        variables: {adCid},
    })

    if (error) {
        return <div>Error: {error.message}</div>
    }

    if (loading) {
        return <div>Loading ...</div>
    }

    var count = data.ipniAdvertisementEntriesCount
    return <Link to={"/ipni/ad/"+adCid+"/entries"}>{count}</Link>
}

export function IpniAdEntries() {
    const params = useParams()
    const navigate = useNavigate()

    // Add a class to the document body when showing the ad detail page
    useEffect(() => {
        document.body.classList.add('modal-open')

        return function () {
            document.body.classList.remove('modal-open')
        }
    })

    const {loading, error, data} = useQuery(IpniAdEntriesQuery, {
        variables: {adCid: params.adCid},
    })

    if (error) {
        return <div className="ad-detail modal" id={params.adCid}>
            <div className="content">
                <div className="close" onClick={() => navigate(-1)}>
                    <img className="icon" alt="" src={closeImg} />
                </div>
                <div className="title">
                    <span>Advertisement {'…'+params.adCid.slice(-8)} Entries</span>
                </div>
                <div><span>Error: {error.message}</span></div>
            </div>
        </div>
    }

    if (loading) {
        return <div>Loading ...</div>
    }

    const idxHost = indexerHost()
    var entries = data.ipniAdvertisementEntries
    return <div className="ad-detail modal" id={params.adCid}>
        <div className="content">
            <div className="close" onClick={() => navigate(-1)}>
                <img className="icon" alt="" src={closeImg} />
            </div>
            <div className="title">
                <span>Advertisement {'…'+params.adCid.slice(-8)} Entries</span>
            </div>
            <div className="entries">
                { entries.length === 0 ? (
                    <span>Advertisement {params.adCid} has no entries</span>
                ) : (
                    <table>
                    <tbody>
                        {entries.map((entry, i) => {
                            return <tr>
                                <td>{i+1}.</td>
                                <td>
                                    <a href={"https://"+idxHost+"/multihash/"+entry}>{entry}</a>
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

export function IpniMenuItem(props) {
    const {data} = useQuery(RetrievalLogsCountQuery, {
        pollInterval: 10000,
        fetchPolicy: 'network-only',
        variables: {
            isIndexer: true,
        }
    })

    var durationDisplay = ''
    var count = 0
    if (data && data.retrievalLogsCount) {
        const plc = data.retrievalLogsCount
        count = plc.Count
        durationDisplay = durationNanoToString(plc.Period)
    }

    return (
        <div className="menu-item" >
            <img className="icon" alt="" src={bezier2Img} />
            <Link key="ipni" to={basePath}>
                <h3>Network Indexer</h3>
            </Link>
        </div>
    )
}

function scrollTop() {
    var top = 0
    const el = document.getElementById('ipni-retrievals')
    if (el) {
        top = el.offsetTop
    }

    window.scrollTo({ top, behavior: "smooth" })
}

const RowsPerPage = {
    Default: 10,

    settingsKey: "settings.network-indexer-logs.per-page",

    load: () => {
        const saved = localStorage.getItem(RowsPerPage.settingsKey)
        return JSON.parse(saved) || RowsPerPage.Default
    },

    save: (val) => {
        localStorage.setItem(RowsPerPage.settingsKey, JSON.stringify(val));
    }
}

