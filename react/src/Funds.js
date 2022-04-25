/* global BigInt */
import {useMutation, useQuery} from "@apollo/react-hooks";
import {FundsQuery, FundsLogsQuery, FundsMoveToEscrow} from "./gql";
import {useState, useEffect, React}  from "react";
import moment from "moment";
import {humanFIL, max, parseFil} from "./util"
import {Info} from "./Info"
import {PageContainer, ShortDealLink} from "./Components";
import {NavLink, useParams} from "react-router-dom";
import coinImg from './bootstrap-icons/icons/coin.svg'
import {CumulativeBarChart, CumulativeBarLabels} from "./CumulativeBarChart";
import './Funds.css'
import {ShowBanner} from "./Banner";
import {Pagination} from "./Pagination";

export function FundsPage(props) {
    return (
        <PageContainer pageType="funds" title="Funds">
            <FundsChart />
            <FundsLogs />
        </PageContainer>
    )
}

function FundsChart(props) {
    const {loading, error, data} = useQuery(FundsQuery, { pollInterval: 1000 })

    if (loading) {
        return <div>Loading...</div>
    }
    if (error) {
        return <div>Error: {error.message}</div>
    }

    const funds = data.funds
    const collatBalance = funds.Collateral.Balance

    const total = {
        collatBalance: funds.Collateral.Balance,
        escrow: funds.Escrow.Tagged + funds.Escrow.Available + funds.Escrow.Locked,
        pubMsg: funds.PubMsg.Balance,
    }
    const amtMax = max(total.collatBalance, total.escrow, total.pubMsg)

    return <div className="chart">
        <div className="amounts">
            <CollateralSource collateral={funds.Collateral} amtMax={amtMax} />
            <FundsEscrow escrow={funds.Escrow} amtMax={amtMax} />
            <PubMsgWallet pubMsg={funds.PubMsg} address={funds.PubMsg.Address} amtMax={amtMax} />
        </div>

        <TopupCollateral maxTopup={collatBalance} />
    </div>
}

function CollateralSource(props) {
    const barPct = props.amtMax ? toPercentage(props.collateral.Balance, props.amtMax) : 0

    const bars = [{
        className: 'balance',
        amount: props.collateral.Balance,
    }]

    return <div className="collateral-source">
        <div className="title">
            Deal Collateral Source Wallet
            <Info>
                The Storage Provider must have sufficient collateral in escrow for each
                storage deal.<br/>
                <br/>
                When the deal is published, the network checks whether there is enough
                collateral in escrow.<br/>
                <br/>
                The Collateral Source Wallet is the wallet from which funds
                are moved to escrow.
            </Info>
        </div>
        <WalletAddress address={props.collateral.Address} />
        <div className="bar-limit" style={{width: barPct + '%'}}>
            <CumulativeBarChart bars={bars} unit="attoFIL" />
        </div>
    </div>

}

function FundsEscrow(props) {
    const escrow = props.escrow
    const bars = [{
        name: 'Tagged',
        className: 'tagged',
        amount: escrow.Tagged,
    }, {
        name: 'Available',
        className: 'available',
        amount: escrow.Available,
    }, {
        name: 'Locked',
        className: 'locked',
        amount: escrow.Locked,
    }]

    var total = 0n
    for (const bar of bars) {
        total += bar.amount
    }
    const barPct = props.amtMax ? toPercentage(total, props.amtMax) : 0

    return <div className="escrow">
        <div className="title">
            Deal Collateral in Escrow
            <Info>
                The Storage Provider must have sufficient collateral in escrow for each
                storage deal.<br/>
                <br/>
                When a deal is accepted, the collateral for the deal is "tagged". Those
                funds cannot be used as collateral for another deal.<br/>
                <br/>
                When a deal is published, there must be enough funds in escrow to cover
                the collateral for the deal. On publish, the tagged funds are moved on
                chain from "Available" to "Locked" until the deal is complete.<br/>
            </Info>
        </div>

        <div className="bar-content">
            <div className="bar-limit" style={{width: barPct + '%'}}>
                <CumulativeBarChart bars={bars} unit="attoFIL" />
            </div>
            <CumulativeBarLabels bars={bars} unit="attoFIL" />
        </div>
    </div>
}

function PubMsgWallet(props) {
    const pubMsg = props.pubMsg

    const bars = [{
        name: 'Tagged',
        className: 'tagged',
        amount: pubMsg.Tagged,
    }, {
        name: 'Available',
        className: 'available',
        amount: pubMsg.Balance - pubMsg.Tagged,
    }]
    const barPct = props.amtMax ? toPercentage(pubMsg.Balance, props.amtMax) : 0

    return <div className="pubmsg-wallet">
        <div className="title">
            Publish Storage Deals Wallet
            <Info>
                The Publish Storage Deals Wallet is used to pay the gas cost
                for sending the Publish Storage Deals message on chain.
            </Info>
        </div>
        <WalletAddress address={props.address} />
        <div className="bar-content">
            <div className="bar-limit" style={{width: barPct + '%'}}>
                <CumulativeBarChart bars={bars} unit="attoFIL" />
            </div>
            <CumulativeBarLabels bars={bars} unit="attoFIL" />
        </div>
    </div>

}

function toPercentage(num, denom) {
    return Math.floor(Number(1000n * BigInt(num) / BigInt(denom)) / 10)
}

function WalletAddress(props) {
    const shortAddr = props.address.substring(0, 8)+'…'+props.address.substring(props.address.length-8)
    return <div className="wallet-address">
        <a href={"https://filfox.info/en/address/"+props.address} target="_blank" rel="noreferrer">
            {shortAddr}
        </a>
    </div>
}

function TopupCollateral(props) {
    const [showForm, setShowForm] = useState(false)
    const [topupAmount, setTopupAmount] = useState('')
    const handleTopupChange = event => {
        setTopupAmount(event.target.value)
    }

    const [fundsMoveToEscrow] = useMutation(FundsMoveToEscrow, {
        variables: {amount: parseFil(topupAmount)}
    })

    useEffect(() => {
        const el = document.getElementById('topup-amount')
        el && el.focus()
    })

    async function topUpAvailable() {
        const amt = topupAmount
        const res = fundsMoveToEscrow()
        setTopupAmount('')
        setShowForm(false)
        try {
            await res
            ShowBanner('Moved ' + amt + ' FIL to escrow (page will update after 1 epoch)')
        } catch(e) {
            console.log(e)
            ShowBanner(e.message, true)
        }
    }

    function handleCancel() {
        setTopupAmount('')
        setShowForm(false)
    }

    return (
        <div className="top-up">
            { showForm ? (
                <div className="top-up-form">
                    <input
                        id="topup-amount"
                        type="number"
                        value={topupAmount}
                        onChange={handleTopupChange}
                    />
                    FIL
                    <div className="buttons">
                        <div className="button" onClick={() => topUpAvailable()}>Move</div>
                        <div className="button cancel" onClick={handleCancel}>Cancel</div>
                    </div>
                </div>
            ) : (
                <div className="button" onClick={() => setShowForm(true)}>Move collateral to escrow</div>
            )}
        </div>
    )
}

function FundsLogs(props) {
    const params = useParams()
    const pageNum = params.pageNum ? parseInt(params.pageNum) : 1
    const rowsPerPage = 10
    const dealListOffset = (pageNum-1) * rowsPerPage

    var queryCursor = null
    if (pageNum > 1 && params.cursor) {
        try {
            queryCursor = BigInt(params.cursor)
        } catch {}
    }
    const {loading, error, data} = useQuery(FundsLogsQuery, {
        pollInterval: pageNum === 1 ? 5000 : undefined,
        variables: {
            cursor: queryCursor,
            limit: rowsPerPage,
            offset: dealListOffset,
        }
    })

    if (loading) {
        return <div>Loading...</div>
    }
    if (error) {
        return <div>Error: {error.message}</div>
    }

    var logs = data.fundsLogs.logs
    if (logs.length === 0) {
        return null
    }

    const totalCount = data.fundsLogs.totalCount

    var cursor = params.cursor ? parseInt(params.cursor) : undefined
    if (pageNum === 1 && logs.length) {
        cursor = logs[0].CreatedAt.getTime()
    }

    const paginationParams = {
        basePath: '/funds',
        moreRows: data.fundsLogs.more,
        cursor, pageNum, totalCount, rowsPerPage
    }

    return <div className="funds-logs-section">
        <h3>Funds logs</h3>
        <table className="funds-logs">
            <tbody>
                <tr>
                    <th></th>
                    <th>Deal ID</th>
                    <th>Amount</th>
                    <th>Description</th>
                </tr>
                {logs.map((l, i) => <FundsLog key={i} log={l} />)}
            </tbody>
        </table>

        <Pagination {...paginationParams} />
    </div>
}

function FundsLog(props) {
    return <tr>
        <td>{moment(props.log.CreatedAt).fromNow()}</td>
        <td><ShortDealLink id={props.log.DealUUID} /></td>
        <td>{humanFIL(props.log.Amount)}</td>
        <td>{props.log.Text}</td>
    </tr>
}

export function FundsMenuItem(props) {
    const {data} = useQuery(FundsQuery, {
        pollInterval: 5000,
        fetchPolicy: "network-only",
    })

    const escrow = {
        used: 0n,
        free: 0n,
        total: 0n,
    }
    const pubMsg = {
        used: 0n,
        free: 0n,
        total: 0n,
    }

    if (data) {
        const funds = data.funds
        escrow.free = funds.Escrow.Available
        escrow.used = funds.Escrow.Tagged + funds.Escrow.Locked
        escrow.total = escrow.used + escrow.free

        pubMsg.total = funds.PubMsg.Balance
        pubMsg.used = funds.PubMsg.Tagged
        escrow.free = escrow.total - escrow.used
    }

    escrow.bars = [{
        className: 'used',
        amount: escrow.used,
    }, {
        className: 'free',
        amount: escrow.free,
    }]

    pubMsg.bars = [{
        className: 'used',
        amount: pubMsg.used,
    }, {
        className: 'free',
        amount: pubMsg.free,
    }]

    return <NavLink key="funds" className="sidebar-item sidebar-item-funds" to="/funds">
        <span className="sidebar-icon">
            <svg width="25" height="24" viewBox="0 0 25 24" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M24 15.13h-4.865a3.236 3.236 0 0 1-3.235-3.235 3.236 3.236 0 0 1 3.235-3.234H24M19.685 11.821h-.374" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/><path clip-rule="evenodd" d="M7.306 1.435h10.388A6.306 6.306 0 0 1 24 7.74v8.625a6.306 6.306 0 0 1-6.306 6.306H7.306A6.306 6.306 0 0 1 1 16.366V7.741a6.306 6.306 0 0 1 6.306-6.306z" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/><path d="M6.45 6.889h6.489" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>
        </span>
        <span className="sidebar-title">Funds</span>
        <div className="sidebar-item-excerpt">
            <div className="row">
                <div className="col-sm-6">
                    <span className="sub-label">Publish Message</span>
                    <div className="progress">
                        <span className="bar" style={{width:"0%"}}></span>
                        {/*<CumulativeBarChart bars={pubMsg.bars} unit="byte" compact={true} />*/}
                    </div>
                    <div className="explanation">
                        <span className="numerator">{humanFIL(pubMsg.used)}</span>
                        <span> of </span>
                        <span className="denominator">{humanFIL(pubMsg.total)}</span>
                        <span> used</span>
                    </div>
                </div>
                <div className="col-sm-6">
                    <span className="sub-label sub-label-escrow">Escrow</span>
                    <div className="progress">
                        <span className="bar" style={{width:"5%"}}></span>
                        {/*<CumulativeBarChart bars={escrow.bars} unit="byte" compact={true} />*/}
                    </div>
                    <div className="explanation">
                        <span className="numerator">{humanFIL(escrow.used)}</span>
                        <span> of </span>
                        <span className="denominator">{humanFIL(escrow.total)}</span>
                        <span> used</span>
                    </div>
                </div>
            </div>
        </div>
    </NavLink>
}