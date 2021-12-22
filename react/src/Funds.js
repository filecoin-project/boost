/* global BigInt */
import {useMutation, useQuery} from "@apollo/react-hooks";
import {FundsQuery, FundsLogsQuery, FundsMoveToEscrow} from "./gql";
import {useState, React}  from "react";
import moment from "moment";
import {humanFIL, humanFileSize, max, parseFil, toFixed} from "./util"
import {Info} from "./Info"
import {PageContainer, ShortDealLink} from "./Components";
import {Link} from "react-router-dom";
import coinImg from './bootstrap-icons/icons/coin.svg'
import {CumulativeBarChart, CumulativeBarLabels} from "./CumulativeBarChart";
import './Funds.css'

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
    const barPct = toPercentage(props.collateral.Balance, props.amtMax)

    const bars = [{
        className: 'balance',
        amount: props.collateral.Balance,
    }]

    return <div className="collateral-source">
        <div className="title">
            Collateral Source Wallet
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
    const barPct = toPercentage(total, props.amtMax)

    return <div className="escrow">
        <div className="title">
            Collateral in Escrow
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
    const barPct = toPercentage(pubMsg.Balance, props.amtMax)

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

    function topUpAvailable() {
        fundsMoveToEscrow()
        setTopupAmount('')
        setShowForm(false)
    }

    function handleCancel() {
        setTopupAmount('')
        setShowForm(false)
    }

    return (
        <div className="top-up">
            { showForm ? (
                <div className={"top-up-form"}>
                    <input
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
    const {loading, error, data} = useQuery(FundsLogsQuery)

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

    return <table className="funds-logs">
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

    return <Link key="funds" className="funds menu-item" to="/funds">
        <img className="icon" alt="" src={coinImg} />
        <h3>Funds</h3>

        <div className="menu-desc">
            <div className="title">Escrow</div>
            <CumulativeBarChart bars={escrow.bars} unit="byte" compact={true} />
            <b>{humanFIL(escrow.used)}</b> of <b>{humanFIL(escrow.total)}</b> used
        </div>

        <div className="menu-desc">
            <div className="title">Publish Message</div>
            <CumulativeBarChart bars={pubMsg.bars} unit="byte" compact={true} />
            <b>{humanFIL(pubMsg.used)}</b> of <b>{humanFIL(pubMsg.total)}</b> used
        </div>
    </Link>
}