import {useMutation, useQuery} from "./hooks";
import {FundsQuery, FundsLogsQuery, FundsMoveToEscrow} from "./gql";
import {useState, React}  from "react";
import moment from "moment";
import {humanFIL} from "./util"
import {Info} from "./Info"

export function FundsPage(props) {
    return (
        <>
            <FundsChart />
            <FundsLogs />
        </>
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
    const escrow = {
        tagged: funds.Escrow.Tagged,
        available: funds.Escrow.Available,
        locked: funds.Escrow.Locked,
    }
    escrow.total = escrow.tagged + escrow.available + escrow.locked

    const pubMsg = {
        tagged: funds.PubMsg.Tagged,
        total: funds.PubMsg.Balance,
    }
    pubMsg.available = pubMsg.total - pubMsg.tagged

    const amtMax = Math.max(collatBalance, escrow.total, pubMsg.total)
    const collatBarPct = 100 * collatBalance / amtMax

    const escrowBar = {
        tagged: Math.floor(100 * escrow.tagged / amtMax)-1,
        available: Math.floor(100 * escrow.available / amtMax)-1,
        locked: Math.floor(100 * escrow.locked / amtMax),
        unit: Math.floor(100*escrow.total / amtMax)
    }

    const pubMsgBar = {
        tagged: Math.floor(100 * pubMsg.tagged / pubMsg.total)-1,
        available: Math.floor(100 * pubMsg.available / pubMsg.total)-2,
        unit: Math.floor(100 * pubMsg.total / amtMax),
    }

    const barSpaceRatio = 0.6

    return <div className="chart">
        <div className="amounts">
            <div className="collateral-source">
                <div className="title">
                    Collateral Source Wallet
                    <Info>
                        The Collateral Source Wallet is the wallet from which funds
                        are moved to escrow.
                    </Info>
                </div>
                <WalletAddress address={funds.Collateral.Address} />
                <div className="bar-content">
                    <div className="bar" style={{ width: barSpaceRatio*collatBarPct+'%'}}>
                        <div className="collat"></div>
                    </div>
                    <div className="bar-total">{humanFIL(collatBalance)}</div>
                </div>
            </div>

            <div className="escrow">
                <div className="title">
                    Collateral in Escrow
                    <Info>
                        Collateral in Escrow is the funds that are kept in escrow on chain.<br/>
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
                    <div className="bar" style={{ width: barSpaceRatio*escrowBar.unit+'%'}}>
                        <div className="tagged" style={{width: escrowBar.tagged + '%'}} />
                        { escrow.available ? (
                            <div className="available" style={{width: escrowBar.available + '%'}} />
                        ) : null }
                        { escrow.locked ? (
                            <div className="locked" style={{width: escrowBar.locked + '%'}} />
                        ) : null }
                    </div>

                    <div className="bar-total">{humanFIL(escrow.total)}</div>

                    <div className="labels">
                        <div className="label tagged">
                            <div className="bar-color"></div>
                            <div className="text">Tagged</div>
                            <div className="amount">{humanFIL(escrow.tagged)}</div>
                        </div>
                        <div className="label available">
                            <div className="bar-color"></div>
                            <div className="text">Available</div>
                            <div className="amount">{humanFIL(escrow.available)}</div>
                        </div>
                        { escrow.locked ? (
                            <div className="label locked">
                                <div className="bar-color"></div>
                                <div className="text">Locked</div>
                                <div className="amount">{humanFIL(escrow.locked)}</div>
                            </div>
                        ) : null }
                    </div>
                </div>
            </div>

            <div className="pubmsg-wallet">
                <div className="title">
                    Publish Storage Deals Wallet
                    <Info>
                        The Publish Storage Deals Wallet is used to pay the gas cost
                        for sending the Publish Storage Deals message on chain.
                    </Info>
                </div>
                <WalletAddress address={funds.PubMsg.Address} />
                <div className="bar-content">
                    <div className="bar" style={{ width: barSpaceRatio*pubMsgBar.unit+'%'}} />
                    <div className="bar-total">{humanFIL(pubMsg.total)}</div>

                    <div className="labels">
                        <div className="label tagged">
                            <div className="bar-color"></div>
                            <div className="text">Tagged</div>
                            <div className="amount">{humanFIL(pubMsg.tagged)}</div>
                        </div>
                        { pubMsg.available ? (
                            <div className="label available">
                                <div className="bar-color"></div>
                                <div className="text">Available</div>
                                <div className="amount">{humanFIL(pubMsg.available)}</div>
                            </div>
                        ) : null }
                    </div>
                </div>
            </div>
        </div>

        <TopupCollateral maxTopup={collatBalance} />
    </div>
}

function WalletAddress(props) {
    const shortAddr = props.address.substring(0, 8)+'…'+props.address.substring(props.address.length-8)
    return <div className="wallet-address">
        <a href={"https://filfox.info/en/address/"+props.address}>
            {shortAddr}
        </a>
    </div>
}

function TopupCollateral(props) {
    const [showForm, setShowForm] = useState(false)
    const [topupAmount, setTopupAmount] = useState('')
    const handleTopupChange = event => setTopupAmount(event.target.value)

    const [fundsMoveToEscrow] = useMutation(FundsMoveToEscrow, {
        variables: {amount: parseFloat(topupAmount*1e18)}
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
                        max={props.maxTopup*1e-18}
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
        <td>{props.log.DealUUID}</td>
        <td>{humanFIL(props.log.Amount)}</td>
        <td>{props.log.Text}</td>
    </tr>
}
