import {useMutation, useQuery} from "./hooks";
import {FundsQuery, FundsLogsQuery, FundsMoveToEscrow} from "./gql";
import {BarChart} from "./Chart";
import {useState, React}  from "react";
import moment from "moment";
import {humanFIL} from "./util"

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

    function fundsAmount(tp) {
        return (data.funds.find(f => f.Name === tp) || {}).Amount || 0
    }

    var barChart = [{
        Name: 'Available collateral',
        Capacity: fundsAmount('escrow-available'),
    }, {
        Name: 'Tagged collateral',
        Capacity: fundsAmount('escrow-tagged'),
    }, {
        Name: 'Available for Publish message',
        Capacity: fundsAmount('publish-message-balance'),
    }, {
        Name: 'Tagged for Publish message',
        Capacity: fundsAmount('publish-message-tagged'),
    }]

    return <>
        <BarChart fields={barChart} unit="attofil" />

        <div className="collateral">
            <table>
                <tbody>
                <tr>
                    <td>Locked Collateral</td>
                    <td>{humanFIL(fundsAmount('escrow-locked'))}</td>
                </tr>
                <tr>
                    <td>Pledge Collateral</td>
                    <td>{humanFIL(fundsAmount('collateral-balance'))}</td>
                </tr>
                </tbody>
            </table>

            <TopupCollateral maxTopup={fundsAmount('collateral-balance')} />
        </div>
    </>
}

function TopupCollateral(props) {
    const [showForm, setShowForm] = useState(false)
    const [topupAmount, setTopupAmount] = useState('')
    const handleTopupChange = event => setTopupAmount(event.target.value)

    const [fundsMoveToEscrow] = useMutation(FundsMoveToEscrow, {
        variables: {amount: parseFloat(topupAmount)}
    })

    function topUpAvailable() {
        fundsMoveToEscrow()
        setTopupAmount('')
        setShowForm(false)
    }

    return (
        <div className="top-up">
            { showForm ? (
                <div className={"top-up-form"}>
                    <input
                        type="number"
                        max={props.maxTopup}
                        value={topupAmount}
                        onChange={handleTopupChange}
                    />
                    attoFIL
                    <div className="buttons">
                        <div className="button" onClick={() => topUpAvailable()}>Top up</div>
                        <div className="button cancel" onClick={() => setShowForm(false)}>Cancel</div>
                    </div>
                </div>
            ) : (
                <div className="button" onClick={() => setShowForm(true)}>Top up available collateral</div>
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

    return <table className="funds-logs">
        <tbody>
            <tr>
                <th></th>
                <th>Deal ID</th>
                <th>Amount</th>
                <th>Description</th>
            </tr>
            {logs.map(l => <FundsLog key={l.CreatedAt} log={l} />)}
        </tbody>
    </table>
}

function FundsLog(props) {
    return <tr>
        <td>{moment(props.log.CreatedAt).fromNow()}</td>
        <td>{props.log.DealID}</td>
        <td>{humanFIL(props.log.Amount)}</td>
        <td>{props.log.Text}</td>
    </tr>
}
