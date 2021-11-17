import {useQuery} from "./hooks";
import {FundsQuery, FundsLogsQuery} from "./gql";
import {BarChart} from "./Chart";
import React from "react";
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
    const {loading, error, data} = useQuery(FundsQuery)

    if (loading) {
        return <div>Loading...</div>
    }
    if (error) {
        return <div>Error: {error.message}</div>
    }

    return <BarChart fields={data.funds} />
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
