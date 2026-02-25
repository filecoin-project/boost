import {useQuery} from "@apollo/client";
import {MpoolQuery} from "./gql";
import {React, useState} from "react";
import {humanFIL} from "./util";
import './Mpool.css'
import {PageContainer} from "./Components";
import {EpochQuery} from "./gql";
import moment from "moment";

export function MpoolPage(props) {
    return <PageContainer pageType="mpool" title="Message Pool">
        <MpoolContent />
    </PageContainer>
}

function MpoolContent(props) {
    const [alerts, setAlerts] = useState(true)
    const {loading, error, data} = useQuery(MpoolQuery, { variables: { alerts } , pollInterval: 10000, fetchPolicy: "network-only"})

    if (loading) {
        return <div>Loading...</div>
    }
    if (error) {
        return <div>Error: {error.message}</div>
    }

    const msgs = data.mpool.Messages

    return <div className="mpool">
        <div className="header">
            Showing {msgs.length} {alerts ? 'alerts' : 'messages'} in message pool.
            <div className="button" onClick={() => setAlerts(!alerts)}>
                Show {alerts ? 'All local messages' : 'Alerts'}
            </div>
        </div>

        <table>
            <tbody>
                {msgs.map((msg, i) => <MpoolMessage msg={msg} key={i} />)}
            </tbody>
        </table>
    </div>
}

function MpoolMessage(props) {
    const i = props.i
    const msg = props.msg

    const {data} = useQuery(EpochQuery)

    const handleParamsClick = (el) => {
        el.target.classList.toggle('expanded')
    }

    let elapsed = (data.epoch.Epoch+'') - (msg.SentEpoch+'')
    let x = moment().add(-(elapsed)*(data.epoch.SecondsPerEpoch+''), 'seconds').fromNow()

    return <>
        <tr key={"sentepoch"}>
            <td>Sent At Epoch</td>
            <td>{msg.SentEpoch+''} ({elapsed} epochs / {x} ago)</td>
        </tr>
        <tr key={"to"}>
            <td>To</td>
            <td className="address">{msg.To}</td>
        </tr>
        <tr key={"from"}>
            <td>From</td>
            <td className="address">{msg.From}</td>
        </tr>
        <tr key={i+"nonce"}>
            <td>Nonce</td>
            <td>{msg.Nonce+''}</td>
        </tr>
        <tr key={i+"value"}>
            <td>Value</td>
            <td>{humanFIL(msg.Value)}</td>
        </tr>
        <tr key={i+"method"}>
            <td>Method</td>
            <td>{msg.Method}</td>
        </tr>
        <tr key={i+"params"}>
            <td>Params</td>
            <td>
                <div onClick={handleParamsClick} className="params">{msg.Params}</div>
            </td>
        </tr>
        <tr key={i+"gas-fee-cap"}>
            <td>Gas Fee Cap</td>
            <td>{humanFIL(msg.GasFeeCap)}</td>
        </tr>
        <tr key={i+"gas-limit"}>
            <td>Gas Limit</td>
            <td>{humanFIL(msg.GasLimit)}</td>
        </tr>
        <tr key={i+"gas-premium"}>
            <td>Gas Premium</td>
            <td>{humanFIL(msg.GasPremium)}</td>
        </tr>
        <tr key={i+"base-fee"}>
            <td>
                Base Fee
                <FeeGraph msg={msg} />
            </td>
            <td>{humanFIL(msg.BaseFee)}</td>
        </tr>
        <tr key={i+"max-fees"} className="max-fees">
            <td>Max Fees</td>
            <td>
                Gas Limit x Gas Fee Cap<br/>
                {humanFIL(msg.GasLimit * msg.GasFeeCap)}
            </td>
        </tr>
        <tr key={i+"miner-reward"} className="miner-reward">
            <td>Miner Reward</td>
            <td>
                Gas Limit x Gas Premium<br/>
                {humanFIL(msg.GasLimit * msg.GasPremium)}
            </td>
        </tr>
        <tr key={i+"spacer"} className="spacer">
            <td colSpan="2"><hr/></td>
        </tr>
    </>
}

function FeeGraph(props) {
    return <div className="fee-graph">
        <div className="max-fees">
            <div className="axes" />
            <div className="gas-premium"></div>
            <div className="max-fees-label">Max Fees</div>
        </div>
        <div className="label-gas-limit">
            Gas Limit
            <div className="tick-right" />
        </div>
        <div className="label-gas-premium">
            Gas Premium
            <div className="tick-top" />
        </div>
        <div className="label-gas-fee-cap">
            Gas Fee Cap
            <div className="tick-top" />
        </div>
    </div>
}
