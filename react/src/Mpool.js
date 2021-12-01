import {useQuery} from "./hooks";
import {MpoolQuery} from "./gql";
import React from "react";
import {humanFIL} from "./util";

export function MpoolPage(props) {
    const {loading, error, data} = useQuery(MpoolQuery)

    if (loading) {
        return <div>Loading...</div>
    }
    if (error) {
        return <div>Error: {error.message}</div>
    }

    const msgs = data.mpool

    return <div className="mpool">
        <table>
            <tbody>
                {msgs.map(msg => (
                    <>
                        <tr>
                            <td>To</td>
                            <td className="address">{msg.To}</td>
                        </tr>
                        <tr>
                            <td>From</td>
                            <td className="address">{msg.From}</td>
                        </tr>
                        <tr>
                            <td>Nonce</td>
                            <td>{msg.Nonce}</td>
                        </tr>
                        <tr>
                            <td>Value</td>
                            <td>{humanFIL(msg.Value)}</td>
                        </tr>
                        <tr>
                            <td>GasFeeCap</td>
                            <td>{humanFIL(msg.GasFeeCap)}</td>
                        </tr>
                        <tr>
                            <td>GasLimit</td>
                            <td>{humanFIL(msg.GasLimit)}</td>
                        </tr>
                        <tr>
                            <td>GasPremium</td>
                            <td>{humanFIL(msg.GasPremium)}</td>
                        </tr>
                        <tr>
                            <td>Method</td>
                            <td>{msg.Method}</td>
                        </tr>
                        <tr>
                            <td>Params</td>
                            <td>{msg.Params}</td>
                        </tr>
                        <tr className="spacer">
                            <td colSpan="2"><hr/></td>
                        </tr>
                    </>
                ))}
            </tbody>
        </table>
    </div>
}
