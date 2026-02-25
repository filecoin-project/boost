/* global BigInt */

import {useMutation, useQuery} from "@apollo/client";
import {Libp2pAddrInfoQuery, StorageAskQuery, StorageAskUpdate} from "./gql";
import React, {useState} from "react";
import {PageContainer} from "./Components";
import {Link} from "react-router-dom";
import moment from "moment"
import settingsImg from './bootstrap-icons/icons/gear.svg'
import './Settings.css'
import {addCommas, humanFIL, humanFileSize, oneNanoFil} from "./util";

export function SettingsPage(props) {
    return <PageContainer pageType="settings" title="Settings">
        <SettingsContent />
    </PageContainer>
}

function SettingsContent() {
    return (
        <>
            <StorageAsk />
            <Libp2pInfo />
        </>
    )
}

function Libp2pInfo(props) {
    const {loading, error, data} = useQuery(Libp2pAddrInfoQuery)

    if (loading) {
        return <div>Loading...</div>
    }
    if (error) {
        return <div>Error: {error.message}</div>
    }

    return (
        <div className="libp2p">
            <h3>Libp2p</h3>
            <table>
                <tbody>
                    <tr>
                        <th>Peer ID</th>
                        <td>{data.libp2pAddrInfo.PeerID}</td>
                    </tr>
                    <tr>
                        <th>Addresses</th>
                        <td>
                            {data.libp2pAddrInfo.Addresses.map(addr => {
                                return <div key={addr}>{addr}</div>
                            })}
                        </td>
                    </tr>
                    <tr>
                        <th>Protocols</th>
                        <td>
                            {data.libp2pAddrInfo.Protocols.map(proto => {
                                return <div key={proto}>{proto}</div>
                            })}
                        </td>
                    </tr>
                </tbody>
            </table>
        </div>
    )
}

function StorageAsk(props) {
    const {loading, error, data} = useQuery(StorageAskQuery)

    if (loading) {
        return <div>Loading...</div>
    }
    if (error) {
        return <div>Error: {error.message}</div>
    }

    return (
        <div className="storage-ask">
            <h3>Storage Ask</h3>
            <table>
                <tbody>
                    <EditableField
                        name="Price / epoch / Gib"
                        fieldName="Price"
                        type="fil"
                        value={data.storageAsk.Price}
                    />
                    <EditableField
                        name="Verified Price / epoch / Gib"
                        fieldName="VerifiedPrice"
                        type="fil"
                        value={data.storageAsk.VerifiedPrice}
                    />
                    <EditableField
                        name="Min Piece Size"
                        fieldName="MinPieceSize"
                        value={data.storageAsk.MinPieceSize}
                    />
                    <EditableField
                        name="Max Piece Size"
                        fieldName="MaxPieceSize"
                        value={data.storageAsk.MaxPieceSize}
                    />
                    <tr>
                        <th>Expiry Epoch</th>
                        <td>
                            {addCommas(data.storageAsk.ExpiryEpoch)}
                            <span className="expiry-time">
                                {(moment(data.storageAsk.ExpiryTime).fromNow())}
                            </span>
                        </td>
                    </tr>
                </tbody>
            </table>
        </div>
    )
}

export function EditableField(props) {
    const isCurrency = props.type === 'fil'
    const initialValue = props.value+''
    const [editing, setEditing] = useState(false)
    const [currentVal, setCurrentVal] = useState(initialValue)
    const [storageAskUpdate] = useMutation(StorageAskUpdate, {
        refetchQueries: [{ query: StorageAskQuery }],
    })
    const handleValChange = (event) => {
        setCurrentVal(event.target.value)
    }
    const save = () => {
        const update = {}
        var rawVal = currentVal || 0
        if (isCurrency) {
            rawVal = BigInt(currentVal)
            setCurrentVal(rawVal+'')
        }
        update[props.fieldName] = rawVal
        storageAskUpdate({
            variables: {update}
        })
        setEditing(false)
    }

    var displayVal
    if (isCurrency) {
        displayVal = addCommas(currentVal) + ' atto'
    } else {
        displayVal = humanFileSize(BigInt(currentVal)) + ''
    }

    const cancel = () => {
        setEditing(false)
        setCurrentVal(initialValue)
    }

    return (
        <tr>
            <th>{props.name}</th>
            {editing ? (
                <td className="editor">
                    <input
                        type="number"
                        value={currentVal}
                        onChange={handleValChange}
                    /> {isCurrency ? 'atto' : 'bytes'}
                    <div className="button" onClick={save}>Save</div>
                    <div className="button cancel" onClick={cancel}>Cancel</div>
                    <PreviewAmt currentVal={currentVal} isCurrency={isCurrency} />
                </td>
            ) : (
                <td className="val" onClick={() => setEditing(true)}>
                    {displayVal}
                    <span className="edit" />
                    { isCurrency ? <PreviewAmt currentVal={currentVal} isCurrency={isCurrency}  /> : null }
                </td>
            )}
        </tr>
    )
}

function PreviewAmt({currentVal, isCurrency}) {
    if (isCurrency) {
        if (BigInt(currentVal) > oneNanoFil) {
            return <span className="human">({humanFIL(BigInt(currentVal))})</span>
        }
    } else {
        if (BigInt(currentVal) > BigInt(1024)) {
            return <span className="human">({humanFileSize(BigInt(currentVal))})</span>
        }
    }
    return null
}

export function SettingsMenuItem(props) {
    return (
        <Link key="settings" className="menu-item" to="/settings">
            <img className="icon" alt="" src={settingsImg} />
            <h3>Settings</h3>
        </Link>
    )
}