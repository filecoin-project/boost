/* global BigInt */

import {useMutation, useQuery} from "@apollo/react-hooks";
import {Libp2pAddrInfoQuery, StorageAskQuery, StorageAskUpdate} from "./gql";
import React, {useState} from "react";
import {PageContainer} from "./Components";
import {Link} from "react-router-dom";
import moment from "moment"
import settingsImg from './bootstrap-icons/icons/gear.svg'
import './Settings.css'
import {addCommas, humanFileSize} from "./util";

export function SettingsPage(props) {
    return <PageContainer pageType="settings" title="Settings">
        <SettingsContent />
    </PageContainer>
}

function SettingsContent() {
    return (
        <>
            <AddrInfo />
            <StorageAsk />
        </>
    )
}

function AddrInfo(props) {
    const {loading, error, data} = useQuery(Libp2pAddrInfoQuery)

    if (loading) {
        return <div>Loading...</div>
    }
    if (error) {
        return <div>Error: {error.message}</div>
    }

    return (
        <div>
            <h3>Libp2p Address Info</h3>
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
                                return <div className="address" key={addr}>{addr}</div>
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
        <div>
            <h3>Storage Ask</h3>
            <table>
                <tbody>
                    <EditableField
                        name="Price"
                        fieldName="Price"
                        type="fil"
                        value={data.storageAsk.Price}
                    />
                    <EditableField
                        name="Verified Price"
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

function toNano(num) {
    const tmp = (BigInt(1e6)*BigInt(num))/BigInt(1e9)
    return Number(tmp)/1e6
}

export function EditableField(props) {
    const isCurrency = props.type === 'fil'
    const initialValue = isCurrency ? toNano(props.value)+'' : props.value+''
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
            rawVal = BigInt(currentVal * 1e9)
        }
        update[props.fieldName] = rawVal
        storageAskUpdate({
            variables: {update}
        })
        setEditing(false)
    }

    var displayVal
    if (isCurrency) {
        displayVal = currentVal + ' nano'
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
                    /> {isCurrency ? 'nano' : 'bytes'}
                    <div className="button" onClick={save}>Save</div>
                    <div className="button cancel" onClick={cancel}>Cancel</div>
                </td>
            ) : (
                <td className="val" onClick={() => setEditing(true)}>
                    {displayVal}
                    <span className="edit" />
                </td>
            )}
        </tr>
    )
}

export function SettingsMenuItem(props) {
    return (
        <Link key="settings" className="menu-item" to="/settings">
            <img className="icon" alt="" src={settingsImg} />
            <h3>Settings</h3>
        </Link>
    )
}