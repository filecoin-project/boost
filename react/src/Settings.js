/* global BigInt */

import {useMutation, useQuery} from "@apollo/react-hooks";
import {Libp2pAddrInfoQuery, StorageAskQuery, StorageAskUpdate} from "./gql";
import React, {useState} from "react";
import {PageContainer} from "./Components";
import {NavLink} from "react-router-dom";
import moment from "moment"
import settingsImg from './bootstrap-icons/icons/gear.svg'
import './Settings.css'
import {addCommas, humanFIL, humanFileSize, oneNanoFil} from "./util";

export function SettingsPage(props) {
    return <PageContainer icon={<SettingsIcon />} title="Settings">
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
            <h5>Libp2p</h5>
            <table className="horizontal-table fixed-width">
                <tbody>
                    <tr>
                        <th>Peer ID</th>
                        <td className="fixed-width">{data.libp2pAddrInfo.PeerID}</td>
                    </tr>
                    <tr>
                        <th>Addresses</th>
                        <td className="fixed-width">
                            {data.libp2pAddrInfo.Addresses.map(addr => {
                                return <div key={addr}>{addr}</div>
                            })}
                        </td>
                    </tr>
                    <tr>
                        <th>Protocols</th>
                        <td className="fixed-width">
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
            <h5>Storage Ask</h5>
            <table className="horizontal-table">
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
                                ({(moment(data.storageAsk.ExpiryTime).fromNow())})
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
    const initialValue = isCurrency ? props.value+'' : props.value+''
    const [editing, setEditing] = useState(false)
    const [currentVal, setCurrentVal] = useState(initialValue)
    const [storageAskUpdate] = useMutation(StorageAskUpdate, {
        refetchQueries: [{ query: StorageAskQuery }],
    })
    const handleValChange = (event) => {
        setCurrentVal(event.target.value)
    }

    var sanitizedVal = currentVal
    if (sanitizedVal.indexOf('.') >= 0) {
        sanitizedVal = sanitizedVal.replace(/\..*/, '')
    }
    const save = () => {
        const update = {}
        var rawVal = sanitizedVal || 0
        if (isCurrency) {
            rawVal = BigInt(sanitizedVal)
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
        displayVal = addCommas(sanitizedVal) + ' atto'
    } else {
        displayVal = humanFileSize(BigInt(sanitizedVal)) + ''
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
                    <div className="button btn btn-primary" onClick={save}>Save</div>
                    <div className="button cancel btn btn-primary" onClick={cancel}>Cancel</div>
                    { BigInt(sanitizedVal) > oneNanoFil ? (
                        <span className="human">({humanFIL(BigInt(sanitizedVal))})</span>
                    ) : null }
                </td>
            ) : (
                <td className="val" onClick={() => setEditing(true)}>
                    {displayVal}
                    <span className="edit" />
                    { BigInt(sanitizedVal) > oneNanoFil ? (
                        <span className="human">({humanFIL(BigInt(sanitizedVal))})</span>
                    ) : null }
                </td>
            )}
        </tr>
    )
}

export function SettingsMenuItem(props) {
    return (
        <NavLink key="settings" className="sidebar-item sidebar-item-deal-transfers" to="/settings">
            <span className="sidebar-icon">
                <SettingsIcon />
            </span>
            <span className="sidebar-title">Settings</span>
        </NavLink>
    )
}

function SettingsIcon(props) {
    return <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" className="bi bi-gear"
                viewBox="0 0 16 16">
        <path
            d="M8 4.754a3.246 3.246 0 1 0 0 6.492 3.246 3.246 0 0 0 0-6.492zM5.754 8a2.246 2.246 0 1 1 4.492 0 2.246 2.246 0 0 1-4.492 0z"/>
        <path
            d="M9.796 1.343c-.527-1.79-3.065-1.79-3.592 0l-.094.319a.873.873 0 0 1-1.255.52l-.292-.16c-1.64-.892-3.433.902-2.54 2.541l.159.292a.873.873 0 0 1-.52 1.255l-.319.094c-1.79.527-1.79 3.065 0 3.592l.319.094a.873.873 0 0 1 .52 1.255l-.16.292c-.892 1.64.901 3.434 2.541 2.54l.292-.159a.873.873 0 0 1 1.255.52l.094.319c.527 1.79 3.065 1.79 3.592 0l.094-.319a.873.873 0 0 1 1.255-.52l.292.16c1.64.893 3.434-.902 2.54-2.541l-.159-.292a.873.873 0 0 1 .52-1.255l.319-.094c1.79-.527 1.79-3.065 0-3.592l-.319-.094a.873.873 0 0 1-.52-1.255l.16-.292c.893-1.64-.902-3.433-2.541-2.54l-.292.159a.873.873 0 0 1-1.255-.52l-.094-.319zm-2.633.283c.246-.835 1.428-.835 1.674 0l.094.319a1.873 1.873 0 0 0 2.693 1.115l.291-.16c.764-.415 1.6.42 1.184 1.185l-.159.292a1.873 1.873 0 0 0 1.116 2.692l.318.094c.835.246.835 1.428 0 1.674l-.319.094a1.873 1.873 0 0 0-1.115 2.693l.16.291c.415.764-.42 1.6-1.185 1.184l-.291-.159a1.873 1.873 0 0 0-2.693 1.116l-.094.318c-.246.835-1.428.835-1.674 0l-.094-.319a1.873 1.873 0 0 0-2.692-1.115l-.292.16c-.764.415-1.6-.42-1.184-1.185l.159-.291A1.873 1.873 0 0 0 1.945 8.93l-.319-.094c-.835-.246-.835-1.428 0-1.674l.319-.094A1.873 1.873 0 0 0 3.06 4.377l-.16-.292c-.415-.764.42-1.6 1.185-1.184l.292.159a1.873 1.873 0 0 0 2.692-1.115l.094-.319z"/>
    </svg>
}