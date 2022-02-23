import {useQuery} from "@apollo/react-hooks";
import {Libp2pAddrInfoQuery, StorageAskQuery} from "./gql";
import React from "react";
import {PageContainer} from "./Components";
import {Link} from "react-router-dom";
import moment from "moment"
import settingsImg from './bootstrap-icons/icons/gear.svg'
import './Settings.css'
import {addCommas, humanFIL, humanFileSize} from "./util";
import {dateFormat} from "./util-date";

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
        return <tr><td colSpan="2">Loading...</td></tr>
    }
    if (error) {
        return <tr><td colSpan="2">Error: {error.message}</td></tr>
    }

    return (
        <div>
            <h3>Storage Ask</h3>
            <table>
                <tbody>
                    <tr>
                        <th>Price</th>
                        <td>{humanFIL(data.storageAsk.Price)}</td>
                    </tr>
                    <tr>
                        <th>Verified Price</th>
                        <td>{humanFIL(data.storageAsk.VerifiedPrice)}</td>
                    </tr>
                    <tr>
                        <th>Min Piece Size</th>
                        <td>{humanFileSize(data.storageAsk.MinPieceSize)}</td>
                    </tr>
                    <tr>
                        <th>Max Piece Size</th>
                        <td>{humanFileSize(data.storageAsk.MaxPieceSize)}</td>
                    </tr>
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

export function SettingsMenuItem(props) {
    return (
        <Link key="settings" className="menu-item" to="/settings">
            <img className="icon" alt="" src={settingsImg} />
            <h3>Settings</h3>
        </Link>
    )
}