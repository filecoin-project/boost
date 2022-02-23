import {useQuery} from "@apollo/react-hooks";
import {Libp2pAddrInfoQuery} from "./gql";
import React from "react";
import {PageContainer} from "./Components";
import {Link} from "react-router-dom";
import settingsImg from './bootstrap-icons/icons/gear.svg'
import './Settings.css'

export function SettingsPage(props) {
    return <PageContainer pageType="settings" title="Settings">
        <SettingsContent />
    </PageContainer>
}

function SettingsContent() {
    const {loading, error, data} = useQuery(Libp2pAddrInfoQuery)

    if (loading) {
        return <div>Loading...</div>
    }
    if (error) {
        return <div>Error: {error.message}</div>
    }

    return (
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