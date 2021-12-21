import {useQuery} from "@apollo/react-hooks";
import {StorageQuery} from "./gql";
import React from "react";
import {addCommas, humanFileSize} from "./util";
import './StorageSpace.css'
import {PageContainer} from "./Components";
import {Link} from "react-router-dom";

export function StorageSpacePage(props) {
    return <PageContainer pageType="storage-space" title="Storage Space">
        <StorageSpaceContent />
    </PageContainer>
}

function StorageSpaceContent(props) {
    const {loading, error, data} = useQuery(StorageQuery, { pollInterval: 1000 })

    if (loading) {
        return <div>Loading...</div>
    }
    if (error) {
        return <div>Error: {error.message}</div>
    }

    var storage = data.storage

    var totalSize = 0n
    totalSize += storage.Staged
    totalSize += storage.Transferred
    totalSize += storage.Pending
    totalSize += storage.Free

    return <>
        <div className="storage-chart">
            <div className="total-size">
                {humanFileSize(totalSize)}
            </div>
            <div className="bars">
                <div className="staged" style={{width: Number(storage.Staged*100n/totalSize)+'%'}} />
                <div className="transferred" style={{width: Number(storage.Transferred*100n/totalSize)+'%'}} />
                <div className="pending" style={{width: Number(storage.Pending*100n/totalSize)+'%'}} />
                <div className="free" style={{width: Number(storage.Free*100n/totalSize)+'%'}} />
            </div>
            <div className="labels">
                <div className="label staged">
                    <div className="bar-color"></div>
                    <div className="text">Staged</div>
                    <div className="amount">{humanFileSize(storage.Staged)}</div>
                </div>
                <div className="label transferred">
                    <div className="bar-color"></div>
                    <div className="text">Transferred</div>
                    <div className="amount">{humanFileSize(storage.Transferred)}</div>
                </div>
                <div className="label pending">
                    <div className="bar-color"></div>
                    <div className="text">Pending</div>
                    <div className="amount">{humanFileSize(storage.Pending)}</div>
                </div>
                <div className="label free">
                    <div className="bar-color"></div>
                    <div className="text">Free</div>
                    <div className="amount">{humanFileSize(storage.Free)}</div>
                </div>
            </div>
        </div>

        <table className="storage-fields">
            <tbody>
                <tr>
                    <td>Staged</td>
                    <td>{addCommas(storage.Staged)} bytes</td>
                </tr>
                <tr>
                    <td>Transferred</td>
                    <td>{addCommas(storage.Transferred)} bytes</td>
                </tr>
                <tr>
                    <td>Pending</td>
                    <td>{addCommas(storage.Pending)} bytes</td>
                </tr>
                <tr>
                    <td>Free</td>
                    <td>{addCommas(storage.Free)} bytes</td>
                </tr>
                <tr>
                    <td>Mount Point</td>
                    <td>{storage.MountPoint}</td>
                </tr>
            </tbody>
        </table>
    </>
}

export function StorageSpaceMenuItem(props) {
    const {data} = useQuery(StorageQuery, {
        pollInterval: 5000,
        fetchPolicy: 'network-only',
    })

    var pct = 0
    if (data) {
        var storage = data.storage

        var totalSize = 0n
        totalSize += storage.Staged
        totalSize += storage.Transferred
        totalSize += storage.Pending
        totalSize += storage.Free

        pct = ((totalSize - storage.Free) * 100n) / totalSize
    }

    return (
        <Link key="storage-space" className="menu-item" to="/storage-space">
            Storage Space
            <div className="aux">Used: {pct.toString()}%</div>
        </Link>
    )
}