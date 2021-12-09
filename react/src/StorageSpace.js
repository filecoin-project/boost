import {useQuery} from "./hooks";
import {StorageQuery} from "./gql";
import React from "react";
import {humanFileSize, addCommas} from "./util";
import './StorageSpace.css'

export function StorageSpacePage(props) {
    const {loading, error, data} = useQuery(StorageQuery)

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
                <div className="completed" style={{width: Number(storage.Staged*100n/totalSize)+'%'}} />
                <div className="transferring" style={{width: Number(storage.Transferred*100n/totalSize)+'%'}} />
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
