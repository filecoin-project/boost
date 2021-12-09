import {useQuery} from "./hooks";
import {StorageQuery} from "./gql";
import React from "react";
import {humanFileSize} from "./util";
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
    var totalSize = 0
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
                <div className="staged" style={{width: Math.floor(100*storage.Staged/totalSize)+'%'}} />
                <div className="transferred" style={{width: Math.floor(100*storage.Transferred/totalSize)+'%'}} />
                <div className="pending" style={{width: Math.floor(100*storage.Pending/totalSize)+'%'}} />
                <div className="free" style={{width: Math.floor(100*storage.Free/totalSize)+'%'}} />
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
                    <td>{Math.round(storage.Staged).toLocaleString('en-US')} bytes</td>
                </tr>
                <tr>
                    <td>Transferred</td>
                    <td>{Math.round(storage.Transferred).toLocaleString('en-US')} bytes</td>
                </tr>
                <tr>
                    <td>Pending</td>
                    <td>{Math.round(storage.Pending).toLocaleString('en-US')} bytes</td>
                </tr>
                <tr>
                    <td>Free</td>
                    <td>{Math.round(storage.Free).toLocaleString('en-US')} bytes</td>
                </tr>
                <tr>
                    <td>Mount Point</td>
                    <td>{storage.MountPoint}</td>
                </tr>
            </tbody>
        </table>
    </>
}
