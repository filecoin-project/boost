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
    totalSize += storage.Completed
    totalSize += storage.Transferring
    totalSize += storage.Pending
    totalSize += storage.Free

    return <>
        <div className="storage-chart">
            <div className="total-size">
                {humanFileSize(totalSize)}
            </div>
            <div className="bars">
                <div className="completed" style={{width: Math.floor(100*storage.Completed/totalSize)+'%'}} />
                <div className="transferring" style={{width: Math.floor(100*storage.Transferring/totalSize)+'%'}} />
                <div className="pending" style={{width: Math.floor(100*storage.Pending/totalSize)+'%'}} />
                <div className="free" style={{width: Math.floor(100*storage.Free/totalSize)+'%'}} />
            </div>
            <div className="labels">
                <div className="label completed">
                    <div className="bar-color"></div>
                    <div className="text">Completed</div>
                    <div className="amount">{humanFileSize(storage.Completed)}</div>
                </div>
                <div className="label transferring">
                    <div className="bar-color"></div>
                    <div className="text">Transferring</div>
                    <div className="amount">{humanFileSize(storage.Transferring)}</div>
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
                    <td>Completed</td>
                    <td>{Math.round(storage.Completed).toLocaleString('en-US')} bytes</td>
                </tr>
                <tr>
                    <td>Transferring</td>
                    <td>{Math.round(storage.Transferring).toLocaleString('en-US')} bytes</td>
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
