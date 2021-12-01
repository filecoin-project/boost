import {useQuery} from "./hooks";
import {StorageQuery} from "./gql";
import React from "react";
import {humanFileSize} from "./util";

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

    console.log(data)

    return <>
        <div className="storage-chart">
            <div className="total-size">
                {humanFileSize(totalSize)}
            </div>
            <div className="bars">
                <div className="completed" style={{width: Math.floor(100*storage.Completed/totalSize)+'%'}}>
                    <div className="label">
                        Completed - Adding to sector<br/>
                        {humanFileSize(storage.Completed)}
                    </div>
                </div>
                <div className="transferring" style={{width: Math.floor(100*storage.Transferring/totalSize)+'%'}}>
                    <div className="label">
                        Transferring<br/>
                        {humanFileSize(storage.Transferring)}
                    </div>
                </div>
                <div className="pending" style={{width: Math.floor(100*storage.Pending/totalSize)+'%'}}>
                    <div className="label">
                        Pending<br/>
                        {humanFileSize(storage.Pending)}
                    </div>
                </div>
                <div className="free" style={{width: Math.floor(100*storage.Free/totalSize)+'%'}}>
                    <div className="label">
                        Free<br/>
                        {humanFileSize(storage.Free)}
                    </div>
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
