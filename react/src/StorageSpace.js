import {useQuery} from "@apollo/react-hooks";
import {StorageQuery} from "./gql";
import React from "react";
import {addCommas, humanFileSize} from "./util";
import './StorageSpace.css'
import archiveImg from './bootstrap-icons/icons/archive.svg'
import {PageContainer} from "./Components";
import {Link} from "react-router-dom";
import {CumulativeBarChart, CumulativeBarLabels} from "./CumulativeBarChart";

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

    const bars = [{
        name: 'Staged',
        className: 'staged',
        amount: storage.Staged,
    }, {
        name: 'Transferred',
        className: 'transferred',
        amount: storage.Transferred,
    }, {
        name: 'Pending',
        className: 'pending',
        amount: storage.Pending,
    }, {
        name: 'Free',
        className: 'free',
        amount: storage.Free,
    }]

    return <>
        <div className="storage-chart">
            <CumulativeBarChart bars={bars} unit="byte" />
            <CumulativeBarLabels bars={bars} unit="byte" />
        </div>

        <table className="storage-fields">
            <tbody>
                {bars.map(bar => (
                    <tr key={bar.name}>
                        <td>{bar.name}</td>
                        <td>{humanFileSize(bar.amount)} <span className="aux">({addCommas(bar.amount)} bytes)</span></td>
                    </tr>
                ))}
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

    var totalSize = 0n
    var used = 0n
    if (data) {
        const storage = data.storage

        totalSize += storage.Staged
        totalSize += storage.Transferred
        totalSize += storage.Pending
        totalSize += storage.Free
        used = totalSize - storage.Free
    }

    const bars = [{
        className: 'used',
        amount: used,
    }, {
        className: 'free',
        amount: totalSize - used,
    }]

    return (
        <Link key="storage-space" className="menu-item storage-space" to="/storage-space">
            <img className="icon" alt="" src={archiveImg} />
            <h3>Storage Space</h3>
            <div className="menu-desc">
                <CumulativeBarChart bars={bars} unit="byte" compact={true} />
                <b>{humanFileSize(used)}</b> of <b>{humanFileSize(totalSize)}</b> used
            </div>
        </Link>
    )
}