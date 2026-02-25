import {useQuery} from "@apollo/client";
import {StorageQuery} from "./gql";
import React from "react";
import {addCommas, humanFileSize} from "./util";
import './StorageSpace.css'
import archiveImg from './bootstrap-icons/icons/archive.svg'
import {PageContainer} from "./Components";
import {Link} from "react-router-dom";
import {CumulativeBarChart, CumulativeBarLabels} from "./CumulativeBarChart";
import {Info} from "./Info"

export function StorageSpacePage(props) {
    return <PageContainer pageType="storage-space" title="Storage Space">
        <StorageSpaceContent />
    </PageContainer>
}

function StorageSpaceContent(props) {
    const {loading, error, data} = useQuery(StorageQuery, { pollInterval: 10000 })

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
        description: 'Deal data that has completed downloading and is waiting to be added to a sector'
    }, {
        name: 'Transferred',
        className: 'transferred',
        amount: storage.Transferred,
        description: 'Deal data that has been downloaded so far in an ongoing transfer'
    }, {
        name: 'Pending',
        className: 'pending',
        amount: storage.Pending,
        description: 'The total space needed for data that is currently being downloaded'
    }, {
        name: 'Free',
        className: 'free',
        amount: storage.Free,
        description: 'Available space for future downloads'
    }]

    return <>
        <h3>Deal transfers</h3>

        <div className="storage-chart">
            <CumulativeBarChart bars={bars} unit="byte" />
            <CumulativeBarLabels bars={bars} unit="byte" />
        </div>

        <table className="storage-fields">
            <tbody>
                {bars.map(bar => (
                    <tr key={bar.name}>
                        <td>
                            {bar.name}
                            <Info>{bar.description}</Info>
                        </td>
                        <td>{humanFileSize(bar.amount)} <span className="aux">({addCommas(bar.amount)} bytes)</span></td>
                    </tr>
                ))}
                <tr>
                    <td>
                        Mount Point
                        <Info>The path to the directory where downloaded data is kept until the deal is added to a sector</Info>
                    </td>
                    <td>{storage.MountPoint}</td>
                </tr>
            </tbody>
        </table>
    </>
}

export function StorageSpaceMenuItem(props) {
    const {data} = useQuery(StorageQuery, {
        pollInterval: 10000,
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
