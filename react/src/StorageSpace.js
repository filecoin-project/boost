import {useQuery} from "@apollo/react-hooks";
import {LegacyStorageQuery, StorageQuery} from "./gql";
import React from "react";
import {addCommas, humanFileSize} from "./util";
import {PageContainer} from "./Components";
import {NavLink} from "react-router-dom";
import {CumulativeBarChart, CumulativeBarLabels} from "./CumulativeBarChart";
import {Info} from "./Info"

export function StorageSpacePage(props) {
    return <PageContainer icon={<StorageSpaceIcon />} title="Storage Space">
        <StorageSpaceContent />
        <LegacyStorageSpaceContent />
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
        <h5>Deal transfers</h5>

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

function LegacyStorageSpaceContent(props) {
    const {loading, error, data} = useQuery(LegacyStorageQuery, { pollInterval: 1000 })

    if (loading) {
        return <div>Loading...</div>
    }
    if (error) {
        return <div>Error: {error.message}</div>
    }

    var storage = data.legacyStorage
    if (storage.Capacity === 0n) {
        return null
    }

    var free = storage.Capacity - storage.Used
    if (free < 0) {
        free = 0
    }
    const bars = [{
        name: 'Used',
        className: 'used',
        amount: storage.Used,
    }, {
        name: 'Free',
        className: 'free',
        amount: free,
    }]

    return <div className="legacy-deal-transfers">
        <h5>Legacy Deal transfers</h5>

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
    </div>
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
        <NavLink key="storage-space" className="sidebar-item sidebar-item-deals" to="/storage-space">
            <span className="sidebar-icon">
                <StorageSpaceIcon />
            </span>
            <span className="sidebar-title">Storage Space</span>
            <div className="sidebar-item-excerpt">
                <div className="row">
                    <div className="col-sm-8 col-xxl-6">
                        <div className="progress">
                            <span className="bar" style={{width:"0%"}}></span>
                        </div>
                        {/*<CumulativeBarChart bars={bars} unit="byte" compact={true} />*/}
                        <div className="explanation">
                            <span className="numerator">{humanFileSize(used)}</span>
                            <span> / </span>
                            <span className="denominator">{humanFileSize(totalSize)}</span>
                        </div>
                    </div>
                </div>
            </div>
        </NavLink>
    )
}

function StorageSpaceIcon(props) {
    return <svg width="26" height="25" viewBox="0 0 26 25" fill="none" xmlns="http://www.w3.org/2000/svg">
        <path d="M3 7h20v15a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V7z" stroke-width="2"/>
        <rect x="1" y="1" width="24" height="6" rx="2" stroke-width="2"/>
        <path d="M8 11h10v2H8z"/>
    </svg>
}