import React from "react";
import {Chart} from "react-google-charts";
import {useQuery} from "@apollo/client";
import {TransfersQuery, TransferStatsQuery} from "./gql";
import moment from "moment"
import {PageContainer} from "./Components";
import {Link} from "react-router-dom";
import arrowLeftRightImg from './bootstrap-icons/icons/arrow-left-right.svg'
import './DealTransfers.css'

var maxMegabits = 0

export function DealTransfersPage(props) {
    return <PageContainer pageType="deal-transfers" title="Deal Transfers">
        <DealTransfersContent />
    </PageContainer>
}

function DealTransfersContent(props) {
    return <div>
        <DealTransfersChart />
        <TransferStats />
    </div>
}

function DealTransfersChart(props) {
    const {loading, error, data} = useQuery(TransfersQuery, {
        pollInterval: 1000,
        fetchPolicy: 'network-only',
    })

    if (loading) {
        return <div>Loading...</div>
    }
    if (error) {
        return <div>Error: {error.message}</div>
    }

    if (data.transfers.length === 0) {
        return <div>No active transfers</div>
    }

    var cols = ['Time', 'Transfers']

    // Clone from read-only to writable array
    var points = data.transfers.map(p => ({ At: p.At, Bytes: p.Bytes }))
    points.sort((a, b) => a.At.getTime() - b.At.getTime())

    var chartData = [cols]
    for (const point of points) {
        const megabits = 8 * Number(point.Bytes) / 1e6
        chartData.push([moment(point.At).format('HH:mm:ss'), megabits])
        if (megabits > maxMegabits) {
            maxMegabits = megabits
        }
    }

    // chartData = [
    //     ['Time', 'Transfers'],
    //     ['10:15:05', 800],
    //     ['10:15:06', 840],
    //     ['10:15:07', 660],
    // ]

    return <div>
        <Chart
            width={800}
            height={'400px'}
            chartType="LineChart"
            loader={<div>Loading Chart</div>}
            data={chartData}
            options={{
                hAxis: { titleTextStyle: { color: '#333' } },
                vAxis: { minValue: 0, maxValue: maxMegabits || undefined, title: 'Megabits / s' },
            }}
        />
    </div>
}

function TransferStats(props) {
    const {loading, error, data} = useQuery(TransferStatsQuery, {
        pollInterval: 1000,
        fetchPolicy: 'network-only',
    })

    if (loading) {
        return <div>Loading...</div>
    }
    if (error) {
        return <div>Error: {error.message}</div>
    }

    const stats = data.transferStats
    return <div className="transfer-stats">
        <div>
            <h3>Transfers by Host</h3>
            <table>
                <tbody>
                <tr>
                    <th>Host</th>
                    <th>Total</th>
                    <th>Queued</th>
                    <th>Active</th>
                    <th>Stalled</th>
                    <th>Rate</th>
                </tr>
                { stats.Stats.map((hostStats) => {
                    return <tr key={hostStats.Host}>
                        <td>{hostStats.Host}</td>
                        <td>{hostStats.Total}</td>
                        <td>{hostStats.Total-hostStats.Started}</td>
                        <td>{hostStats.Started-hostStats.Stalled}</td>
                        <td>{hostStats.Stalled}</td>
                        <td className="transfer-rate">{getTransferRate(hostStats.TransferSamples)}</td>
                    </tr>
                }) }
                </tbody>
            </table>
        </div>

        <div className="config">
            <h3>Config</h3>
            <table>
                <tbody>
                <tr>
                    <th>HTTP concurrent download limit:</th>
                    <td>{stats.HttpMaxConcurrentDownloads}</td>
                </tr>
                </tbody>
            </table>
        </div>
    </div>
}

function getTransferRate(samples) {
    var dataRate = 0
    if (samples && samples.length) {
        // Clone from read-only to writable array
        var points = samples.map(p => ({At: p.At, Bytes: p.Bytes}))
        points.sort((a, b) => a.At.getTime() - b.At.getTime())

        // Get average transfer rate over last 10 seconds
        const subset = points.slice(-10)
        // Allow for some clock skew, but ignore samples older than 2 minutes
        const cutOff = new Date(new Date().getTime() - 2*60*1000)
        var total = 0
        var count = 0
        for (const point of subset) {
            if (point.At > cutOff) {
                total += Number(point.Bytes)
                count++
            }
        }
        if (count > 0) {
            dataRate = total / count
        }
    }
    return humanTransferRate(dataRate)
}

export function humanTransferRate(bytesPerSecond) {
    // Convert from bytes to kbps or Mbps
    const kbps = (bytesPerSecond*8) / 1024
    if (kbps < 1024) {
        if (kbps < 10) {
            return kbps.toFixed(2) + " kbps"
        }
        return kbps.toFixed(1) + " kbps"
    }
    const mbps = kbps / 1024
    if (mbps < 10) {
        return mbps.toFixed(2) + " Mbps"
    }
    return mbps.toFixed(1) + " Mbps"
}

export function DealTransfersMenuItem(props) {
    const {data} = useQuery(TransfersQuery, {
        pollInterval: 5000,
        fetchPolicy: 'network-only',
    })

    var dataRate = ""
    if (data && data.transfers) {
        dataRate = getTransferRate(data.transfers)
    }

    return <Link key="deal-transfers" className="menu-item" to="/deal-transfers">
        <img className="icon" alt="" src={arrowLeftRightImg} />
        <h3>Deal Transfers</h3>
        <div className="menu-desc">
            <b>{dataRate}</b> (10s avg)
        </div>
    </Link>
}
