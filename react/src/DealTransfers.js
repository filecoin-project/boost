import React from "react";
import {Chart} from "react-google-charts";
import {useQuery} from "@apollo/react-hooks";
import {TransfersQuery} from "./gql";
import moment from "moment"
import {PageContainer} from "./Components";
import {Link} from "react-router-dom";
import {toFixed} from "./util";
import arrowLeftRightImg from './bootstrap-icons/icons/arrow-left-right.svg'

var maxMegabits = 0

export function DealTransfersPage(props) {
    return <PageContainer pageType="deal-transfers" title="Deal Transfers">
        <DealTransfersContent />
    </PageContainer>
}

function DealTransfersContent(props) {
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

export function DealTransfersMenuItem(props) {
    const {data} = useQuery(TransfersQuery, {
        pollInterval: 5000,
        fetchPolicy: 'network-only',
    })

    var dataRate = 0
    if (data && data.transfers.length) {
        // Clone from read-only to writable array
        var points = data.transfers.map(p => ({At: p.At, Bytes: p.Bytes}))
        points.sort((a, b) => a.At.getTime() - b.At.getTime())

        // Get average transfer rate over last 10 seconds
        var total = 0
        const tenSecondsAgo = new Date().getTime() - 11 * 1000
        for (const point of points) {
            if (point.At.getTime() > tenSecondsAgo) {
                const megabits = 8 * Number(point.Bytes) / 1e6
                total += megabits
            }
        }
        dataRate = total / 10
    }

    return <Link key="deal-transfers" className="menu-item" to="/deal-transfers">
        <img className="icon" alt="" src={arrowLeftRightImg} />
        <h3>Deal Transfers</h3>
        <div className="menu-desc">
            <b>{toFixed(dataRate, 1)}</b> Mbps (10s avg)
        </div>
    </Link>
}