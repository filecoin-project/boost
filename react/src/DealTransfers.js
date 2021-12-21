import React from "react";
import { Chart } from "react-google-charts";
import {useQuery} from "@apollo/react-hooks";
import {TransfersQuery} from "./gql";
import moment from "moment"
import {PageContainer} from "./Components";

var maxMegabits = 0

export function DealTransfersPage(props) {
    return <PageContainer pageType="deal-transfers" title="Deal Transfers">
        <DealTransfersContent />
    </PageContainer>
}

function DealTransfersContent(props) {
    const {loading, error, data} = useQuery(TransfersQuery, { pollInterval: 1000 })

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
