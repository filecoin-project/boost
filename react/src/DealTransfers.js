import React from "react";
import { Chart } from "react-google-charts";
import {useQuery} from "./hooks";
import {TransfersQuery} from "./gql";
import moment from "moment"

var maxBytes = 0

export function DealTransfersPage(props) {
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

    var points = data.transfers
    points.sort((a, b) => a.At.getTime() - b.At.getTime())

    var chartData = [cols]
    for (const point of points) {
        chartData.push([moment(point.At).format('HH:mm:ss'), point.Bytes])
        if (point.Bytes > maxBytes) {
            maxBytes = point.Bytes
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
                vAxis: { maxValue: maxBytes || undefined },
            }}
        />
    </div>
}
