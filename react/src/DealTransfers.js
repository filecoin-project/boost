import React from "react";
import {Chart} from "react-google-charts";
import {useQuery} from "@apollo/react-hooks";
import {TransfersQuery} from "./gql";
import moment from "moment"
import {PageContainer} from "./Components";
import {NavLink} from "react-router-dom";
import {toFixed} from "./util";
import './DealTransfers.css'

var maxMegabits = 0

export function DealTransfersPage(props) {
    return <PageContainer icon={<DealTransfersIcon />} title="Deal Transfers">
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

    // var chartData = [["Time","Transfers"],["10:22:26",3.057664],["10:22:27",4.85376],["10:22:28",5.0176],["10:22:29",4.88448],["10:22:30",4.8128],["10:22:31",4.849664],["10:22:32",4.78208],["10:22:33",4.933632],["10:22:34",4.929536],["10:22:35",4.974592],["10:22:36",1.605632],["10:22:37",0],["10:22:38",0],["10:22:39",0]]

    return <div className="transfers-chart">
        <Chart
            width={'100%'}
            height={'500px'}
            chartType="LineChart"
            loader={<div>Loading Chart</div>}
            data={chartData}
            options={{
                backgroundColor: 'transparent',
                colors: ["#B92454"],
                lineWidth: 5,
                fontName: '"Poppins", sans-serif',
                hAxis: {
                    titleTextStyle: { color: '#415364' },
                    showTextEvery: 2,
                },
                vAxis: {
                    minValue: 0,
                    maxValue: maxMegabits || undefined,
                    title: 'Megabits / s',
                    textStyle: { bold: true, color: '#415364' },
                    baselineColor: '#bbb',
                },
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

    return <NavLink key="deal-transfers" className="sidebar-item sidebar-item-deal-transfers" to="/deal-transfers">
        <span className="sidebar-icon">
            <DealTransfersIcon />
        </span>
        <span className="sidebar-title">Deal Transfers</span>
        <div className="sidebar-item-excerpt">
            <span className="figure">{toFixed(dataRate, 1)}</span>
            <span className="label">Mbps</span>
        </div>
    </NavLink>
}

function DealTransfersIcon(props) {
    return <svg className="alternative" width="22" height="30" viewBox="0 0 22 30" xmlns="http://www.w3.org/2000/svg">
        <path
            d="M1 21a1 1 0 1 0 0 2v-2zm20.707 1.707a1 1 0 0 0 0-1.414l-6.364-6.364a1 1 0 0 0-1.414 1.414L19.586 22l-5.657 5.657a1 1 0 0 0 1.414 1.414l6.364-6.364zM1 23h20v-2H1v2zM21 9a1 1 0 1 0 0-2v2zM.293 7.293a1 1 0 0 0 0 1.414l6.364 6.364a1 1 0 0 0 1.414-1.414L2.414 8l5.657-5.657A1 1 0 0 0 6.657.93L.293 7.293zM21 7H1v2h20V7z"/>
    </svg>
}