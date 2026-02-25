/* global BigInt */
import {addCommas, humanFIL, humanFileSize, isInteger} from "./util";
import React from "react";
import './CumulativeBarChart.css'

export function CumulativeBarChart(props) {
    var totalSize = 0n
    for (const bar of props.bars) {
        bar.amount = BigInt(bar.amount)
        totalSize += bar.amount
    }
    const total = amountToString(props.unit, totalSize)

    return <div className={'cumulative-bar-chart' + (props.compact ? ' compact' : '')}>
        <div className="bars">
            {props.bars.map((bar, i) => (
                <Bar
                    key={bar.className}
                    totalSize={totalSize}
                    bar={bar}
                    isLast={i === props.bars.length - 1}
                />
            ))}
        </div>
        <div className="total">{total}</div>
    </div>
}

function Bar(props) {
    const {bar, isLast, totalSize} = props

    var barWidthPct = 0
    if (totalSize > 0) {
        barWidthPct = Number(bar.amount * 100n / totalSize)
        if (barWidthPct === 0) {
            // Don't display bars that are 0% of the total
            return null
        }
    } else {
        // If the total size of all bars is 0, only show the last bar with
        // a width of 100%
        if (isLast) {
            barWidthPct = 100
        } else {
            return null
        }
    }

    return (
        <div className={'bar ' + bar.className} style={{width: barWidthPct + '%'}}>
            <div className="bar-inner" />
        </div>
    )
}

export function CumulativeBarLabels(props) {
    return <div className="cumulative-bar-labels">
        {props.bars.map(bar => (
            <div className="label" key={bar.className}>
                <div className={'bar-color ' + bar.className}></div>
                <div className="text">{bar.name}</div>
                <div className="amount">{amountToString(props.unit, bar.amount)}</div>
            </div>
        ))}
    </div>
}

function amountToString(unit, amount) {
    switch (unit) {
        case 'byte':
            return humanFileSize(amount)
        case 'attoFIL':
            return humanFIL(amount)
        default:
            const amtStr = amount.toString()
            if (isInteger(amtStr)) {
                return addCommas(amtStr)
            }
            return amtStr
    }
}

