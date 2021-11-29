import React from "react";
import {humanFIL, humanFileSize} from "./util";

export function BarChart(props) {
    var max = 0
    for (let field of props.fields) {
        field.Type = field.Name.replace(/[ ()]/g, '-')
        if (field.Capacity > max) {
            max = field.Capacity
        }
    }

    return <div className="bar-chart">
        <table>
            <tbody>
            <tr>
                {props.fields.map(field => <ChartBar key={field.Type} usage={field} max={max} unit={props.unit} />)}
            </tr>
            <tr>
                {props.fields.map(field => <td key={field.Name} className="label">{field.Name}</td>)}
            </tr>
            </tbody>
        </table>
    </div>
}

function ChartBar(props) {
    var barHeightRatio = props.usage.Capacity / props.max
    var barHeight = Math.round(barHeightRatio * 100)
    var fillerHeight = 100 - barHeight
    var usedPct = Math.floor(100 * props.usage.Used / (props.usage.Capacity || 1))

    var humanize = val => val
    if (props.unit === 'attofil') {
        humanize = humanFIL
    } else if (props.unit === 'bytes') {
        humanize = humanFileSize
    }

    return <td className={'field ' + props.usage.Type}>
        <div className="filler" style={{height: fillerHeight + '%'}}></div>
        <div className="bar" style={{height: barHeight + '%'}}>
            <div className="size">
                <div className="size-text">
                    {humanize(props.usage.Capacity)}
                </div>
            </div>
            {props.usage.Used ? (
                <div style={{height: '100%'}}>
                    <div className="unused" style={{height: (100 - usedPct) + '%'}}/>
                    <div className="used" style={{height: usedPct + '%'}}/>
                </div>
            ) : null}
        </div>
    </td>
}