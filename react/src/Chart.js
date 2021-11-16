import React from "react";
import {humanFileSize} from "./util";

export function BarChart(props) {
    var max = 0
    for (let field of props.fields) {
        field.Type = field.Name.replace(/[ )]/, '-')
        if (field.Capacity > max) {
            max = field.Capacity
        }
    }

    return <div className="bar-chart">
        <table>
            <tbody>
            <tr>
                {props.fields.map(field => <ChartBar key={field.Type} usage={field} max={max}/>)}
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

    return <td className={'field ' + props.usage.Type}>
        <div className="filler" style={{height: fillerHeight + '%'}}></div>
        <div className="bar" style={{height: barHeight + '%'}}>
            <div className="size">{humanFileSize(props.usage.Capacity)}</div>
            {props.usage.Used ? (
                <div style={{height: '100%'}}>
                    <div className="unused" style={{height: (100 - usedPct) + '%'}}/>
                    <div className="used" style={{height: usedPct + '%'}}/>
                </div>
            ) : null}
        </div>
    </td>
}