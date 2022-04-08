import {useQuery} from "@apollo/react-hooks";
import {SealingPipelineQuery} from "./gql";
import React from "react";
import {humanFileSize} from "./util";
import {PageContainer, ShortDealLink} from "./Components";
import './SealingPipeline.css'
import {dateFormat} from "./util-date";
import moment from 'moment';
import {Link} from "react-router-dom";
import layerBackwardImg from './bootstrap-icons/icons/layer-backward.svg'
import {CumulativeBarChart} from "./CumulativeBarChart";

export function SealingPipelinePage(props) {
    return <PageContainer pageType="sealing-pipeline" title="Sealing Pipeline">
        <SealingPipelineContent />
    </PageContainer>
}

function SealingPipelineContent(props) {
    const {loading, error, data} = useQuery(SealingPipelineQuery, { pollInterval: 2000 })

    if (loading) {
        return <div>Loading...</div>
    }
    if (error) {
        return <div>Error: {error.message}</div>
    }

    const sealingPipeline = data.sealingpipeline

    return <div className="sealing-pipeline">
        <WaitDeals {...sealingPipeline.WaitDeals} />
        <Sealing states={sealingPipeline.SectorStates} />
        <Workers workers={sealingPipeline.Workers} />
    </div>
}

function WaitDeals(props) {
    var totalSize = 0n
    for (let deal of props.Deals) {
        totalSize += deal.Size
    }
    const free = props.SectorSize - totalSize
    const haveDeals = props.Deals.length > 0

    var bars = [{
        className: 'free',
        amount: free,
    }]
    if (haveDeals) {
        bars = [{
            className: 'filled',
            amount: totalSize,
        }].concat(bars)
    }

    return <div className="wait-deals">
        <div className="title">Wait Deals</div>

        <CumulativeBarChart bars={bars} unit="byte" />

        { haveDeals ? <WaitDealsSizes free={free} deals={props.Deals} /> : (
            <div className="no-deals">There are no deals in the Wait Deals state</div>
        )}
    </div>
}

function WaitDealsSizes(props) {
    return <table>
        <tbody>
            {props.deals.map(deal => (
                <tr key={deal.ID}>
                    <td className="deal-id">
                        <ShortDealLink id={deal.ID} />
                    </td>
                    <td className="deal-size">{humanFileSize(deal.Size)}</td>
                </tr>
            ))}
            <tr key="free">
                <td className="deal-id">Free</td>
                <td className="deal-size">{humanFileSize(props.free)}</td>
            </tr>
        </tbody>
    </table>
}

function Sealing(props) {
    return (
        <table className="sealing">
            <tbody>
            <tr>
                <td className="sealing-type">
                    <SealingType states={props.states} sectorType="Regular" />
                </td>
                <td className="sealing-type">
                    <SealingType states={props.states} sectorType="Snap Deals" />
                </td>
            </tr>
            </tbody>
        </table>
    )
}

function SealingType(props) {
    var states = []
    var errStates = []
    if (props.sectorType === 'Regular') {
        states = props.states.Regular
        errStates = props.states.RegularError
    } else {
        states = props.states.SnapDeals
        errStates = props.states.SnapDealsError
    }
    states = states.map(s => s).sort((a, b) => a.Order - b.Order)
    states = states.concat(errStates.map(s => s).sort((a, b) => a.Order - b.Order))

    const title = props.sectorType === 'Snap Deals' ? 'Snap Deals Sectors' : 'Regular Sectors'
    const className = props.sectorType.toLowerCase().replace(/ /g, '_')
    return <div className={"sealing-type-content " + className}>
        <div className="title">{title}</div>

        <table className="sector-states">
            <tbody>
            {states.map(sec => (
                <tr key={sec.Key}>
                    <td className="color"><div className={sec.Key.replace(/ /g, '_')} /></td>
                    <td className="state">{sec.Key}</td>
                    <td className={"count " + (sec.Value ? '' : 'zero')}>{sec.Value}</td>
                </tr>
            ))}
            </tbody>
        </table>
    </div>
}

function Workers(props) {
    return <div className="workers">
        <div className="title">Workers</div>
        {props.workers.length === 0 ? <div className="no-workers">No active workers</div> : (
            <table>
                <tbody>
                <tr>
                    <th className="start">Start</th>
                    <th className="worker-id">ID</th>
                    <th className="stage">Stage</th>
                    <th className="sector">Sector</th>
                </tr>
                {props.workers.map(worker => (
                    <tr key={worker.ID}>
                        <td className="start">{moment(worker.Start).format(dateFormat)}</td>
                        <td className="worker-id">{worker.ID}</td>
                        <td className="stage">{worker.Stage}</td>
                        <td className="sector">{worker.Sector}</td>
                    </tr>
                ))}
                </tbody>
            </table>
        )}
    </div>
}

export function SealingPipelineMenuItem(props) {
    const {data} = useQuery(SealingPipelineQuery, {
        pollInterval: 5000,
        fetchPolicy: "network-only",
    })

    var total = 0
    if (data) {
        for (let sec of data.sealingpipeline.SectorStates.Regular) {
            total += Number(sec.Value)
        }
        for (let sec of data.sealingpipeline.SectorStates.RegularError) {
            total += Number(sec.Value)
        }
        for (let sec of data.sealingpipeline.SectorStates.SnapDeals) {
            total += Number(sec.Value)
        }
        for (let sec of data.sealingpipeline.SectorStates.SnapDealsError) {
            total += Number(sec.Value)
        }
    }

    return <Link key="sealing-pipeline" className="menu-item" to="/sealing-pipeline">
        <img className="icon" alt="" src={layerBackwardImg} />
        <h3>Sealing Pipeline</h3>
        <div className="menu-desc">
            <b>{total}</b> sector{total === 1 ? '' : 's'}
        </div>
    </Link>
}
