import {useQuery} from "@apollo/client";
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
    const {loading, error, data} = useQuery(SealingPipelineQuery, { pollInterval: 10000 })

    if (loading) {
        return <div>Loading...</div>
    }
    if (error) {
        return <div>Error: {error.message}</div>
    }

    const sealingPipeline = data.sealingpipeline

    return <div className="sealing-pipeline">
        <WaitDeals wdSectors={sealingPipeline.WaitDealsSectors} sdwdSectors={sealingPipeline.SnapDealsWaitDealsSectors} />
        <Sealing states={sealingPipeline.SectorStates} />
        <Workers workers={sealingPipeline.Workers} />
    </div>
}

function WaitDeals(props) {
    return <table className="wait-deals">
        <tbody>
        <tr>
            <td className="wait-deals-type">
                <WaitDealsType sectors={props.wdSectors} name="Regular"/>
            </td>
            <td className="wait-deals-type">
                <WaitDealsType sectors={props.sdwdSectors} name="Snap Deals"/>
            </td>
        </tr>
        </tbody>
    </table>
}

function WaitDealsType(props) {
    return <div>
        <div className="title">{props.name} Wait Deals</div>
        { props.sectors.length ? (
            props.sectors.map(s => <WaitDealsSector sector={s}/>)
        ) : (
            <div className="no-deals">There are no sectors in the WaitDeals state</div>
        ) }
    </div>
}

function WaitDealsSector(props) {
    const sector = props.sector
    const free = sector.SectorSize - sector.Used
    const haveDeals = sector.Deals.length > 0

    var bars = [{
        className: 'free',
        amount: free,
    }]
    if (haveDeals) {
        bars = [{
            className: 'filled',
            amount: sector.Used,
        }].concat(bars)
    }

    return <div className="wait-deals-sector">
        <div className="sector-id">Sector {sector.SectorID+''}</div>
        <CumulativeBarChart bars={bars} unit="byte" />

        { haveDeals ? <WaitDealsSizes free={free} deals={sector.Deals} /> : (
            <div className="no-deals">There are no deals in this sector</div>
        )}
    </div>
}

function WaitDealsSizes(props) {
    return <table className="wait-deals-sizes">
        <tbody>
            {props.deals.map(deal => (
                <tr key={deal.ID}>
                    <td className="deal-id">
                        <ShortDealLink id={deal.ID} isLegacy={deal.IsLegacy} isDirect={deal.IsDirect}/>
                    </td>
                    <td className="deal-size">{humanFileSize(deal.Size)}</td>
                </tr>
            ))}
            <tr key="free" className="free">
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
    const workers = JSON.parse(JSON.stringify(props.workers)).sort((a, b) => {
        if (a.Start > b.Start) return 1
        if (a.Start < b.Start) return -1
        return 0
    })
    return <div className="workers">
        <div className="title">Workers</div>
        {workers.length === 0 ? <div className="no-workers">No active jobs</div> : (
            <table>
                <tbody>
                <tr>
                    <th className="start">Start</th>
                    <th className="worker-id">ID</th>
                    <th className="stage">Stage</th>
                    <th className="sector">Sector</th>
                </tr>
                {workers.map(worker => (
                    <tr key={worker.ID+worker.Sector}>
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
        pollInterval: 10000,
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
