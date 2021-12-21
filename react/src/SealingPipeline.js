import {useQuery} from "@apollo/react-hooks";
import {SealingPipelineQuery} from "./gql";
import React from "react";
import {humanFileSize} from "./util";
import {PageContainer, ShortDealID, ShortDealLink} from "./Components";
import './SealingPipeline.css'
import {dateFormat} from "./util-date";
import moment from 'moment';

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
        <Sealing {...sealingPipeline.SectorStates} />
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

    return <div className="wait-deals">
        <div className="title">Wait Deals</div>

        <div className="cumulative-bar-chart">
            <div className="bars">
                {!haveDeals ? null : (
                    <div className="bar used" style={{width: (totalSize * 100n / props.SectorSize) + '%'}}>
                        <div className="bar-inner"></div>
                    </div>
                )}
            </div>
            <div className="total">{humanFileSize(props.SectorSize)}</div>
        </div>

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
    const sectorStates = [{
        Name: 'Add Piece',
        Count: props.AddPiece,
    }, {
        Name: 'Packing',
        Count: props.Packing,
    }, {
        Name: 'Pre-commit 1',
        Count: props.PreCommit1,
    }, {
        Name: 'Pre-commit 2',
        Count: props.PreCommit2,
    }, {
        Name: 'Pre-commit Wait',
        Count: props.PreCommitWait,
    }, {
        Name: 'Wait Seed',
        Count: props.WaitSeed,
    }, {
        Name: 'Committing',
        Count: props.Committing,
    }, {
        Name: 'Committing Wait',
        Count: props.CommittingWait,
    }, {
        Name: 'Finalize Sector',
        Count: props.FinalizeSector,
    }]

    var total = 0
    var lastVisibleIndex = -1
    for (let i = 0; i < sectorStates.length; i++) {
        let sec = sectorStates[i]
        total += sec.Count
        if (sec.Count > 0) {
            lastVisibleIndex = i
        }
        sec.className = sec.Name.replace(/ /g, '_')
    }

    return <div className="sealing">
        <div className="title">Sealing</div>

        <div className="cumulative-bar-chart">
            <div className="bars">
                {sectorStates.map((sec, i) => sec.Count === 0 ? null : (
                    <div
                        key={sec.Name}
                        className={"bar " + sec.className + ' ' + ((i === lastVisibleIndex) ? 'last-visible' : '')}
                        style={{width:(100*sec.Count/total)+'%'}}
                    >
                        <div className="bar-inner"></div>
                    </div>
                ))}
            </div>
            <div className="total">{total}</div>
        </div>

        <table className="sector-states">
            <tbody>
            {sectorStates.map(sec => (
                <tr key={sec.Name}>
                    <td className="color"><div className={sec.className} /></td>
                    <td className="state">{sec.Name}</td>
                    <td className="count">{sec.Count}</td>
                </tr>
            ))}
            </tbody>
        </table>
    </div>
}

function Workers(props) {
    return <div className="workers">
        <div className="title">Workers</div>
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
    </div>
}
