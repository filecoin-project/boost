import {useMutation, useQuery} from "@apollo/react-hooks";
import {DealPublishNowMutation, DealPublishQuery} from "./gql";
import React from "react";
import moment from "moment";
import {PageContainer, ShortClientAddress, ShortDealID, ShortDealLink} from "./Components";
import {NavLink} from "react-router-dom";
import sendImg from './bootstrap-icons/icons/send.svg'
import './DealPublish.css'
import {humanFileSize} from "./util";
import {ShowBanner} from "./Banner";

export function DealPublishPage(props) {
    return <PageContainer pageType="deal-publish" title="Publish Deals">
        <DealPublishContent />
    </PageContainer>
}

function DealPublishContent() {
    const {loading, error, data} = useQuery(DealPublishQuery, {
        pollInterval: 5000,
    })
    const [publishNow] = useMutation(DealPublishNowMutation, {
        refetchQueries: [{ query: DealPublishQuery }]
    })

    if (loading) {
        return <div>Loading...</div>
    }
    if (error) {
        return <div>Error: {error.message}</div>
    }

    async function doPublish() {
        const dealCount = deals.length
        await publishNow()
        ShowBanner('Published '+dealCount+' deals')
    }

    var period = moment.duration(data.dealPublish.Period, 'seconds')
    var publishTime = moment(data.dealPublish.Start).add(period)

    var deals = data.dealPublish.Deals
    return <div>
        {deals.length ? (
            <>
            <p>
                {deals.length} deal{deals.length === 1 ? '' : 's'} will be published
                at <b>{publishTime.format('HH:mm:ss')}</b> (in {publishTime.toNow()})
            </p>

            <div className="buttons">
                <div className="button" onClick={doPublish}>Publish Now</div>
            </div>
            </>
        ) : null}

        <h3>Deal Publish Config</h3>

        <table className="deal-publish">
            <tbody>
                <tr>
                    <th>Deal publish period</th>
                    <td>{period.humanize()}</td>
                </tr>
                <tr>
                    <th>Max deals per message</th>
                    <td>{data.dealPublish.MaxDealsPerMsg}</td>
                </tr>
            </tbody>
        </table>

        { deals.length ? <DealsTable deals={deals} /> : (
            <p>There are no deals in the batch publish queue</p>
        ) }
    </div>
}

function DealsTable(props) {
    return (
        <>
            <h3>Deals</h3>

            <table className="deals">
                <tbody>
                    <tr>
                        <th>Created</th>
                        <th>Deal ID</th>
                        <th>Size</th>
                        <th>Piece Size</th>
                        <th>Client</th>
                    </tr>
                    {props.deals.map(deal => (
                        <tr key={deal.ID}>
                            <td>{moment(deal.CreatedAt).fromNow()}</td>
                            <td className="deal-id">
                                {deal.IsLegacy ? (
                                    <NavLink to={"/legacy-deals/" + deal.ID}>
                                        <ShortDealID id={deal.ID} />
                                    </NavLink>
                                ) : (
                                    <ShortDealLink id={deal.ID} />
                                )}
                            </td>
                            <td className="size">{humanFileSize(deal.Transfer.Size)}</td>
                            <td className="piece-size">{humanFileSize(deal.PieceSize)}</td>
                            <td className="client">
                                <ShortClientAddress address={deal.ClientAddress} />
                            </td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </>
    )
}

export function DealPublishMenuItem(props) {
    const {data} = useQuery(DealPublishQuery, {
        pollInterval: 5000,
        fetchPolicy: 'network-only',
    })

    return (
        <NavLink key="deal-publish" className="sidebar-item sidebar-item-deals" to="/deal-publish">
            <span className="sidebar-icon">
                <svg width="24" height="26" viewBox="0 0 24 26" fill="none" xmlns="http://www.w3.org/2000/svg"><rect x="1" y="1" width="9" height="7" rx="2" stroke-width="2"/><rect x="23" y="25" width="9" height="7" rx="2" transform="rotate(-180 23 25)" stroke-width="2"/><rect x="1" y="12" width="9" height="13" rx="2" stroke-width="2"/><rect x="23" y="14" width="9" height="13" rx="2" transform="rotate(-180 23 14)" stroke-width="2"/></svg>
            </span>
            <span className="sidebar-title">
                Publish Deals
            </span>

            {data && data.dealPublish.Deals ? (
                <div className="sidebar-item-excerpt">
                    <span className="figure">{data.dealPublish.Deals.length}</span>
                    <span className="label"> ready to publish</span>
                </div>
            ) : null}
        </NavLink>
    )
}
