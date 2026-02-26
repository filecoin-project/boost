import {useMutation, useQuery} from "@apollo/client";
import {DealPublishNowMutation, DealPublishQuery} from "./gql";
import React from "react";
import moment from "moment";
import {PageContainer, ShortClientAddress, ShortDealLink} from "./Components";
import {Link} from "react-router-dom";
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
        pollInterval: 10000,
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

    if (data.dealPublish.ManualPSD) {
        return <div>
            {deals.length ? (
                <>
                    <p>
                        {deals.length} deal{deals.length === 1 ? '' : 's'} pending to be published
                    </p>

                    <div className="buttons">
                        <div className="button" onClick={doPublish}>Publish Now</div>
                    </div>
                </>
            ) : null}

            <h5>Note: Manual deal publishing is enabled in config: deals will not be published automatically, the Storage Provider must publish deals manually</h5>

            { deals.length ? <DealsTable deals={deals} /> : (
                <p>There are no deals in the batch publish queue</p>
            ) }
        </div>
    }

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
                                <ShortDealLink id={deal.ID} isLegacy={deal.IsLegacy} isDirect={deal.IsDirect} />
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
        pollInterval: 10000,
        fetchPolicy: 'network-only',
    })

    return (
        <Link key="deal-publish" className="menu-item" to="/deal-publish">
            <img className="icon" alt="" src={sendImg} />
            <h3>Publish Deals</h3>

            {data && data.dealPublish.Deals ? (
                <div className="menu-desc">
                    <b>{data.dealPublish.Deals.length}</b> ready to publish
                </div>
            ) : null}
        </Link>
    )
}
