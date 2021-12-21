import {useMutation, useQuery} from "@apollo/react-hooks";
import {DealPublishNowMutation, DealPublishQuery} from "./gql";
import React from "react";
import moment from "moment";
import {PageContainer, ShortDealLink} from "./Components";

export function DealPublishPage(props) {
    return <PageContainer pageType="deal-publish" title="Publish Deals">
        <DealPublishContent />
    </PageContainer>
}

function DealPublishContent() {
    const {loading, error, data} = useQuery(DealPublishQuery)
    const [publishNow] = useMutation(DealPublishNowMutation, {
        refetchQueries: [{ query: DealPublishQuery }]
    })

    if (loading) {
        return <div>Loading...</div>
    }
    if (error) {
        return <div>Error: {error.message}</div>
    }

    var period = moment.duration(data.dealPublish.Period, 'seconds')
    var publishTime = moment(data.dealPublish.Start).add(period)

    var deals = data.dealPublish.Deals
    return <div>
        {deals.length ? (
            <>
            <div className="buttons">
                <div className="button" onClick={publishNow}>Publish Now</div>
            </div>

            <p>
                {deals.length} deal{deals.length === 1 ? '' : 's'} will be published
                at {publishTime.format('HH:mm:ss')} (in {publishTime.toNow()})
            </p>
            </>
        ) : null}

        <table className="deal-publish">
            <tbody>
                <tr>
                    <td>Deal publish period</td>
                    <td>{period.humanize()}</td>
                </tr>
                <tr>
                    <td>Max deals per message</td>
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
                    </tr>
                    {props.deals.map(deal => (
                        <tr key={deal.ID}>
                            <td>{moment(deal.CreatedAt).fromNow()}</td>
                            <td className="deal-id">
                                <ShortDealLink id={deal.ID} />
                            </td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </>
    )
}
