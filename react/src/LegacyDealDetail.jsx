import React, {useEffect} from "react";
import {useQuery} from "@apollo/client";
import {EpochQuery, LegacyDealQuery} from "./gql";
import {useNavigate} from "react-router-dom";
import {dateFormat} from "./util-date";
import moment from "moment";
import {humanFIL, addCommas, humanFileSize} from "./util";
import {useParams, Link} from "react-router-dom";
import './DealDetail.css'
import closeImg from './bootstrap-icons/icons/x-circle.svg'

export function LegacyDealDetail(props) {
    const params = useParams()
    const navigate = useNavigate()

    // Add a class to the document body when showing the deal detail page
    useEffect(() => {
        document.body.classList.add('modal-open')

        return function () {
            document.body.classList.remove('modal-open')
        }
    })

    const currentEpochData = useQuery(EpochQuery)

    const {loading, error, data} = useQuery(LegacyDealQuery, {
        pollInterval: 10000,
        variables: {id: params.dealID},
    })

    if (error) {
        return <div>Error: {error.message}</div>
    }

    if (loading) {
        return <div>Loading ...</div>
    }

    var deal = data.legacyDeal

    const currentEpoch = (((currentEpochData || {}).data || {}).epoch || {}).Epoch
    var startEpochTime, endEpochTime
    if (currentEpoch) {
        const secondsPerEpoch = currentEpochData.data.epoch.SecondsPerEpoch
        const startEpochDelta = Number(deal.StartEpoch - currentEpoch)
        startEpochTime = new Date(new Date().getTime() + startEpochDelta*secondsPerEpoch*1000)
        const endEpochDelta = Number(deal.EndEpoch - currentEpoch)
        endEpochTime = new Date(new Date().getTime() + endEpochDelta*secondsPerEpoch*1000)
    }

    return <div className="deal-detail modal" id={deal.ID}>
        <div className="content">
            <div className="close" onClick={() => navigate(-1)}>
                <img className="icon" alt="" src={closeImg} />
            </div>
            <div className="title">Deal {deal.ID}</div>
            <table className="deal-fields">
                <tbody>
                <tr>
                    <th>CreatedAt</th>
                    <td>
                        {moment(deal.CreatedAt).format(dateFormat)}
                        &nbsp;
                        <span className="aux">({moment(deal.CreatedAt).fromNow()} ago)</span>
                    </td>
                </tr>
                <tr>
                    <th>Client Address</th>
                    <td>
                        <a href={"https://filfox.info/en/address/"+deal.ClientAddress} target="_blank" rel="noreferrer">
                            {deal.ClientAddress}
                        </a>
                    </td>
                </tr>
                <tr>
                    <th>Client Peer ID</th>
                    <td>{deal.ClientPeerID}</td>
                </tr>
                <tr>
                    <th>Deal Data Root CID</th>
                    <td><Link to={'/piece-doctor/'+deal.DealDataRoot}>{deal.DealDataRoot}</Link></td>
                </tr>
                <tr>
                    <th>CAR File Path</th>
                    <td>{deal.InboundCARPath}</td>
                </tr>
                <tr>
                    <th>Piece CID</th>
                    <td><Link to={'/piece-doctor/'+deal.PieceCid}>{deal.PieceCid}</Link></td>
                </tr>
                <tr>
                    <th>Verified</th>
                    <td>{deal.IsVerified ? 'Yes' : 'No'}</td>
                </tr>
                <tr>
                    <th>Piece Size</th>
                    <td>
                        {humanFileSize(deal.PieceSize)}
                        &nbsp;
                        <span className="aux">({addCommas(deal.PieceSize)} bytes)</span>
                    </td>
                </tr>
                <tr>
                    <th>Piece File Path</th>
                    <td>{deal.PiecePath}</td>
                </tr>
                <tr>
                    <th>Provider Collateral</th>
                    <td>{humanFIL(deal.ProviderCollateral)}</td>
                </tr>
                <tr>
                    <th>Funds Reserved</th>
                    <td>{humanFIL(deal.FundsReserved)}</td>
                </tr>
                <tr>
                    <th>Current Epoch</th>
                    <td>{currentEpoch ? currentEpoch.toString() : null}</td>
                </tr>
                <tr>
                    <th>Start Epoch</th>
                    <td>
                        {deal.StartEpoch.toString()}
                        <span className="aux">
                            {startEpochTime ? ' (' + moment(startEpochTime).fromNow() + ')' : null}
                        </span>
                    </td>
                </tr>
                <tr>
                    <th>End Epoch</th>
                    <td>
                        {deal.EndEpoch.toString()}
                        <span className="aux">
                            {endEpochTime ? ' (' + moment(endEpochTime).fromNow() + ')' : null}
                        </span>
                    </td>
                </tr>
                <tr>
                    <th>Transfer Type</th>
                    <td>{deal.TransferType}</td>
                </tr>
                <tr>
                    <th>Transferred</th>
                    <td>
                        {humanFileSize(deal.Transferred)}
                        &nbsp;
                        <span className="aux">({addCommas(deal.Transferred)} bytes)</span>
                    </td>
                </tr>
                <tr>
                    <th>Transfer Size</th>
                    <td>
                        {deal.TransferSize ? addCommas(deal.TransferSize) : null}
                    </td>
                </tr>
                {deal.SectorNumber > 0 ? (
                    <>
                        <tr>
                            <th>Sector ID</th>
                            <td>{deal.SectorNumber + ''}</td>
                        </tr>
                    </>
                ) : null}
                <tr>
                    <th>Available for Retrieval</th>
                    <td>{deal.AvailableForRetrieval ? 'Yes' : 'No'}</td>
                </tr>
                {deal.ChainDealID > 0 ? (
                    <>
                        <tr>
                            <th>Chain Deal ID</th>
                            <td>{deal.ChainDealID + ''}</td>
                        </tr>
                    </>
                ) : null}
                <tr>
                    <th>Publish Message CID</th>
                    <td>
                        <a href={"https://filfox.info/en/message/"+deal.PublishCid} target="_blank" rel="noreferrer">
                            {deal.PublishCid}
                        </a>
                    </td>
                </tr>
                <tr>
                    <th>Status</th>
                    <td>{deal.Status}</td>
                </tr>
                <tr>
                    <th>Message</th>
                    <td>{deal.Message}</td>
                </tr>
                </tbody>
            </table>
        </div>
    </div>
}
