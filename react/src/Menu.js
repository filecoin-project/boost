import {Link} from "react-router-dom";
import {StorageDealsMenuItem} from "./Deals";
import {StorageSpaceMenuItem} from "./StorageSpace";
import {DealPublishMenuItem} from "./DealPublish";
import {DealTransfersMenuItem} from "./DealTransfers";
import {SealingPipelineMenuItem} from "./SealingPipeline";
import {FundsMenuItem} from "./Funds";
import gridImg from './bootstrap-icons/icons/grid-3x3-gap.svg'
import './Menu.css'

export function Menu(props) {
    return (
        <td className="menu">
            <StorageDealsMenuItem />
            <StorageSpaceMenuItem />
            <SealingPipelineMenuItem />
            <FundsMenuItem />
            <DealPublishMenuItem />
            <DealTransfersMenuItem />
            <Link key="mpool" className="menu-item" to="/mpool">
                <img className="icon" alt="" src={gridImg} />
                <h3>Message Pool</h3>
            </Link>
        </td>
    )
}

