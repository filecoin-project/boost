import {Link} from "react-router-dom";
import {StorageDealsMenuItem} from "./Deals";
import {StorageSpaceMenuItem} from "./StorageSpace";
import {DealPublishMenuItem} from "./DealPublish";
import {DealTransfersMenuItem} from "./DealTransfers";
import {SealingPipelineMenuItem} from "./SealingPipeline";
import {FundsMenuItem} from "./Funds";

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
                Message Pool
            </Link>
        </td>
    )
}

