import {Link} from "react-router-dom";
import {StorageDealsMenuItem} from "./Deals";
import {StorageSpaceMenuItem} from "./StorageSpace";
import {DealPublishMenuItem} from "./DealPublish";
import {DealTransfersMenuItem} from "./DealTransfers";
import {SealingPipelineMenuItem} from "./SealingPipeline";
import {FundsMenuItem} from "./Funds";
import gridImg from './bootstrap-icons/icons/grid-3x3-gap.svg'
import './Menu.css'
import {SettingsMenuItem} from "./Settings";
import {InspectMenuItem} from "./Inspect";
import {ProposalLogsMenuItem} from "./ProposalLogs";

export function Menu(props) {
    function scrollToTop() {
        window.scrollTo({ top: 0, behavior: "smooth" })
    }

    return (
        <td onClick={scrollToTop} className="menu">
            <StorageDealsMenuItem />
            <ProposalLogsMenuItem />
            <StorageSpaceMenuItem />
            <SealingPipelineMenuItem />
            <FundsMenuItem />
            <DealPublishMenuItem />
            <DealTransfersMenuItem />
            <InspectMenuItem />
            <Link key="mpool" className="menu-item" to="/mpool">
                <img className="icon" alt="" src={gridImg} />
                <h3>Message Pool</h3>
            </Link>
            <SettingsMenuItem />
        </td>
    )
}

